package helpers

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nsqio/go-nsq"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/require"
)

const (
	// NsqdAddr is the nsqd TCP address the compose file publishes.
	NsqdAddr = "127.0.0.1:4150"
	// nsqdHTTPAddr is the nsqd HTTP api, used to create a topic up front.
	nsqdHTTPAddr = "127.0.0.1:4151"
	// toxiproxyAddr is the toxiproxy api used by the durability test.
	toxiproxyAddr = "127.0.0.1:8474"
	// redialTimeout bounds PushEventually. go-nsq backs off between reconnect
	// attempts, so the first pushes after an outage are expected to fail.
	redialTimeout = time.Second * 60
	redialTick    = time.Second
)

func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Resume",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Empty{}))
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Pause",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Empty{}))
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Destroy",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Pipelines{}))
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, autoAck, 0)},
			&jobsProto.Empty{}))
	}
}

func PushToPipeDelayed(address string, pipeline string, delay int64) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, false, delay)},
			&jobsProto.Empty{}))
	}
}

func dummyJob(pipeline string, autoAck bool, delay int64) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
			Delay:    delay,
		},
	}
}

// PushEventually keeps retrying a push until it lands. Used after a broker
// outage, where the producer needs a few attempts to redial.
func PushEventually(t *testing.T, address string, pipeline string) {
	t.Helper()

	require.Eventually(t, func() bool {
		client := NewJobsClient(t, address)

		return client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: dummyJob(pipeline, false, 0)},
			&jobsProto.Empty{}) == nil
	}, redialTimeout, redialTick, "the producer never recovered after the outage")
}

// DeclarePipe declares a pipeline over rpc and requires the call to succeed.
func DeclarePipe(address string, pipeline string, topic string) func(t *testing.T) {
	return func(t *testing.T) {
		require.NoError(t, Declare(t, address, map[string]string{
			"driver":   "nsq",
			"name":     pipeline,
			"topic":    topic,
			"channel":  "default",
			"priority": "3",
			"prefetch": "10",
		}))
	}
}

// Declare issues a raw declare call and returns its error, so negative tests
// can assert on a rejected pipeline configuration.
func Declare(t *testing.T, address string, pipeline map[string]string) error {
	t.Helper()

	client := NewJobsClient(t, address)

	return client.Call("jobs.Declare",
		&jobsProto.DeclareRequest{Pipeline: pipeline},
		&jobsProto.Empty{})
}

// StatsFor returns the state the jobs plugin reports for one pipeline. Picking
// it by name keeps the assertion stable when several are registered.
func StatsFor(t *testing.T, address string, pipeline string) *jobState.State {
	t.Helper()

	resp := &jobsProto.Stats{}
	require.NoError(t, NewJobsClient(t, address).Call("jobs.Stat", &jobsProto.Empty{}, resp))

	for _, st := range resp.GetStats() {
		if st.GetPipeline() != pipeline {
			continue
		}

		return &jobState.State{
			Queue:    st.GetQueue(),
			Pipeline: st.GetPipeline(),
			Driver:   st.GetDriver(),
			Active:   st.GetActive(),
			Delayed:  st.GetDelayed(),
			Reserved: st.GetReserved(),
			Ready:    st.GetReady(),
			Priority: st.GetPriority(),
		}
	}

	require.FailNowf(t, "pipeline not reported", "no stats for %q", pipeline)

	return nil
}

// PublishRaw puts a message on the topic without going through RoadRunner, so a
// test can hand the listener a body the driver did not produce.
func PublishRaw(t *testing.T, topic string, body []byte) {
	t.Helper()

	producer, err := nsq.NewProducer(NsqdAddr, nsq.NewConfig())
	require.NoError(t, err)
	t.Cleanup(producer.Stop)

	require.NoError(t, producer.Publish(topic, body))
}

// CreateTopic registers the topic with nsqd up front. nsqlookupd only learns
// about a topic once nsqd has it, and a consumer discovering through lookupd
// would otherwise wait out a full poll interval.
func CreateTopic(t *testing.T, topic string) {
	t.Helper()

	post(t, fmt.Sprintf("http://%s/topic/create?topic=%s", nsqdHTTPAddr, url.QueryEscape(topic)), nil, http.StatusOK)
}

// CreateProxy fronts nsqd with a toxiproxy the durability test can cut. Both
// addresses are resolved inside the compose network, not on the host.
func CreateProxy(t *testing.T, name string, listen string, upstream string) {
	t.Helper()

	// a proxy left behind by an interrupted run would make the create conflict
	deleteProxy(t, name)

	body := fmt.Sprintf(`{"name":%q,"listen":%q,"upstream":%q,"enabled":true}`, name, listen, upstream)
	post(t, "http://"+toxiproxyAddr+"/proxies", []byte(body), http.StatusCreated)
	t.Cleanup(func() { deleteProxy(t, name) })
}

// SetProxyEnabled cuts or restores the connection to nsqd.
func SetProxyEnabled(t *testing.T, name string, enabled bool) {
	t.Helper()

	body := fmt.Sprintf(`{"enabled":%t}`, enabled)
	post(t, "http://"+toxiproxyAddr+"/proxies/"+name, []byte(body), http.StatusOK)
}

func deleteProxy(t *testing.T, name string) {
	t.Helper()

	// runs from t.Cleanup, where the test context is already canceled
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, "http://"+toxiproxyAddr+"/proxies/"+name, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Contains(t, []int{http.StatusNoContent, http.StatusNotFound}, resp.StatusCode)
}

func post(t *testing.T, addr string, body []byte, wantStatus int) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, addr, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, wantStatus, resp.StatusCode, "POST %s", addr)
}
