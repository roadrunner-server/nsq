package nsqjobs

import (
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
)

// testPipeline is the jobs.Pipeline the jobs plugin hands to the driver.
type testPipeline struct {
	name     string
	driver   string
	priority int64
}

func (p *testPipeline) Name() string                      { return p.name }
func (p *testPipeline) Driver() string                    { return p.driver }
func (p *testPipeline) Priority() int64                   { return p.priority }
func (*testPipeline) With(string, any)                    {}
func (*testPipeline) Has(string) bool                     { return false }
func (*testPipeline) String(_ string, d string) string    { return d }
func (*testPipeline) Int(_ string, d int) int             { return d }
func (*testPipeline) Bool(_ string, d bool) bool          { return d }
func (*testPipeline) Map(string, map[string]string) error { return nil }
func (*testPipeline) Get(string) any                      { return nil }

var _ jobs.Pipeline = (*testPipeline)(nil)

// testMessage is the jobs.Message the jobs plugin hands to Push.
type testMessage struct {
	name     string
	id       string
	payload  []byte
	headers  map[string][]string
	priority int64
	groupID  string
	delay    int64
	autoAck  bool
}

func (m *testMessage) ID() string                   { return m.id }
func (m *testMessage) GroupID() string              { return m.groupID }
func (m *testMessage) Priority() int64              { return m.priority }
func (m *testMessage) Name() string                 { return m.name }
func (m *testMessage) Payload() []byte              { return m.payload }
func (m *testMessage) Delay() int64                 { return m.delay }
func (m *testMessage) AutoAck() bool                { return m.autoAck }
func (m *testMessage) Headers() map[string][]string { return m.headers }
func (m *testMessage) UpdatePriority(p int64)       { m.priority = p }
func (*testMessage) Offset() int64                  { return 0 }
func (*testMessage) Partition() int32               { return 0 }
func (*testMessage) Topic() string                  { return "" }
func (*testMessage) Metadata() string               { return "" }

var _ jobs.Message = (*testMessage)(nil)

// newTestDriver builds a driver with everything unpack touches and nothing else,
// so the body decoding can be covered without an nsqd.
func newTestDriver(priority int64) *Driver {
	d := &Driver{
		log:  slog.New(&recorder{}),
		conf: &config{Topic: "rr-topic", Priority: priority},
	}

	var pipe jobs.Pipeline = &testPipeline{name: "test-1", driver: pluginName, priority: priority}
	d.pipeline.Store(&pipe)

	return d
}

func TestFromJob(t *testing.T) {
	item := fromJob(&testMessage{
		name:     "some/php/namespace",
		id:       "job-id",
		payload:  []byte(`{"hello":"world"}`),
		headers:  map[string][]string{"test": {"test2"}},
		priority: 3,
		groupID:  "test-1",
		delay:    5,
		autoAck:  true,
	})

	require.Equal(t, "job-id", item.ID())
	require.Equal(t, "test-1", item.GroupID())
	require.Equal(t, int64(3), item.Priority())
	require.Equal(t, []byte(`{"hello":"world"}`), item.Body())
	require.Equal(t, map[string][]string{"test": {"test2"}}, item.Headers())
	require.Equal(t, int64(5), item.Options.Delay)
	require.True(t, item.Options.AutoAck)
}

func TestDelayDuration(t *testing.T) {
	require.Equal(t, 5*time.Second, (&Options{Delay: 5}).DelayDuration())
	require.Zero(t, (&Options{}).DelayDuration())
}

func TestItemContext(t *testing.T) {
	item := &Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		Hdrs:    map[string][]string{"test": {"test2"}},
		Options: &Options{Pipeline: "test-1", Queue: "rr-topic"},
	}

	data, err := item.Context()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, "job-id", got["id"])
	require.Equal(t, "nsq", got["driver"])
	require.Equal(t, "rr-topic", got["queue"])
	require.Equal(t, "test-1", got["pipeline"])
}

// TestUnpackRoundTrip covers a message this driver produced. NSQ has no broker
// header table, so the whole item travels in the body.
func TestUnpackRoundTrip(t *testing.T) {
	d := newTestDriver(11)

	body, err := json.Marshal(&Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		Payload: []byte(`{"hello":"world"}`),
		Hdrs:    map[string][]string{"test": {"test2"}},
		Options: &Options{Priority: 3, Delay: 5},
	})
	require.NoError(t, err)

	item := d.unpack(nsq.NewMessage(nsq.MessageID{}, body))

	require.Equal(t, "job-id", item.ID())
	require.Equal(t, int64(3), item.Priority())
	require.Equal(t, map[string][]string{"test": {"test2"}}, item.Headers())
	require.Equal(t, "test-1", item.Options.Pipeline)
	require.Equal(t, "rr-topic", item.Options.Queue)
}

// TestUnpackInheritsPipelinePriority covers a body that carries no priority of
// its own, which has to fall back to the pipeline default rather than zero.
func TestUnpackInheritsPipelinePriority(t *testing.T) {
	d := newTestDriver(11)

	body, err := json.Marshal(&Item{Ident: "job-id", Options: &Options{}})
	require.NoError(t, err)

	item := d.unpack(nsq.NewMessage(nsq.MessageID{}, body))

	require.Equal(t, int64(11), item.Priority())
	require.NotNil(t, item.Headers())
}

// TestUnpackSyntheticItem covers messages published by something other than
// RoadRunner. The raw bytes become the payload instead of being dropped.
func TestUnpackSyntheticItem(t *testing.T) {
	for name, body := range map[string][]byte{
		"not json":   []byte("fooobarrbazzz"),
		"no options": []byte(`{"job":"x","id":"y"}`),
	} {
		t.Run(name, func(t *testing.T) {
			item := newTestDriver(11).unpack(nsq.NewMessage(nsq.MessageID{}, body))

			require.Equal(t, auto, item.Job)
			require.NotEmpty(t, item.ID())
			require.Equal(t, body, item.Body())
			require.Equal(t, int64(11), item.Priority())
			require.Equal(t, "test-1", item.Options.Pipeline)
		})
	}
}

// newStoppedItem returns an item whose pipeline has already been stopped.
func newStoppedItem() *Item {
	stopped := &atomic.Uint64{}
	stopped.Store(1)

	return &Item{Options: &Options{stopped: stopped}}
}

// TestStoppedPipelineRejectsReply covers the guard that keeps a late worker
// reply from touching a consumer the driver has already torn down.
func TestStoppedPipelineRejectsReply(t *testing.T) {
	require.ErrorContains(t, newStoppedItem().Ack(), "the pipeline is probably stopped")
	require.ErrorContains(t, newStoppedItem().Nack(), "the pipeline is probably stopped")
	require.ErrorContains(t, newStoppedItem().NackWithOptions(true, 0), "the pipeline is probably stopped")
	require.ErrorContains(t, newStoppedItem().Requeue(nil, 0), "the pipeline is probably stopped")
}

// newAutoAckItem returns a running item the listener has already finished.
func newAutoAckItem(delay int64, delayed *atomic.Int64) *Item {
	return &Item{Options: &Options{
		AutoAck: true,
		Delay:   delay,
		stopped: &atomic.Uint64{},
		delayed: delayed,
	}}
}

// TestAutoAckItemSkipsBroker checks the worker reply is a no-op once the
// listener finished the message, so none of these reach the nil delegate.
func TestAutoAckItemSkipsBroker(t *testing.T) {
	delayed := &atomic.Int64{}

	require.NoError(t, newAutoAckItem(0, delayed).Ack())
	require.NoError(t, newAutoAckItem(0, delayed).Nack())
	require.NoError(t, newAutoAckItem(0, delayed).NackWithOptions(true, 0))
	require.NoError(t, newAutoAckItem(0, delayed).Requeue(map[string][]string{"a": {"b"}}, 0))
}

// TestDelayedCounterReleasedOnAck covers the locally tracked delayed count the
// state report reads. A delayed job holds a slot until it is acknowledged.
func TestDelayedCounterReleasedOnAck(t *testing.T) {
	delayed := &atomic.Int64{}
	delayed.Store(1)

	require.NoError(t, newAutoAckItem(5, delayed).Ack())
	require.Zero(t, delayed.Load())
}
