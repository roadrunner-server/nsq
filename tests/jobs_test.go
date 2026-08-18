package tests

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"tests/helpers"

	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	nsqPlugin "github.com/roadrunner-server/nsq/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	initAddr     = "127.0.0.1:7002"
	declareAddr  = "127.0.0.1:7001"
	pqAddr       = "127.0.0.1:6601"
	errAddr      = "127.0.0.1:7005"
	rawAddr      = "127.0.0.1:7006"
	noGlobalAddr = "127.0.0.1:7008"
	lookupdAddr  = "127.0.0.1:7009"
	// declared is the pipeline the declare configs create over rpc.
	declared = "test-3"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	}
}

// boot starts the container with the observed logger and waits for the rpc
// listener, which is the readiness signal the fixed sleeps used to stand in for.
func boot(t *testing.T, cfgPath string, addr string, opts ...helpers.Option) (*helpers.RR, func()) {
	t.Helper()

	return helpers.Start(t, cfgPath, jobsPlugins(),
		append([]helpers.Option{
			helpers.WithObservedLogger(),
			helpers.WithTCPProbe(addr),
		}, opts...)...)
}

// TestBoots covers the config-declared pipelines: both come up at startup and
// both come down on destroy.
func TestBoots(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-init.yaml", initAddr)

	rr.RequireLogCount(t, "pipeline was started", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestPushAndProcess follows two jobs from the rpc call to the worker ack.
func TestPushAndProcess(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-init.yaml", initAddr)

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-2", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 2)
	rr.RequireLogCount(t, "job was processed successfully", 2)
}

// TestAutoAck checks the listener finishes the message itself, before the worker
// ever sees it, when the job carries the auto ack option.
func TestAutoAck(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-init.yaml", initAddr)

	helpers.PushToPipe("test-1", true, initAddr)(t)
	helpers.PushToPipe("test-2", true, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "auto_ack option enabled", 2)
}

// TestPriorityQueueBacklog pushes far more jobs than the two slow workers can
// take, so most of them sit in the priority queue until the pipelines are
// destroyed under them.
func TestPriorityQueueBacklog(t *testing.T) {
	const rounds = 100

	rr, _ := boot(t, "configs/.rr-nsq-pq.yaml", pqAddr)

	for range rounds {
		helpers.PushToPipe("test-1-pq", false, pqAddr)(t)
		helpers.PushToPipe("test-2-pq", false, pqAddr)(t)
	}

	rr.RequireLogCount(t, "job was pushed successfully", 2*rounds)

	// both workers have to be busy before the destroy, otherwise the backlog
	// would never form
	rr.WaitLog(t, "job processing was started", 2)

	helpers.DestroyPipelines(pqAddr, "test-1-pq", "test-2-pq")(t)

	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestDeclareAndConsume declares a pipeline over rpc, runs a job through it and
// pauses it again. The old test made the same calls and asserted nothing.
func TestDeclareAndConsume(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-declare.yaml", declareAddr)

	helpers.DeclarePipe(declareAddr, declared, "rr-declare-1")(t)
	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(declared, false, declareAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.DestroyPipelines(declareAddr, declared)(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestPauseStopsConsuming checks a paused pipeline still accepts pushes but
// leaves them on the topic until it is resumed.
func TestPauseStopsConsuming(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-declare.yaml", declareAddr)

	helpers.DeclarePipe(declareAddr, declared, "rr-pause-1")(t)
	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PausePipelines(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.PushToPipe(declared, false, declareAddr)(t)
	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.NeverLog(t, "job was processed successfully")

	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(declareAddr, declared)(t)
}

// TestStatsTrackDelayed covers the state report. NSQ exposes no per-channel
// counters, so the driver reports identity, readiness, and the delayed jobs it
// tracks itself. The old test waited out the delay with a flat 12 second sleep.
func TestStatsTrackDelayed(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-declare.yaml", declareAddr)

	helpers.DeclarePipe(declareAddr, declared, "rr-stats-1")(t)
	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(declared, false, declareAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	// a paused pipeline reports itself as not ready, and a deferred publish is
	// counted until the job is acknowledged
	helpers.PushToPipeDelayed(declareAddr, declared, 5)(t)

	delayed := helpers.WaitStats(t, declareAddr, declared, func(s *jobState.State) bool {
		return s.Delayed == 1
	})

	require.Equal(t, "nsq", delayed.Driver)
	require.Equal(t, "rr-stats-1", delayed.Queue)
	require.Equal(t, uint64(3), delayed.Priority)
	require.False(t, delayed.Ready)

	// resuming drains the deferred job once its delay lapses
	helpers.ResumePipes(declareAddr, declared)(t)

	drained := helpers.WaitStats(t, declareAddr, declared, func(s *jobState.State) bool {
		return s.Delayed == 0
	})

	require.True(t, drained.Ready)
	require.Equal(t, uint64(3), drained.Priority)

	helpers.DestroyPipelines(declareAddr, declared)(t)
}

// TestRequeueRetriesUntilComplete covers the worker that fails a job with a
// growing attempts header and only completes it on the fourth delivery. The old
// test slept out the three five second delays and asserted nothing.
func TestRequeueRetriesUntilComplete(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-jobs-err.yaml", errAddr)

	helpers.DeclarePipe(errAddr, declared, "rr-err-1")(t)
	helpers.ResumePipes(errAddr, declared)(t)
	helpers.PushToPipe(declared, false, errAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(errAddr, declared)(t)
	helpers.DestroyPipelines(errAddr, declared)(t)

	// one original delivery plus the three the worker requeued
	rr.RequireLogCount(t, "job processing was started", 4)
	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestRawMessage covers a publish that did not come from RoadRunner. The body
// is not an item, so the driver has to wrap it rather than drop it.
func TestRawMessage(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-nsq-raw.yaml", rawAddr)

	helpers.PublishRaw(t, "rr-raw", []byte("fooobarrbazzz"))

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(rawAddr, "test-raw")(t)

	rr.RequireLogCount(t, "failed to unpack the message body, using a synthetic item", 1)
	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.RequireLogCount(t, "job processing was started", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestLookupdDiscovery covers the consumer discovering nsqd through nsqlookupd
// instead of dialing it. The topic is registered up front, otherwise the first
// lookup finds no producer and the consumer waits out a whole poll interval.
func TestLookupdDiscovery(t *testing.T) {
	helpers.CreateTopic(t, "rr-lookupd-1")

	rr, _ := boot(t, "configs/.rr-nsq-lookupd.yaml", lookupdAddr)

	helpers.PushToPipe("test-lookupd", false, lookupdAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(lookupdAddr, "test-lookupd")(t)

	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestNoGlobalSection covers a config with pipelines but no nsq section. The
// plugin disables itself and the container still serves.
func TestNoGlobalSection(t *testing.T) {
	boot(t, "configs/.rr-no-global.yaml", noGlobalAddr, helpers.WithLogLevel(slog.LevelError))
}

// TestOTELSpans checks the spans the driver emits and that a listener span
// continues the trace the push started.
func TestOTELSpans(t *testing.T) {
	tracer := newInMemoryTracer(t)

	rr, _ := boot(t, "configs/.rr-nsq-otel.yaml", initAddr, helpers.WithPlugin(tracer))

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-1", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1")(t)

	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)

	spanNames := make(map[string]struct{})
	listenerSpanIDs := make(map[trace.SpanID]struct{})
	for _, s := range tracer.exp.GetSpans() {
		spanNames[s.Name] = struct{}{}
		if s.Name == "nsq_listener" {
			listenerSpanIDs[s.SpanContext.SpanID()] = struct{}{}
		}
	}

	names := make([]string, 0, len(spanNames))
	for name := range spanNames {
		names = append(names, name)
	}
	slices.Sort(names)

	require.Equal(t, []string{
		"destroy_pipeline",
		"jobs_listener",
		"nsq_listener",
		"nsq_push",
		"nsq_stop",
		"push",
	}, names)

	// the listener span continues the push trace, it is never chained to another
	// listener span
	for _, s := range tracer.exp.GetSpans() {
		if s.Name != "nsq_listener" {
			continue
		}

		require.True(t, s.Parent.IsValid(), "nsq_listener span has no parent from the push trace")
		_, parentIsListener := listenerSpanIDs[s.Parent.SpanID()]
		require.False(t, parentIsListener, "nsq_listener span is chained to another listener span")
	}
}

// inMemoryTracer stands in for the otel plugin, keeping the spans in process.
type inMemoryTracer struct {
	tp  *sdktrace.TracerProvider
	exp *tracetest.InMemoryExporter
}

func newInMemoryTracer(t *testing.T) *inMemoryTracer {
	t.Helper()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return &inMemoryTracer{tp: tp, exp: exp}
}

func (*inMemoryTracer) Init() error                        { return nil }
func (*inMemoryTracer) Name() string                       { return "inMemoryTracer" }
func (m *inMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }
