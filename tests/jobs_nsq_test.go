package tests

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/nsqio/go-nsq"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/logger/v6"
	nsqPlugin "github.com/roadrunner-server/nsq/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestNSQInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*2))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("PushPipeline", helpers.PushToPipe("test-1", false, "127.0.0.1:7002"))
	t.Run("PushPipeline", helpers.PushToPipe("test-2", false, "127.0.0.1:7002"))

	time.Sleep(time.Second)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7002", "test-1", "test-2"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
}

func TestNSQInitPQ(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-pq.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	for range 100 {
		t.Run("PushPipeline", helpers.PushToPipe("test-1-pq", false, "127.0.0.1:6601"))
		t.Run("PushPipeline", helpers.PushToPipe("test-2-pq", false, "127.0.0.1:6601"))
	}

	time.Sleep(time.Second * 2)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6601", "test-1-pq", "test-2-pq"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 200, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	require.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job processing was started").Len(), 4)
}

func TestNSQInitAutoAck(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-init.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("PushPipeline", helpers.PushToPipe("test-1", true, "127.0.0.1:7002"))
	t.Run("PushPipeline", helpers.PushToPipe("test-2", true, "127.0.0.1:7002"))

	time.Sleep(time.Second * 2)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7002", "test-1", "test-2"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("auto_ack option enabled").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
}

func TestNSQDeclare(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-declare.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("DeclareNSQPipeline", declareNSQPipe("127.0.0.1:7001"))
	t.Run("ConsumeNSQPipeline", helpers.ResumePipes("127.0.0.1:7001", "test-3"))
	t.Run("PushNSQPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7001"))
	time.Sleep(time.Second * 2)
	t.Run("PauseNSQPipeline", helpers.PausePipelines("127.0.0.1:7001", "test-3"))
	time.Sleep(time.Second * 3)
	t.Run("DestroyNSQPipeline", helpers.DestroyPipelines("127.0.0.1:7001", "test-3"))

	time.Sleep(time.Second * 3)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestNSQStats(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-declare.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("DeclarePipeline", declareNSQPipe("127.0.0.1:7001"))
	t.Run("ConsumePipeline", helpers.ResumePipes("127.0.0.1:7001", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7001"))
	time.Sleep(time.Second * 2)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:7001", "test-3"))
	time.Sleep(time.Second)
	t.Run("PushPipelineDelayed", helpers.PushToPipeDelayed("127.0.0.1:7001", "test-3", 8))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7001"))
	time.Sleep(time.Second)

	out := &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:7001", out))

	assert.Equal(t, "test-3", out.Pipeline)
	assert.Equal(t, "nsq", out.Driver)
	assert.NotEmpty(t, out.Queue)
	assert.Equal(t, int64(1), out.Delayed)
	assert.False(t, out.Ready)
	assert.Equal(t, uint64(3), out.Priority)

	time.Sleep(time.Second)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:7001", "test-3"))
	time.Sleep(time.Second * 12)

	out = &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:7001", out))

	assert.Equal(t, "test-3", out.Pipeline)
	assert.Equal(t, "nsq", out.Driver)
	assert.NotEmpty(t, out.Queue)
	assert.Equal(t, int64(0), out.Delayed)
	assert.True(t, out.Ready)
	assert.Equal(t, uint64(3), out.Priority)

	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7001", "test-3"))

	time.Sleep(time.Second)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestNSQJobsError(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-jobs-err.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("DeclarePipeline", declareNSQPipe("127.0.0.1:7005"))
	t.Run("ConsumePipeline", helpers.ResumePipes("127.0.0.1:7005", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7005"))
	time.Sleep(time.Second * 25)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:7005", "test-3"))
	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7005", "test-3"))

	time.Sleep(time.Second * 5)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestNSQNoGlobalSection(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-no-global.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	_, err = cont.Serve()
	require.NoError(t, err)
}

func TestNSQRaw(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-raw.yaml",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	// publish a non-RoadRunner (raw) message directly to the topic; the driver
	// must wrap it into a synthetic item and hand it to the worker.
	producer, err := nsq.NewProducer("127.0.0.1:4150", nsq.NewConfig())
	require.NoError(t, err)

	err = producer.Publish("rr-raw", []byte("fooobarrbazzz"))
	require.NoError(t, err)

	time.Sleep(time.Second * 5)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7006", "test-raw"))

	stopCh <- struct{}{}
	wg.Wait()
	producer.Stop()
	time.Sleep(time.Second)

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was processed successfully").Len())
}

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

func (m *inMemoryTracer) Init() error                      { return nil }
func (m *inMemoryTracer) Name() string                     { return "inMemoryTracer" }
func (m *inMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }

func TestNSQOTEL(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-otel.yaml",
	}

	tracer := newInMemoryTracer(t)
	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		tracer,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)
	t.Run("PushPipeline-1", helpers.PushToPipe("test-1", false, "127.0.0.1:7002"))
	t.Run("PushPipeline-2", helpers.PushToPipe("test-1", false, "127.0.0.1:7002"))
	time.Sleep(time.Second * 2)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7002", "test-1"))

	stopCh <- struct{}{}
	wg.Wait()

	spanMap := make(map[string]struct{})
	for _, s := range tracer.exp.GetSpans() {
		spanMap[s.Name] = struct{}{}
	}

	spans := make([]string, 0, len(spanMap))
	for name := range spanMap {
		spans = append(spans, name)
	}
	slices.Sort(spans)

	expected := []string{
		"destroy_pipeline",
		"jobs_listener",
		"nsq_listener",
		"nsq_push",
		"nsq_stop",
		"push",
	}
	assert.Equal(t, expected, spans)

	// nsq_listener spans must continue the push trace (valid parent) and must
	// not be chained to one another.
	listenerSpanIDs := make(map[trace.SpanID]struct{})
	for _, s := range tracer.exp.GetSpans() {
		if s.Name == "nsq_listener" {
			listenerSpanIDs[s.SpanContext.SpanID()] = struct{}{}
		}
	}
	for _, s := range tracer.exp.GetSpans() {
		if s.Name == "nsq_listener" {
			assert.True(t, s.Parent.IsValid(), "nsq_listener span should have a valid parent from the push trace context")
			_, parentIsListener := listenerSpanIDs[s.Parent.SpanID()]
			assert.False(t, parentIsListener, "nsq_listener span should not be a child of another nsq_listener span")
		}
	}

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
}

func declareNSQPipe(address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := helpers.NewJobsClient(t, address)
		req := &jobsProto.DeclareRequest{Pipeline: map[string]string{
			"driver":   "nsq",
			"name":     "test-3",
			"topic":    uuid.NewString(),
			"channel":  "default",
			"priority": "3",
			"prefetch": "10",
		}}
		_, err := client.Declare(t.Context(), connect.NewRequest(req))
		require.NoError(t, err)
	}
}
