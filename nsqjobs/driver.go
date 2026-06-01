package nsqjobs

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	pluginName string = "nsq"
	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshals it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	mu       sync.Mutex
	log      *slog.Logger
	pq       jobs.Queue
	pipeline atomic.Pointer[jobs.Pipeline]
	tracer   *sdktrace.TracerProvider
	prop     propagation.TextMapPropagator

	// nsq
	conf     *config
	producer *nsq.Producer
	consumer *nsq.Consumer

	listeners atomic.Uint32
	delayed   atomic.Int64
	stopped   atomic.Uint64
}

// FromConfig initializes an NSQ driver from the .rr.yaml configuration.
func FromConfig(_ context.Context, tracer *sdktrace.TracerProvider, configKey string, log *slog.Logger, cfg Configurer, pipe jobs.Pipeline, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_nsq_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nsq configuration, global configuration should contain nsq addr"))
	}

	// parse the pipeline-specific section first, then overlay the global `nsq` section
	var conf config
	if err := cfg.UnmarshalKey(configKey, &conf); err != nil {
		return nil, errors.E(op, err)
	}

	if err := cfg.UnmarshalKey(pluginName, &conf); err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefault()
	if conf.Topic == "" {
		conf.Topic = pipe.Name()
	}

	return newDriver(op, tracer, prop, log, pq, &conf, pipe)
}

// FromPipeline initializes an NSQ driver from a dynamically declared pipeline.
func FromPipeline(_ context.Context, tracer *sdktrace.TracerProvider, pipe jobs.Pipeline, log *slog.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_nsq_consumer_from_pipeline")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nsq configuration, global configuration should contain nsq addr"))
	}

	// only the global section is unmarshalled; per-pipeline values come from the pipeline itself
	var conf config
	if err := cfg.UnmarshalKey(pluginName, &conf); err != nil {
		return nil, errors.E(op, err)
	}

	conf.Topic = pipe.String(topicKey, pipe.Name())
	conf.Channel = pipe.String(channelKey, "")
	conf.Prefetch = pipe.Int(prefetchKey, 10)
	conf.Priority = pipe.Priority()
	conf.MaxAttempts = uint16(pipe.Int(maxAttemptsKey, 0)) //nolint:gosec

	conf.InitDefault()

	return newDriver(op, tracer, prop, log, pq, &conf, pipe)
}

// newDriver builds the driver and its nsqd producer. The consumer is created
// lazily in Run/Resume, once the topic and channel are known.
func newDriver(op errors.Op, tracer *sdktrace.TracerProvider, prop propagation.TextMapPropagator, log *slog.Logger, pq jobs.Queue, conf *config, pipe jobs.Pipeline) (*Driver, error) {
	producer, err := nsq.NewProducer(conf.Addr, conf.nsqConfig())
	if err != nil {
		return nil, errors.E(op, err)
	}
	producer.SetLogger(&nsqLogger{log: log}, nsq.LogLevelWarning)

	// fail fast if nsqd is unreachable
	if err := producer.Ping(); err != nil {
		producer.Stop()
		return nil, errors.E(op, err)
	}

	d := &Driver{
		log:      log,
		pq:       pq,
		tracer:   tracer,
		prop:     prop,
		conf:     conf,
		producer: producer,
	}
	d.pipeline.Store(&pipe)

	return d, nil
}

func (d *Driver) Push(ctx context.Context, job jobs.Message) error {
	const op = errors.Op("nsq_driver_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_push")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != job.GroupID() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", job.GroupID(), pipe.Name()))
	}

	if err := d.handleItem(ctx, fromJob(job)); err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	const op = errors.Op("nsq_driver_run")
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_run")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s, actual: %s", p.Name(), pipe.Name()))
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.startConsumer(); err != nil {
		return errors.E(op, err)
	}
	d.listeners.Store(1)

	d.log.Debug("pipeline was started",
		"driver", pipe.Driver(), "pipeline", pipe.Name(), "topic", d.conf.Topic, "channel", d.conf.Channel,
		"start", start, "elapsed", time.Since(start))

	return nil
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_pause")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	if d.listeners.Load() == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	if d.consumer == nil {
		return errors.Str("no active consumer, nothing to pause")
	}

	// stop delivery without tearing the connection down
	d.consumer.ChangeMaxInFlight(0)
	d.listeners.Store(0)

	d.log.Debug("pipeline was paused",
		"driver", pipe.Driver(), "pipeline", pipe.Name(), "start", start, "elapsed", time.Since(start))

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	const op = errors.Op("nsq_driver_resume")
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_resume")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	if d.listeners.Load() == 1 {
		return errors.Str("nsq listener is already in the active state")
	}

	if err := d.startConsumer(); err != nil {
		return errors.E(op, err)
	}
	d.listeners.Store(1)

	d.log.Debug("pipeline was resumed",
		"driver", pipe.Driver(), "pipeline", pipe.Name(), "start", start, "elapsed", time.Since(start))

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_state")
	defer span.End()

	pipe := *d.pipeline.Load()

	// NSQ's Go client exposes no per-channel stats, so (like the Kafka and Pub/Sub
	// drivers) we report identity + readiness, plus a locally tracked delayed count.
	return &jobs.State{
		Priority: uint64(d.conf.Priority), //nolint:gosec
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    d.conf.Topic,
		Delayed:  d.delayed.Load(),
		Ready:    ready(d.listeners.Load()),
	}, nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_stop")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	// Drain BEFORE flagging the driver stopped: in-flight messages are finished by
	// the worker via Item.Ack/Nack, which short-circuit once stopped == 1. Flipping
	// the flag first would block those acks and stall consumer.Stop() until go-nsq's
	// force-exit timeout fires.
	if d.consumer != nil {
		d.consumer.Stop()
		<-d.consumer.StopChan // wait for in-flight handlers to drain
		d.consumer = nil
	}

	d.stopped.Store(1)

	if d.producer != nil {
		d.producer.Stop()
	}

	pipe := *d.pipeline.Load()
	_ = d.pq.Remove(pipe.Name())

	d.log.Debug("pipeline was stopped",
		"driver", pipe.Driver(), "pipeline", pipe.Name(), "start", start, "elapsed", time.Since(start))

	return nil
}

// startConsumer lazily creates and connects the consumer the first time it is
// needed; subsequent calls simply re-enable delivery. Callers must hold d.mu.
func (d *Driver) startConsumer() error {
	if d.consumer != nil {
		d.consumer.ChangeMaxInFlight(d.conf.Prefetch)
		return nil
	}

	consumer, err := nsq.NewConsumer(d.conf.Topic, d.conf.Channel, d.conf.nsqConfig())
	if err != nil {
		return err
	}
	consumer.SetLogger(&nsqLogger{log: d.log}, nsq.LogLevelWarning)
	consumer.AddConcurrentHandlers(&Listener{driver: d}, d.conf.Prefetch)

	if len(d.conf.Lookupd) > 0 {
		err = consumer.ConnectToNSQLookupds(d.conf.Lookupd)
	} else {
		err = consumer.ConnectToNSQD(d.conf.Addr)
	}
	if err != nil {
		consumer.Stop()
		return err
	}

	d.consumer = consumer

	return nil
}

// handleItem serializes the whole item into the message body (NSQ has no broker
// header table) and publishes it, deferring the delivery when a delay is set.
func (d *Driver) handleItem(ctx context.Context, item *Item) error {
	const op = errors.Op("nsq_driver_handle_item")

	if item.Hdrs == nil {
		item.Hdrs = make(map[string][]string, 2)
	}

	// carry the trace context with the message
	d.prop.Inject(ctx, propagation.HeaderCarrier(item.Hdrs))

	body, err := json.Marshal(item)
	if err != nil {
		return errors.E(op, err)
	}

	if delay := item.Options.DelayDuration(); delay > 0 {
		d.delayed.Add(1)
		if err := d.producer.DeferredPublish(d.conf.Topic, delay, body); err != nil {
			d.delayed.Add(-1)
			return errors.E(op, err)
		}
		return nil
	}

	if err := d.producer.Publish(d.conf.Topic, body); err != nil {
		return errors.E(op, err)
	}

	return nil
}

func ready(r uint32) bool {
	return r > 0
}

// nsqLogger adapts the go-nsq logger interface to the structured logger.
type nsqLogger struct {
	log *slog.Logger
}

func (l *nsqLogger) Output(_ int, s string) error {
	l.log.Debug(s)
	return nil
}
