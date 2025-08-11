package nsqjobs

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	xRoutingKey        = "x-routing-key"
	pluginName  string = "nsq"
	tracerName  string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	mu       sync.RWMutex
	log      *zap.Logger
	pq       jobs.Queue
	pipeline atomic.Pointer[jobs.Pipeline]
	tracer   *sdktrace.TracerProvider
	prop     propagation.TextMapPropagator

	// nsq
	consumer *nsq.Consumer

	listeners uint32
	delayed   *int64
	stopCh    chan struct{}
	stopped   uint64
}

// FromConfig initializes NSQ pipeline
func FromConfig(tracer *sdktrace.TracerProvider, configKey string, log *zap.Logger, cfg Configurer, pipeline jobs.Pipeline, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_nsq_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)
	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nsq configuration, global configuration should contain nsq addrs"))
	}

	// PARSE CONFIGURATION START -------
	var conf config
	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = conf.initDefault()
	if err != nil {
		return nil, err
	}
	// PARSE CONFIGURATION END -------

	ncfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("", "", ncfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = consumer.ConnectToNSQLookupds([]string{""})
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb := &Driver{
		tracer: tracer,
		prop:   prop,
		log:    log,
		pq:     pq,
		stopCh: make(chan struct{}, 1),
		// nsq
		consumer: consumer,
	}

	jb.pipeline.Store(&pipeline)

	return jb, nil
}

// FromPipeline initializes consumer from pipeline
func FromPipeline(tracer *sdktrace.TracerProvider, pipeline jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_nsq_consumer_from_pipeline")
	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)
	// only global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global nsq configuration, global configuration should contain nsq addrs"))
	}

	// PARSE CONFIGURATION -------
	var conf config
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}
	err = conf.initDefault()
	if err != nil {
		return nil, err
	}
	// PARSE CONFIGURATION -------

	// parse prefetch
	prf, err := strconv.Atoi(pipeline.String(prefetch, "10"))
	if err != nil {
		log.Error("prefetch parse, driver will use default (10) prefetch", zap.String("prefetch", pipeline.String(prefetch, "10")))
	}

	_ = prf

	ncfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("", "", ncfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = consumer.ConnectToNSQLookupds([]string{""})
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb := &Driver{
		prop:    prop,
		tracer:  tracer,
		log:     log,
		pq:      pq,
		stopCh:  make(chan struct{}, 1),
		delayed: ptrTo(int64(0)),
	}

	return jb, nil
}

func (d *Driver) Push(ctx context.Context, job jobs.Message) error {
	const op = errors.Op("nsq_driver_push")
	// check if the pipeline registered

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_push")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != job.GroupID() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", job.GroupID(), pipe.Name()))
	}

	// err := d.handleItem(ctx, fromJob(job))
	// if err != nil {
	// 	return errors.E(op, err)
	// }

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	start := time.Now().UTC()
	const op = errors.Op("nsq_driver_run")

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_run")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	// if d.queue == "" {
	// 	return errors.Str("empty queue name, consider adding the queue name to the NSQ configuration")
	// }

	// protect connection (redial)
	d.mu.Lock()
	defer d.mu.Unlock()

	// declare/bind/check the queue

	atomic.StoreUint32(&d.listeners, 1)
	d.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	const op = errors.Op("nsq_driver_state")
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_state")
	defer span.End()

	return nil, nil
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()
	pipe := *d.pipeline.Load()

	// todo
	_ = start

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_pause")
	defer span.End()

	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	// no active listeners
	if atomic.LoadUint32(&d.listeners) == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	atomic.AddUint32(&d.listeners, ^uint32(0))

	// protect connection (redial)
	d.mu.Lock()
	defer d.mu.Unlock()

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()

	//todo
	_ = start

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_resume")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	// protect connection (redial)
	d.mu.Lock()
	defer d.mu.Unlock()

	// no active listeners
	if atomic.LoadUint32(&d.listeners) == 1 {
		return errors.Str("nsq listener is already in the active state")
	}

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "nsq_stop")
	defer span.End()

	atomic.StoreUint64(&d.stopped, 1)
	d.stopCh <- struct{}{}

	pipe := *d.pipeline.Load()
	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

// handleItem
func (d *Driver) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("nsq_driver_handle_item")

	return nil
}

func ready(r uint32) bool {
	return r > 0
}

func ptrTo[T any](val T) *T {
	return &val
}
