package nsq

import (
	"context"
	"log/slog"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/nsq/v6/nsqjobs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var _ jobs.Constructor = (*Plugin)(nil)

const pluginName string = "nsq"

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshals it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *slog.Logger
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

type Plugin struct {
	log    *slog.Logger
	cfg    Configurer
	tracer *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(pluginName) {
		return errors.E(errors.Disabled)
	}

	p.log = log.NamedLogger(pluginName)
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

// DriverFromConfig constructs an NSQ driver from the .rr.yaml configuration.
func (p *Plugin) DriverFromConfig(ctx context.Context, configKey string, pq jobs.Queue, pipeline jobs.Pipeline) (jobs.Driver, error) {
	return nsqjobs.FromConfig(ctx, p.tracer, configKey, p.log, p.cfg, pipeline, pq)
}

// DriverFromPipeline constructs an NSQ driver from a pipeline.
func (p *Plugin) DriverFromPipeline(ctx context.Context, pipe jobs.Pipeline, pq jobs.Queue) (jobs.Driver, error) {
	return nsqjobs.FromPipeline(ctx, p.tracer, pipe, p.log, p.cfg, pq)
}
