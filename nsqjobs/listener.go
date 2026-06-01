package nsqjobs

import (
	"context"

	"github.com/nsqio/go-nsq"
	"go.opentelemetry.io/otel/propagation"
)

// Listener implements the nsq.Handler interface.
type Listener struct {
	driver *Driver
}

func (l *Listener) HandleMessage(m *nsq.Message) error {
	d := l.driver

	// The PHP worker acknowledges asynchronously, long after this handler
	// returns, so we take over the response lifecycle and never let go-nsq
	// auto-finish (or auto-requeue) the message based on the return value.
	m.DisableAutoResponse()

	item := d.unpack(m)

	ctx := d.prop.Extract(context.Background(), propagation.HeaderCarrier(item.Hdrs))
	ctx, span := d.tracer.Tracer(tracerName).Start(ctx, "nsq_listener")
	defer span.End()

	if item.Options.AutoAck {
		m.Finish()
		d.log.Debug("auto_ack option enabled", "id", item.Ident)
	}

	d.prop.Inject(ctx, propagation.HeaderCarrier(item.Hdrs))

	d.pq.Insert(item)
	d.log.Debug("job was pushed to the priority queue", "queue size", d.pq.Len())

	return nil
}
