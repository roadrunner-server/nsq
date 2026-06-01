package nsqjobs

import (
	"context"
	"encoding/json"
	"maps"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nsqio/go-nsq"
	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
)

var _ jobs.Job = (*Item)(nil)

const auto string = "deduced_by_rr"

type Item struct {
	// Job contains the name of the job broker (usually a PHP class).
	Job string `json:"job"`
	// Ident is a unique identifier of the job, provided from the outside.
	Ident string `json:"id"`
	// Payload is the string data (usually JSON) passed to the job broker.
	Payload []byte `json:"payload"`
	// Hdrs are the metadata key-value pairs. Exported so the item round-trips
	// through the NSQ message body (NSQ has no broker-side header table).
	Hdrs map[string][]string `json:"headers"`
	// Options contains a set of options specific to job execution.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle a given job.
type Options struct {
	// Priority is the job priority, default - 10.
	Priority int64 `json:"priority"`
	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`
	// Delay defines the delay (in seconds) before execution. Defaults to none.
	Delay int64 `json:"delay,omitempty"`
	// AutoAck option.
	AutoAck bool `json:"auto_ack"`
	// Queue is the NSQ topic the job belongs to.
	Queue string `json:"queue,omitempty"`

	// private --------------------------------------------------------------
	message   *nsq.Message
	requeueFn func(ctx context.Context, item *Item) error
	stopped   *atomic.Uint64
	delayed   *atomic.Int64
}

// DelayDuration returns the delay duration in the form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

// Body returns the job payload.
func (i *Item) Body() []byte {
	return i.Payload
}

func (i *Item) Headers() map[string][]string {
	return i.Hdrs
}

// Context packs the job metadata into a binary payload.
func (i *Item) Context() ([]byte, error) {
	return json.Marshal(struct {
		ID       string              `json:"id"`
		Job      string              `json:"job"`
		Driver   string              `json:"driver"`
		Queue    string              `json:"queue"`
		Headers  map[string][]string `json:"headers"`
		Pipeline string              `json:"pipeline"`
	}{
		ID:       i.Ident,
		Job:      i.Job,
		Driver:   pluginName,
		Headers:  i.Hdrs,
		Queue:    i.Options.Queue,
		Pipeline: i.Options.Pipeline,
	})
}

func (i *Item) Ack() error {
	if i.Options.stopped.Load() == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}

	if i.Options.Delay > 0 {
		i.Options.delayed.Add(-1)
	}

	// the message was already finished in the listener
	if i.Options.AutoAck {
		return nil
	}

	i.Options.message.Finish()

	return nil
}

func (i *Item) Nack() error {
	if i.Options.stopped.Load() == 1 {
		return errors.Str("failed to negatively acknowledge the JOB, the pipeline is probably stopped")
	}

	if i.Options.AutoAck {
		return nil
	}

	// requeue with the server-computed (attempt-based) default delay; the job is
	// redelivered (not terminal), so the delayed counter is left for the eventual ack.
	i.Options.message.Requeue(-1)

	return nil
}

func (i *Item) NackWithOptions(requeue bool, delay int) error {
	if i.Options.stopped.Load() == 1 {
		return errors.Str("failed to negatively acknowledge the JOB, the pipeline is probably stopped")
	}

	if i.Options.AutoAck {
		return nil
	}

	if requeue {
		// NSQ can requeue the same payload natively, with a delay; the job is
		// redelivered (not terminal), so leave the delayed counter for the eventual ack.
		i.Options.message.Requeue(time.Second * time.Duration(delay))
		return nil
	}

	// drop the message — terminal, so release its delayed slot
	if i.Options.Delay > 0 {
		i.Options.delayed.Add(-1)
	}
	i.Options.message.Finish()

	return nil
}

// Requeue re-publishes the job with the provided headers/delay, then finishes
// the original message. NSQ's native requeue cannot carry new headers, so a
// fresh copy is published instead.
func (i *Item) Requeue(headers map[string][]string, delay int) error {
	if i.Options.stopped.Load() == 1 {
		return errors.Str("failed to requeue the JOB, the pipeline is probably stopped")
	}

	// the original message is terminally finished below; release its delayed slot
	// (the fresh copy re-increments via handleItem if it is published with a delay)
	if i.Options.Delay > 0 {
		i.Options.delayed.Add(-1)
	}

	if i.Options.AutoAck {
		return nil
	}

	if i.Hdrs == nil {
		i.Hdrs = make(map[string][]string, 2)
	}

	if len(headers) > 0 {
		maps.Copy(i.Hdrs, headers)
	}

	i.Options.Delay = int64(delay)

	// publish a fresh copy; on failure keep the original in-flight for redelivery
	if err := i.Options.requeueFn(context.Background(), i); err != nil {
		i.Options.message.Requeue(-1)
		return err
	}

	i.Options.message.Finish()

	return nil
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		Hdrs:    job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),
		},
	}
}

// unpack decodes an item from the NSQ message body and wires the broker handles.
// Messages not produced by RoadRunner (e.g. raw publishes) are wrapped into a
// synthetic item carrying the raw bytes as payload.
func (d *Driver) unpack(msg *nsq.Message) *Item {
	pipe := *d.pipeline.Load()

	item := &Item{}
	if err := json.Unmarshal(msg.Body, item); err != nil || item.Options == nil {
		if err != nil {
			d.log.Debug("failed to unpack the message body, using a synthetic item", "error", err)
		}
		item = &Item{
			Job:     auto,
			Ident:   uuid.NewString(),
			Payload: msg.Body,
			Options: &Options{},
		}
	}

	if item.Hdrs == nil {
		item.Hdrs = make(map[string][]string, 2)
	}

	if item.Options.Priority == 0 {
		item.Options.Priority = d.conf.Priority
	}

	item.Options.Pipeline = pipe.Name()
	item.Options.Queue = d.conf.Topic
	item.Options.message = msg
	item.Options.stopped = &d.stopped
	item.Options.delayed = &d.delayed
	item.Options.requeueFn = d.handleItem

	return item
}
