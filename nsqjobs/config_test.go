package nsqjobs

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	c := &config{}
	c.InitDefault()

	require.Equal(t, defaultAddr, c.Addr)
	require.Equal(t, defaultChannel, c.Channel)
	require.Equal(t, 10, c.Prefetch)
	require.Equal(t, int64(10), c.Priority)
	require.Equal(t, defaultTimeout, c.DialTimeout)
}

// TestConfigTrimsScheme covers addresses written the RoadRunner way. go-nsq
// dials a bare host:port and rejects anything carrying a scheme.
func TestConfigTrimsScheme(t *testing.T) {
	c := &config{Addr: "tcp://127.0.0.1:4150"}
	c.InitDefault()

	require.Equal(t, "127.0.0.1:4150", c.Addr)
}

func TestConfigKeepsExplicitValues(t *testing.T) {
	c := &config{
		Addr:        "nsqd:4150",
		Channel:     "worker",
		Prefetch:    64,
		Priority:    3,
		DialTimeout: time.Second * 5,
	}
	c.InitDefault()

	require.Equal(t, "nsqd:4150", c.Addr)
	require.Equal(t, "worker", c.Channel)
	require.Equal(t, 64, c.Prefetch)
	require.Equal(t, int64(3), c.Priority)
	require.Equal(t, time.Second*5, c.DialTimeout)
}

// TestConfigNegativePrefetch covers a prefetch that would leave the consumer
// unable to take anything.
func TestConfigNegativePrefetch(t *testing.T) {
	c := &config{Prefetch: -1}
	c.InitDefault()

	require.Equal(t, 10, c.Prefetch)
}

// TestNsqConfigCarriesDefaults checks the values InitDefault always sets reach
// the go-nsq config, and that the optional ones are left where go-nsq put them.
func TestNsqConfigCarriesDefaults(t *testing.T) {
	c := &config{}
	c.InitDefault()

	got := c.nsqConfig()
	base := nsq.NewConfig()

	require.Equal(t, defaultTimeout, got.DialTimeout)
	require.Equal(t, 10, got.MaxInFlight)
	require.Equal(t, base.ReadTimeout, got.ReadTimeout)
	require.Equal(t, base.WriteTimeout, got.WriteTimeout)
	require.Equal(t, base.MaxAttempts, got.MaxAttempts)
	require.Equal(t, base.LookupdPollInterval, got.LookupdPollInterval)
}

func TestNsqConfigCarriesOptionalValues(t *testing.T) {
	c := &config{
		ReadTimeout:         time.Second * 7,
		WriteTimeout:        time.Second * 11,
		MaxAttempts:         3,
		LookupdPollInterval: time.Second * 2,
	}
	c.InitDefault()

	got := c.nsqConfig()

	require.Equal(t, time.Second*7, got.ReadTimeout)
	require.Equal(t, time.Second*11, got.WriteTimeout)
	require.Equal(t, uint16(3), got.MaxAttempts)
	require.Equal(t, time.Second*2, got.LookupdPollInterval)
}

// TestNsqConfigIsNotShared covers the reason a fresh config is built per call:
// go-nsq locks a config once it is handed to a client, so the producer and the
// consumer cannot share one.
func TestNsqConfigIsNotShared(t *testing.T) {
	c := &config{}
	c.InitDefault()

	require.NotSame(t, c.nsqConfig(), c.nsqConfig())
}

func TestReady(t *testing.T) {
	require.False(t, ready(0))
	require.True(t, ready(1))
}

// TestNsqLoggerForwards checks go-nsq's own output reaches the plugin logger.
func TestNsqLoggerForwards(t *testing.T) {
	rec := &recorder{}

	require.NoError(t, (&nsqLogger{log: slog.New(rec)}).Output(0, "connecting to nsqd"))
	require.Equal(t, []string{"connecting to nsqd"}, rec.records)
}

// recorder is a slog handler keeping the rendered messages.
type recorder struct {
	records []string
}

func (*recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.records = append(r.records, rec.Message)
	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }
