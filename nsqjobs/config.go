package nsqjobs

import (
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
)

// pipeline option keys.
const (
	topicKey       string = "topic"
	channelKey     string = "channel"
	prefetchKey    string = "prefetch"
	maxAttemptsKey string = "max_attempts"
)

const (
	defaultAddr    string = "127.0.0.1:4150"
	defaultChannel string = "default"
	defaultTimeout        = time.Second
)

// config holds both the global `nsq` section and the per-pipeline options.
// The two are merged in FromConfig (global values overlay the pipeline section).
type config struct {
	// global ------------------------------------------------------------

	// Addr is the nsqd TCP address used by the producer and, by default, the consumer.
	Addr string `mapstructure:"addr"`
	// Lookupd is an optional list of nsqlookupd HTTP addresses used for consumer discovery.
	// When set, the consumer connects via nsqlookupd instead of dialing nsqd directly.
	Lookupd []string `mapstructure:"lookupd"`
	// DialTimeout is the timeout for dialing nsqd.
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	// ReadTimeout is the network read timeout.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	// WriteTimeout is the network write timeout.
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// local -------------------------------------------------------------

	// Topic is the NSQ topic to publish to and consume from. Defaults to the pipeline name.
	Topic string `mapstructure:"topic"`
	// Channel is the NSQ channel the consumer subscribes to.
	Channel string `mapstructure:"channel"`
	// Priority is the default pipeline priority.
	Priority int64 `mapstructure:"priority"`
	// Prefetch is the maximum number of in-flight messages (NSQ max_in_flight).
	Prefetch int `mapstructure:"prefetch"`
	// MaxAttempts is the maximum number of delivery attempts before NSQ gives up (0 = unlimited).
	MaxAttempts uint16 `mapstructure:"max_attempts"`
}

func (c *config) InitDefault() {
	if c.Addr == "" {
		c.Addr = defaultAddr
	}
	// go-nsq wants a bare host:port; accept addresses written with the tcp:// scheme too
	c.Addr = strings.TrimPrefix(c.Addr, "tcp://")

	if c.Channel == "" {
		c.Channel = defaultChannel
	}

	if c.Prefetch <= 0 {
		c.Prefetch = 10
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.DialTimeout == 0 {
		c.DialTimeout = defaultTimeout
	}
}

// nsqConfig builds a go-nsq client configuration from the parsed options.
// A fresh instance is returned on each call so the producer and consumer
// never share the same (internally locked) *nsq.Config.
func (c *config) nsqConfig() *nsq.Config {
	ncfg := nsq.NewConfig()

	// DialTimeout and Prefetch are always set by InitDefault
	ncfg.DialTimeout = c.DialTimeout
	ncfg.MaxInFlight = c.Prefetch

	// the rest are optional and left at the go-nsq defaults when unset
	if c.ReadTimeout > 0 {
		ncfg.ReadTimeout = c.ReadTimeout
	}

	if c.WriteTimeout > 0 {
		ncfg.WriteTimeout = c.WriteTimeout
	}

	if c.MaxAttempts > 0 {
		ncfg.MaxAttempts = c.MaxAttempts
	}

	return ncfg
}
