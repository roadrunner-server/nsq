package nsqjobs

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
)

/*
 DialTimeout time.Duration `opt:"dial_timeout" default:"1s"`

 // Deadlines for network reads and writes
 ReadTimeout  time.Duration `opt:"read_timeout" min:"100ms" max:"5m" default:"60s"`
 WriteTimeout time.Duration `opt:"write_timeout" min:"100ms" max:"5m" default:"1s"`

 // LocalAddr is the local address to use when dialing an nsqd.
 // If empty, a local address is automatically chosen.
 LocalAddr net.Addr `opt:"local_addr"`

 // Duration between polling lookupd for new producers, and fractional jitter to add to
 // the lookupd pool loop. this helps evenly distribute requests even if multiple consumers
 // restart at the same time
 //
 // NOTE: when not using nsqlookupd, LookupdPollInterval represents the duration of time between
 // reconnection attempts
 LookupdPollInterval time.Duration `opt:"lookupd_poll_interval" min:"10ms" max:"5m" default:"60s"`
 LookupdPollJitter   float64       `opt:"lookupd_poll_jitter" min:"0" max:"1" default:"0.3"`
 LookupdPollTimeout  time.Duration `opt:"lookupd_poll_timeout" default:"1m"`

 // Maximum duration when REQueueing (for doubling of deferred requeue)
 MaxRequeueDelay     time.Duration `opt:"max_requeue_delay" min:"0" max:"60m" default:"15m"`
 DefaultRequeueDelay time.Duration `opt:"default_requeue_delay" min:"0" max:"60m" default:"90s"`

 // Backoff strategy, defaults to exponential backoff. Overwrite this to define alternative backoff algrithms.
 BackoffStrategy BackoffStrategy `opt:"backoff_strategy" default:"exponential"`
 // Maximum amount of time to backoff when processing fails 0 == no backoff
 MaxBackoffDuration time.Duration `opt:"max_backoff_duration" min:"0" max:"60m" default:"2m"`
 // Unit of time for calculating consumer backoff
 BackoffMultiplier time.Duration `opt:"backoff_multiplier" min:"0" max:"60m" default:"1s"`

 // Maximum number of times this consumer will attempt to process a message before giving up
 MaxAttempts uint16 `opt:"max_attempts" min:"0" max:"65535" default:"5"`

 // Duration to wait for a message from an nsqd when in a state where RDY
 // counts are re-distributed (e.g. max_in_flight < num_producers)
 LowRdyIdleTimeout time.Duration `opt:"low_rdy_idle_timeout" min:"1s" max:"5m" default:"10s"`
 // Duration to wait until redistributing RDY for an nsqd regardless of LowRdyIdleTimeout
 LowRdyTimeout time.Duration `opt:"low_rdy_timeout" min:"1s" max:"5m" default:"30s"`
 // Duration between redistributing max-in-flight to connections
 RDYRedistributeInterval time.Duration `opt:"rdy_redistribute_interval" min:"1ms" max:"5s" default:"5s"`

 // Identifiers sent to nsqd representing this client
 // UserAgent is in the spirit of HTTP (default: "<client_library_name>/<version>")
 ClientID  string `opt:"client_id"` // (defaults: short hostname)
 Hostname  string `opt:"hostname"`
 UserAgent string `opt:"user_agent"`

 // Duration of time between heartbeats. This must be less than ReadTimeout
 HeartbeatInterval time.Duration `opt:"heartbeat_interval" default:"30s"`
 // Integer percentage to sample the channel (requires nsqd 0.2.25+)
 SampleRate int32 `opt:"sample_rate" min:"0" max:"99"`

 // To set TLS config, use the following options:
 //
 // tls_v1 - Bool enable TLS negotiation
 // tls_root_ca_file - String path to file containing root CA
 // tls_insecure_skip_verify - Bool indicates whether this client should verify server certificates
 // tls_cert - String path to file containing public key for certificate
 // tls_key - String path to file containing private key for certificate
 // tls_min_version - String indicating the minimum version of tls acceptable ('ssl3.0', 'tls1.0', 'tls1.1', 'tls1.2')
 //
 TlsV1     bool        `opt:"tls_v1"`
 TlsConfig *tls.Config `opt:"tls_config"`

 // Compression Settings
 Deflate      bool `opt:"deflate"`
 DeflateLevel int  `opt:"deflate_level" min:"1" max:"9" default:"6"`
 Snappy       bool `opt:"snappy"`

 // Size of the buffer (in bytes) used by nsqd for buffering writes to this connection
 OutputBufferSize int64 `opt:"output_buffer_size" default:"16384"`
 // Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
 //
 // WARNING: configuring clients with an extremely low
 // (< 25ms) output_buffer_timeout has a significant effect
 // on nsqd CPU usage (particularly with > 50 clients connected).
 OutputBufferTimeout time.Duration `opt:"output_buffer_timeout" default:"250ms"`

 // Maximum number of messages to allow in flight (concurrency knob)
 MaxInFlight int `opt:"max_in_flight" min:"0" default:"1"`

 // The server-side message timeout for messages delivered to this client
 MsgTimeout time.Duration `opt:"msg_timeout" min:"0"`

 // Secret for nsqd authentication (requires nsqd 0.2.29+)
 AuthSecret string `opt:"auth_secret"`
 // Use AuthSecret as 'Authorization: Bearer {AuthSecret}' on lookupd queries
 LookupdAuthorization bool `opt:"skip_lookupd_authorization" default:"true"`
*/

type config struct {
	// DialTimeout is the timeout for dialing a connection
	DialTimeout time.Duration `opt:"dial_timeout" default:"1s"`
	// ReadTimeout is the timeout for network reads
	ReadTimeout time.Duration `opt:"read_timeout" min:"100ms" max:"5m" default:"60s"`
	// WriteTimeout is the timeout for network writes
	WriteTimeout time.Duration `opt:"write_timeout" min:"100ms" max:"5m" default:"1s"`
	// LocalAddr is the local address to use when dialing an nsqd
	LocalAddr net.Addr `opt:"local_addr"`
	// LookupdPollInterval is the duration between polling lookupd for new producers
	LookupdPollInterval time.Duration `opt:"lookupd_poll_interval" min:"10ms" max:"5m" default:"60s"`
	// LookupdPollJitter is the fractional jitter to add to the lookupd pool loop
	LookupdPollJitter float64 `opt:"lookupd_poll_jitter" min:"0" max:"1" default:"0.3"`
	// LookupdPollTimeout is the timeout for polling lookupd
	LookupdPollTimeout time.Duration `opt:"lookupd_poll_timeout" default:"1m"`
	// MaxRequeueDelay is the maximum duration when requeueing
	MaxRequeueDelay time.Duration `opt:"max_requeue_delay" min:"0" max:"60m" default:"15m"`
	// DefaultRequeueDelay is the default duration when requeueing
	DefaultRequeueDelay time.Duration `opt:"default_requeue_delay" min:"0" max:"60m" default:"90s"`
	// BackoffStrategy is the strategy for backoff
	BackoffStrategy nsq.BackoffStrategy `opt:"backoff_strategy" default:"exponential"`
	// MaxBackoffDuration is the maximum duration for backoff
	MaxBackoffDuration time.Duration `opt:"max_backoff_duration" min:"0" max:"60m" default:"2m"`
	// BackoffMultiplier is the unit of time for calculating backoff
	BackoffMultiplier time.Duration `opt:"backoff_multiplier" min:"0" max:"60m" default:"1s"`
	// MaxAttempts is the maximum number of times to attempt processing a message
	MaxAttempts uint16 `opt:"max_attempts" min:"0" max:"65535" default:"5"`
	// LowRdyIdleTimeout is the duration to wait for a message when RDY counts are re-distributed
	LowRdyIdleTimeout time.Duration `opt:"low_rdy_idle_timeout" min:"1s" max:"5m" default:"10s"`
	// LowRdyTimeout is the duration to wait until redistributing RDY
	LowRdyTimeout time.Duration `opt:"low_rdy_timeout" min:"1s" max:"5m" default:"30s"`
	// RDYRedistributeInterval is the duration between redistributing max-in-flight to connections
	RDYRedistributeInterval time.Duration `opt:"rdy_redistribute_interval" min:"1ms" max:"5s" default:"5s"`
	// ClientID is the identifier sent to nsqd representing this client
	ClientID string `opt:"client_id"`
	// Hostname is the hostname sent to nsqd representing this client
	Hostname string `opt:"hostname"`
	// UserAgent is the user agent sent to nsqd representing this client
	UserAgent string `opt:"user_agent"`
	// HeartbeatInterval is the duration of time between heartbeats
	HeartbeatInterval time.Duration `opt:"heartbeat_interval" default:"30s"`
	// SampleRate is the integer percentage to sample the channel
	SampleRate int32 `opt:"sample_rate" min:"0" max:"99"`
	// TlsV1 enables TLS negotiation
	TlsV1 bool `opt:"tls_v1"`
	// TlsConfig is the TLS configuration
	TlsConfig *tls.Config `opt:"tls_config"`
	// Deflate enables deflate compression
	Deflate bool `opt:"deflate"`
	// DeflateLevel is the level of deflate compression
	DeflateLevel int `opt:"deflate_level" min:"1" max:"9" default:"6"`
	// Snappy enables snappy compression
	Snappy bool `opt:"snappy"`
	// OutputBufferSize is the size of the buffer (in bytes) used by nsqd for buffering writes to this connection
	OutputBufferSize int64 `opt:"output_buffer_size" default:"16384"`
	// OutputBufferTimeout is the timeout used by nsqd before flushing buffered writes
	OutputBufferTimeout time.Duration `opt:"output_buffer_timeout" default:"250ms"`
	// MaxInFlight is the maximum number of messages to allow in flight
	MaxInFlight int `opt:"max_in_flight" min:"0" default:"1"`
	// MsgTimeout is the server-side message timeout for messages delivered to this client
	MsgTimeout time.Duration `opt:"msg_timeout" min:"0"`
	// AuthSecret is the secret for nsqd authentication
	AuthSecret string `opt:"auth_secret"`
	// LookupdAuthorization uses AuthSecret as 'Authorization: Bearer {AuthSecret}' on lookupd queries
	LookupdAuthorization bool `opt:"skip_lookupd_authorization" default:"true"`
}

func (c *config) initDefault() error {
	return nil
}

const (
	queue    string = "queue"
	prefetch string = "prefetch"
)
