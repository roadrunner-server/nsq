//go:build !race

// go-nsq's Producer has an internal data race in its reconnect path (connect()
// vs. the previous connection's router(), present through the 2025 master). It
// only triggers on redial, so this test cannot run under the race detector. The
// rest of the suite does.
package tests

import (
	"testing"
	"time"

	"tests/helpers"
)

const (
	durabilityAddr = "127.0.0.1:6001"
	// proxyName fronts nsqd on 4155, which the durability config dials. Both
	// addresses are inside the compose network; 4155 is published to the host.
	proxyName     = "redial"
	proxyListen   = "0.0.0.0:4155"
	proxyUpstream = "nsqd:4150"
	// bound for the reconnect; the config sets lookupd_poll_interval to 1s, and
	// go-nsq waits that long before re-dialing a dropped connection
	reconnectWait = time.Second * 60
)

// TestRedialAfterOutage cuts the connection to nsqd underneath a running
// pipeline and checks the driver recovers once it comes back. The old test made
// the same calls behind 27 seconds of sleeps and asserted nothing.
func TestRedialAfterOutage(t *testing.T) {
	helpers.CreateProxy(t, proxyName, proxyListen, proxyUpstream)

	rr, _ := helpers.Start(t, "configs/.rr-nsq-durability-redial.yaml", jobsPlugins(),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(durabilityAddr),
	)

	rr.RequireLogCount(t, "pipeline was started", 2)

	helpers.PushToPipe("test-1", false, durabilityAddr)(t)
	helpers.PushToPipe("test-2", false, durabilityAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.SetProxyEnabled(t, proxyName, false)
	helpers.SetProxyEnabled(t, proxyName, true)

	// the producer and both consumers have to redial before these land
	helpers.PushEventually(t, durabilityAddr, "test-1")
	helpers.PushEventually(t, durabilityAddr, "test-2")

	rr.WaitLogWithin(t, "job was processed successfully", 4, reconnectWait)

	helpers.DestroyPipelines(durabilityAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "pipeline was stopped", 2)
}
