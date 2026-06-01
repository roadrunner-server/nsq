//go:build !race

// go-nsq's Producer has an internal data race in its reconnect path (connect()
// vs. the previous connection's router(), present through the 2025 master). It
// only triggers on redial, so this durability test is functionally correct but
// cannot run under the race detector — hence the !race build constraint. The
// non-durability suite (jobs_nsq_test.go) runs under -race.
package tests

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "google.golang.org/genproto/protobuf/ptype" //nolint:revive,nolintlint

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
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

	"tests/helpers"
)

func TestDurabilityNSQ(t *testing.T) {
	newClient := toxiproxy.NewClient("127.0.0.1:8474")

	// front nsqd (127.0.0.1:4150) with a proxy the test can cut and restore
	_, err := newClient.CreateProxy("redial", "127.0.0.1:4155", "127.0.0.1:4150")
	require.NoError(t, err)
	defer helpers.DeleteProxy("redial", t)

	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-nsq-durability-redial.yaml",
	}

	err = cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&nsqPlugin.Plugin{},
	)
	require.NoError(t, err)

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

	// push while everything is healthy
	t.Run("PushPipeline-1", helpers.PushToPipe("test-1", false, "127.0.0.1:6001"))
	t.Run("PushPipeline-2", helpers.PushToPipe("test-2", false, "127.0.0.1:6001"))
	time.Sleep(time.Second * 2)

	// simulate an nsqd outage
	helpers.DisableProxy("redial", t)
	time.Sleep(time.Second * 5)

	// restore connectivity and let go-nsq reconnect the producer and consumers
	helpers.EnableProxy("redial", t)
	time.Sleep(time.Second * 7)

	// pushes must succeed again after the redial
	t.Run("PushPipelineAfterRedial-1", helpers.PushToPipe("test-1", false, "127.0.0.1:6001"))
	t.Run("PushPipelineAfterRedial-2", helpers.PushToPipe("test-2", false, "127.0.0.1:6001"))

	time.Sleep(time.Second * 10)
	helpers.DestroyPipelines("127.0.0.1:6001", "test-1", "test-2")

	stopCh <- struct{}{}
	wg.Wait()
}
