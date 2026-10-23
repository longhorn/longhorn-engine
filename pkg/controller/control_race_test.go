package controller

import (
	"sync"
	"testing"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

// newSetReplicaModeTestController returns a minimal Controller with a single RW replica.
func newSetReplicaModeTestController(addr string) *Controller {
	c := &Controller{
		VolumeName: "test-vol",
		backend: &replicator{
			backends: map[string]backendWrapper{
				addr: {backend: &noopBackend{}, mode: types.RW},
			},
		},
	}
	c.replicas = []types.Replica{{Address: addr, Mode: types.RW}}
	return c
}

// noopBackend satisfies types.Backend for SetReplicaMode tests,
// which only calls StopMonitoring on the ERR path.
type noopBackend struct {
	types.Backend
}

func (noopBackend) StopMonitoring() {}

// TestSetReplicaModeRWConcurrent checks that concurrent SetReplicaMode(RW)
// calls are race-free. Run with -race.
func TestSetReplicaModeRWConcurrent(t *testing.T) {
	const addr = "127.0.0.1:10000"
	c := newSetReplicaModeTestController(addr)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.SetReplicaMode(addr, types.RW); err != nil {
				t.Errorf("SetReplicaMode failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if c.replicas[0].Mode != types.RW {
		t.Errorf("unexpected replica mode after SetReplicaMode: %v", c.replicas[0].Mode)
	}
}

// TestSetReplicaModeERRConcurrent verifies the ERR branch stays race-free as well.
func TestSetReplicaModeERRConcurrent(t *testing.T) {
	const addr = "127.0.0.1:10000"
	c := newSetReplicaModeTestController(addr)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.SetReplicaMode(addr, types.ERR); err != nil {
				t.Errorf("SetReplicaMode failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if c.replicas[0].Mode != types.ERR {
		t.Errorf("unexpected replica mode after SetReplicaMode: %v", c.replicas[0].Mode)
	}
}
