package controller

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

type Controller struct {
	sync.RWMutex
	Name     string
	size     int64
	replicas []types.Replica
	factory  types.BackendFactory
	backend  *replicator
	frontend types.Frontend
}

func NewController(name string, factory types.BackendFactory, frontend types.Frontend) *Controller {
	c := &Controller{
		factory:  factory,
		Name:     name,
		frontend: frontend,
	}
	c.reset()
	return c
}

func (c *Controller) AddReplica(address string) error {
	return c.addReplica(address, true)
}

func (c *Controller) addReplica(address string, snapshot bool) error {
	newBackend, err := c.factory.Create(address)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	return c.addReplicaNoLock(newBackend, address, snapshot)
}

func (c *Controller) Snapshot() error {
	c.Lock()
	defer c.Unlock()

	return c.backend.Snapshot()
}

func (c *Controller) addReplicaNoLock(newBackend types.Backend, address string, snapshot bool) error {
	if c.hasReplica(address) {
		newBackend.Close()
		return nil
	}

	if snapshot {
		if err := c.backend.Snapshot(); err != nil {
			newBackend.Close()
			return err
		}
	}

	c.replicas = append(c.replicas, types.Replica{
		Address: address,
		Mode:    types.WO,
	})

	c.backend.AddBackend(address, newBackend)

	return nil
}

func (c *Controller) hasReplica(address string) bool {
	for _, i := range c.replicas {
		if i.Address == address {
			return true
		}
	}
	return false
}

func (c *Controller) RemoveReplica(address string) error {
	c.Lock()
	defer c.Unlock()

	if !c.hasReplica(address) {
		return nil
	}

	for i, r := range c.replicas {
		if r.Address == address {
			c.replicas = append(c.replicas[:i], c.replicas[i+1:]...)
			c.backend.RemoveBackend(r.Address)
		}
	}

	return nil
}

func (c *Controller) ListReplicas() []types.Replica {
	return c.replicas
}

func (c *Controller) SetReplicaMode(address string, mode types.Mode) error {
	switch mode {
	case types.ERR:
		c.Lock()
		defer c.Unlock()
	case types.RW:
		c.RLock()
		c.RUnlock()
	default:
		return fmt.Errorf("Can not set to mode %s", mode)
	}

	c.setReplicaModeNoLock(address, mode)
	return nil
}

func (c *Controller) setReplicaModeNoLock(address string, mode types.Mode) {
	for i, r := range c.replicas {
		if r.Mode != types.ERR && r.Address == address {
			r.Mode = mode
			c.replicas[i] = r
			c.backend.SetMode(address, mode)
		}
	}
}

func (c *Controller) Start(addresses ...string) error {
	c.Lock()
	defer c.Unlock()

	if len(addresses) == 0 {
		return nil
	}

	if len(c.replicas) > 0 {
		return nil
	}

	c.reset()

	defer func() {
		if len(c.replicas) > 0 && c.frontend != nil {
			if err := c.frontend.Activate(c.Name, c.size, c); err != nil {
				// FATAL
				logrus.Fatalf("Failed to activate frontend: %v", err)
			}
		}
	}()

	first := true
	for _, address := range addresses {
		newBackend, err := c.factory.Create(address)
		if err != nil {
			return err
		}

		newSize, err := newBackend.Size()
		if err != nil {
			return err
		}

		if first {
			first = false
			c.size = newSize
		} else if c.size != newSize {
			return fmt.Errorf("Backend sizes do not match %d != %d", c.size, newSize)
		}

		if err := c.addReplicaNoLock(newBackend, address, false); err != nil {
			return err
		}
		c.setReplicaModeNoLock(address, types.RW)
	}

	return nil
}

func (c *Controller) WriteAt(b []byte, off int64) (n int, err error) {
	return c.backend.WriteAt(b, off)
}

func (c *Controller) ReadAt(b []byte, off int64) (n int, err error) {
	return c.backend.ReadAt(b, off)
}

func (c *Controller) reset() {
	c.replicas = []types.Replica{}
	c.backend = &replicator{}
}

func (c *Controller) Close() error {
	return c.Shutdown()
}

func (c *Controller) Shutdown() error {
	c.Lock()
	defer c.Unlock()

	c.backend.Close()
	c.reset()

	if c.frontend != nil {
		return c.frontend.Shutdown()
	}

	return nil
}

func (c *Controller) Size() (int64, error) {
	return c.size, nil
}
