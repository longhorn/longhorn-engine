package controller

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
	"github.com/rancher/longhorn/util"
)

type Controller struct {
	sync.RWMutex
	Name       string
	size       int64
	sectorSize int64
	replicas   []types.Replica
	factory    types.BackendFactory
	backend    *replicator
	frontend   types.Frontend
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

func (c *Controller) hasWOReplica() bool {
	for _, i := range c.replicas {
		if i.Mode == types.WO {
			return true
		}
	}
	return false
}

func (c *Controller) canAdd(address string) (bool, error) {
	if c.hasReplica(address) {
		return false, nil
	}
	if c.hasWOReplica() {
		return false, fmt.Errorf("Can only have one WO replica at a time")
	}
	return true, nil
}

func (c *Controller) addReplica(address string, snapshot bool) error {
	c.Lock()
	if ok, err := c.canAdd(address); !ok {
		c.Unlock()
		return err
	}
	c.Unlock()

	newBackend, err := c.factory.Create(address)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	return c.addReplicaNoLock(newBackend, address, snapshot)
}

func (c *Controller) Snapshot(name string) (string, error) {
	c.Lock()
	defer c.Unlock()

	if name == "" {
		name = util.UUID()
	}

	return name, c.backend.Snapshot(name)
}

func (c *Controller) addReplicaNoLock(newBackend types.Backend, address string, snapshot bool) error {
	if ok, err := c.canAdd(address); !ok {
		return err
	}

	if snapshot {
		uuid := util.UUID()

		if err := c.backend.Snapshot(uuid); err != nil {
			newBackend.Close()
			return err
		}
		if err := newBackend.Snapshot(uuid); err != nil {
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
		defer c.RUnlock()
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
			if err := c.frontend.Activate(c.Name, c.size, c.sectorSize, c); err != nil {
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

		newSectorSize, err := newBackend.SectorSize()
		if err != nil {
			return err
		}

		if first {
			first = false
			c.size = newSize
			c.sectorSize = newSectorSize
		} else if c.size != newSize {
			return fmt.Errorf("Backend sizes do not match %d != %d", c.size, newSize)
		} else if c.sectorSize != newSectorSize {
			return fmt.Errorf("Backend sizes do not match %d != %d", c.sectorSize, newSectorSize)
		}

		if err := c.addReplicaNoLock(newBackend, address, false); err != nil {
			return err
		}
		c.setReplicaModeNoLock(address, types.RW)
	}

	return nil
}

func (c *Controller) WriteAt(b []byte, off int64) (int, error) {
	c.RLock()
	n, err := c.backend.WriteAt(b, off)
	c.RUnlock()
	if err != nil {
		return n, c.handleError(err)
	}
	return n, err
}

func (c *Controller) ReadAt(b []byte, off int64) (int, error) {
	c.RLock()
	n, err := c.backend.ReadAt(b, off)
	c.RUnlock()
	if err != nil {
		return n, c.handleError(err)
	}
	return n, err
}

func (c *Controller) handleError(err error) error {
	if bErr, ok := err.(*BackendError); ok {
		c.Lock()
		if len(bErr.Errors) > 0 {
			for address := range bErr.Errors {
				c.setReplicaModeNoLock(address, types.ERR)
			}
			// if we still have a good replica, do not return error
			for _, r := range c.replicas {
				if r.Mode == types.RW {
					err = nil
					break
				}
			}
		}
		c.Unlock()
	}
	return err
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
