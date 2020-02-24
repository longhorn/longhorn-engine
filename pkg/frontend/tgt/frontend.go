package tgt

import (
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/longhorndev"
	"github.com/longhorn/longhorn-engine/pkg/frontend/socket"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

const (
	DevPath = "/dev/longhorn/"

	DefaultTargetID = 1
)

type Tgt struct {
	s *socket.Socket

	isUp         bool
	dev          longhorndev.DeviceService
	frontendName string
}

func New(frontendName string) types.Frontend {
	s := socket.New()
	return &Tgt{s, false, nil, frontendName}
}

func (t *Tgt) FrontendName() string {
	return t.frontendName
}

func (t *Tgt) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	if err := t.Shutdown(); err != nil {
		return err
	}

	if err := t.s.Startup(name, size, sectorSize, rw); err != nil {
		return err
	}

	ldc := longhorndev.LonghornDeviceCreator{}
	dev, err := ldc.NewDevice(name, size, t.frontendName)
	if err != nil {
		return err
	}
	t.dev = dev

	if err := t.dev.Start(); err != nil {
		return err
	}

	t.isUp = true

	return nil
}

func (t *Tgt) Shutdown() error {
	if t.dev != nil {
		if err := t.dev.Shutdown(); err != nil {
			return err
		}
	}
	if err := t.s.Shutdown(); err != nil {
		return err
	}
	t.isUp = false

	return nil
}

func (t *Tgt) State() types.State {
	if t.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (t *Tgt) Endpoint() string {
	if t.isUp {
		return t.dev.GetEndpoint()
	}
	return ""
}

func (t *Tgt) Upgrade(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	ldc := longhorndev.LonghornDeviceCreator{}
	dev, err := ldc.NewDevice(name, size, t.frontendName)
	if err != nil {
		return err
	}
	t.dev = dev

	if err := t.dev.PrepareUpgrade(); err != nil {
		return err
	}

	if err := t.s.Startup(name, size, sectorSize, rw); err != nil {
		return err
	}

	if err := t.dev.FinishUpgrade(); err != nil {
		return err
	}
	t.isUp = true
	logrus.Infof("engine: Finish upgrading for %v", name)

	return nil
}

func (t *Tgt) Expand(size int64) error {
	if t.dev != nil {
		return t.dev.Expand(size)
	}
	return nil
}
