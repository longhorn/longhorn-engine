package tgt

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/iscsiblk"
	"github.com/longhorn/longhorn-engine/frontend/socket"
	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

const (
	frontendName = "tgt"

	DevPath = "/dev/longhorn/"

	DefaultTargetID = 1
)

type Tgt struct {
	s *socket.Socket

	isUp       bool
	scsiDevice *iscsiblk.ScsiDevice
}

func New() types.Frontend {
	s := socket.New()
	return &Tgt{s, false, nil}
}

func (t *Tgt) FrontendName() string {
	return frontendName
}

func (t *Tgt) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	if err := t.Shutdown(); err != nil {
		return err
	}

	if err := t.s.Startup(name, size, sectorSize, rw); err != nil {
		return err
	}

	if err := t.startScsiDevice(); err != nil {
		return err
	}

	if err := t.createDev(); err != nil {
		return err
	}

	t.isUp = true

	return nil
}

func (t *Tgt) Shutdown() error {
	if t.s.Volume != "" {
		dev := t.getDev()
		if err := util.RemoveDevice(dev); err != nil {
			return fmt.Errorf("Fail to remove device %s: %v", dev, err)
		}
		if err := iscsiblk.StopScsi(t.s.Volume, t.scsiDevice.TargetID); err != nil {
			return fmt.Errorf("Fail to stop SCSI device: %v", err)
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
		return t.getDev()
	}
	return ""
}

func (t *Tgt) getDev() string {
	return filepath.Join(DevPath, t.s.Volume)
}

func (t *Tgt) startScsiDevice() error {
	if t.scsiDevice == nil {
		bsOpts := fmt.Sprintf("size=%v", t.s.Size)
		scsiDev, err := iscsiblk.NewScsiDevice(t.s.Volume, t.s.GetSocketPath(), "longhorn", bsOpts, DefaultTargetID)
		if err != nil {
			return err
		}
		t.scsiDevice = scsiDev
	}
	if err := iscsiblk.StartScsi(t.scsiDevice); err != nil {
		return err
	}
	logrus.Infof("SCSI device %s created", t.scsiDevice.Device)
	return nil
}

func (t *Tgt) createDev() error {
	if err := os.MkdirAll(DevPath, 0755); err != nil {
		logrus.Fatalln("Cannot create directory ", DevPath)
	}

	dev := t.getDev()
	if _, err := os.Stat(dev); err == nil {
		logrus.Warnf("Device %s already exists, clean it up", dev)
		if err := util.RemoveDevice(dev); err != nil {
			return errors.Wrapf(err, "cannot cleanup block device file %v", dev)
		}
	}

	if err := util.DuplicateDevice(t.scsiDevice.Device, dev); err != nil {
		return err
	}
	logrus.Infof("Device %s is ready", dev)
	return nil
}
