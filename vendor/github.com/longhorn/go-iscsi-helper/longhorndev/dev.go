package longhorndev

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/iscsiblk"
	"github.com/longhorn/go-iscsi-helper/util"
)

const (
	FrontendTGTBlockDev = "tgt-blockdev"
	FrontendTGTISCSI    = "tgt-iscsi"

	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"

	WaitInterval = time.Second
	WaitCount    = 60

	SwitchWaitInterval = time.Second
	SwitchWaitCount    = 15
)

type LonghornDevice struct {
	*sync.RWMutex
	name     string //VolumeName
	size     int64
	frontend string
	endpoint string

	scsiDevice *iscsiblk.ScsiDevice
}

func NewLonghornDevice(name string, size int64, frontend string) (*LonghornDevice, error) {
	if name == "" || size == 0 {
		return nil, fmt.Errorf("invalid parameter for creating Longhorn device")
	}
	dev := &LonghornDevice{
		RWMutex: &sync.RWMutex{},
		name:    name,
		size:    size,
	}
	if err := dev.SetFrontend(frontend); err != nil {
		return nil, err
	}
	return dev, nil

}

func (d *LonghornDevice) Start(tID int) error {
	d.Lock()
	defer d.Unlock()

	if d.scsiDevice != nil {
		return nil
	}

	stopCh := make(chan struct{})
	if err := <-d.WaitForSocket(stopCh); err != nil {
		return err
	}

	bsOpts := fmt.Sprintf("size=%v", d.size)
	scsiDev, err := iscsiblk.NewScsiDevice(d.name, d.GetSocketPath(), "longhorn", bsOpts, tID)
	if err != nil {
		return err
	}
	d.scsiDevice = scsiDev

	switch d.frontend {
	case FrontendTGTBlockDev:
		if err := iscsiblk.StartScsi(d.scsiDevice); err != nil {
			return err
		}
		if err := d.createDev(); err != nil {
			return err
		}

		d.endpoint = d.getDev()

		logrus.Infof("device %v: SCSI device %s created", d.name, d.scsiDevice.Device)
		break
	case FrontendTGTISCSI:
		if err := iscsiblk.SetupTarget(d.scsiDevice); err != nil {
			return err
		}

		d.endpoint = d.scsiDevice.Target

		logrus.Infof("device %v: iSCSI target %s created", d.name, d.scsiDevice.Target)
		break
	default:
		return fmt.Errorf("unknown frontend %v", d.frontend)
	}

	logrus.Debugf("device %v: frontend start succeed", d.name)
	return nil
}

func (d *LonghornDevice) Shutdown() (int, error) {
	d.Lock()
	defer d.Unlock()

	if d.scsiDevice == nil {
		return 0, nil
	}

	switch d.frontend {
	case FrontendTGTBlockDev:
		dev := d.getDev()
		if err := util.RemoveDevice(dev); err != nil {
			return 0, fmt.Errorf("device %v: fail to remove device %s: %v", d.name, dev, err)
		}
		if err := iscsiblk.StopScsi(d.name, d.scsiDevice.TargetID); err != nil {
			return 0, fmt.Errorf("device %v: fail to stop SCSI device: %v", d.name, err)
		}
		logrus.Infof("device %v: SCSI device %v shutdown", d.name, dev)
		break
	case FrontendTGTISCSI:
		if err := iscsiblk.DeleteTarget(d.scsiDevice.Target, d.scsiDevice.TargetID); err != nil {
			return 0, fmt.Errorf("device %v: fail to delete target %v", d.name, d.scsiDevice.Target)
		}
		logrus.Infof("device %v: SCSI target %v ", d.name, d.scsiDevice.Target)
		break
	case "":
		logrus.Infof("device %v: skip shutdown frontend since it's not enabled", d.name)
		break
	default:
		return 0, fmt.Errorf("device %v: unknown frontend %v", d.name, d.frontend)
	}

	tID := d.scsiDevice.TargetID
	d.scsiDevice = nil
	d.endpoint = ""

	return tID, nil
}

func (d *LonghornDevice) WaitForSocket(stopCh chan struct{}) chan error {
	errCh := make(chan error)
	go func(errCh chan error, stopCh chan struct{}) {
		socket := d.GetSocketPath()
		timeout := time.After(time.Duration(WaitCount) * WaitInterval)
		tick := time.Tick(WaitInterval)
		for {
			select {
			case <-timeout:
				errCh <- fmt.Errorf("device %v: wait for socket %v timed out", d.name, socket)
			case <-tick:
				if _, err := os.Stat(socket); err == nil {
					errCh <- nil
					return
				}
				logrus.Infof("device %v: waiting for socket %v to show up", d.name, socket)
			case <-stopCh:
				logrus.Infof("device %v: stop wait for socket routine", d.name)
				return
			}
		}
	}(errCh, stopCh)

	return errCh
}

func (d *LonghornDevice) GetSocketPath() string {
	return filepath.Join(SocketDirectory, "longhorn-"+d.name+".sock")
}

// call with lock hold
func (d *LonghornDevice) getDev() string {
	return filepath.Join(DevPath, d.name)
}

// call with lock hold
func (d *LonghornDevice) createDev() error {
	if _, err := os.Stat(DevPath); os.IsNotExist(err) {
		if err := os.MkdirAll(DevPath, 0755); err != nil {
			logrus.Fatalf("device %v: Cannot create directory %v", d.name, DevPath)
		}
	}

	dev := d.getDev()
	if _, err := os.Stat(dev); err == nil {
		logrus.Warnf("Device %s already exists, clean it up", dev)
		if err := util.RemoveDevice(dev); err != nil {
			return errors.Wrapf(err, "cannot cleanup block device file %v", dev)
		}
	}

	if err := util.DuplicateDevice(d.scsiDevice.Device, dev); err != nil {
		return err
	}

	logrus.Debugf("device %v: Device %s is ready", d.name, dev)

	return nil
}

func (d *LonghornDevice) PrepareUpgrade() error {
	if err := util.RemoveFile(d.GetSocketPath()); err != nil {
		return errors.Wrapf(err, "failed to remove socket %v", d.GetSocketPath())
	}
	return nil
}

func (d *LonghornDevice) FinishUpgrade() (err error) {
	stopCh := make(chan struct{})
	socketError := d.WaitForSocket(stopCh)
	select {
	case err = <-socketError:
		if err != nil {
			logrus.Errorf("error waiting for the socket %v", err)
			err = errors.Wrapf(err, "error waiting for the socket")
		}
		break
	}
	close(stopCh)
	close(socketError)

	if err != nil {
		return err
	}

	if err = d.ReloadSocketConnection(); err != nil {
		return err
	}

	return nil
}

func (d *LonghornDevice) ReloadSocketConnection() error {
	d.RLock()
	dev := d.getDev()
	d.RUnlock()

	cmd := exec.Command("sg_raw", dev, "a6", "00", "00", "00", "00", "00")
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to reload socket connection")
	}
	return nil
}

func (d *LonghornDevice) SetFrontend(frontend string) error {
	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI {
		return fmt.Errorf("invalid frontend %v", frontend)
	}

	d.Lock()
	defer d.Unlock()
	if d.frontend != "" {
		if d.frontend != frontend {
			return fmt.Errorf("engine frontend %v is already up and cannot be set to %v", d.frontend, frontend)
		}
		if d.scsiDevice != nil {
			logrus.Infof("Engine frontend %v is already up", frontend)
			return nil
		}
		// d.scsiDevice == nil
		return fmt.Errorf("engine frontend had been set to %v, but its frontend cannot be started before engine manager shutdown its frontend", frontend)
	}

	if d.scsiDevice != nil {
		return fmt.Errorf("BUG: engine launcher frontend is empty but scsi device hasn't been cleanup in frontend start")
	}

	d.frontend = frontend

	return nil
}

func (d *LonghornDevice) Enabled() bool {
	d.RLock()
	defer d.RUnlock()
	return d.scsiDevice != nil
}

func (d *LonghornDevice) GetEndpoint() string {
	d.RLock()
	defer d.RUnlock()
	return d.endpoint
}
