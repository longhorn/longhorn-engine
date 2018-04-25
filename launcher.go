package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/rancher/longhorn-engine-launcher/rpc"
	"github.com/rancher/longhorn-engine/iscsi"
	"github.com/rancher/longhorn-engine/util"
)

const (
	FrontendTGTBlockDev = "tgt-blockdev"
	FrontendTGTISCSI    = "tgt-iscsi"

	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"

	WaitInterval = time.Second
	WaitCount    = 10
)

type Launcher struct {
	listen         string
	longhornBinary string
	frontend       string
	volumeName     string
	size           int64

	scsiDevice *iscsi.ScsiDevice

	rpcService    *grpc.Server
	rpcShutdownCh chan error

	currentController           *Controller
	currentControllerShutdownCh chan error
}

func NewLauncher(listen, longhornBinary, frontend, volumeName string, size int64) (*Launcher, error) {
	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI {
		return nil, fmt.Errorf("Invalid frontend %v", frontend)
	}
	return &Launcher{
		listen:         listen,
		longhornBinary: longhornBinary,
		frontend:       frontend,
		volumeName:     volumeName,
		size:           size,
	}, nil
}

func (l *Launcher) StartController(c *Controller) error {
	l.currentController = c
	l.currentControllerShutdownCh = c.Start()
	if err := l.startFrontend(); err != nil {
		return err
	}
	return nil
}

func (l *Launcher) ShutdownController(c *Controller) error {
	if err := l.stopFrontend(); err != nil {
		return err
	}
	c.Stop()
	return nil
}

func (l *Launcher) stopFrontend() error {
	switch l.frontend {
	case FrontendTGTBlockDev:
		dev := l.getDev()
		if err := util.RemoveDevice(dev); err != nil {
			return fmt.Errorf("Fail to remove device %s: %v", dev, err)
		}
		if err := iscsi.StopScsi(l.volumeName); err != nil {
			return fmt.Errorf("Fail to stop SCSI device: %v", err)
		}
		logrus.Infof("launcher: SCSI device %v shutdown", dev)
		break
	case FrontendTGTISCSI:
		if err := iscsi.DeleteTarget(l.scsiDevice.Target); err != nil {
			return fmt.Errorf("Fail to delete target %v", l.scsiDevice.Target)
		}
		logrus.Infof("launcher: SCSI target %v ", l.scsiDevice.Target)
		break
	default:
		return fmt.Errorf("unknown frontend %v", l.frontend)
	}
	return nil
}

func (l *Launcher) getDev() string {
	return filepath.Join(DevPath, l.volumeName)
}

func (l *Launcher) GetSocketPath() string {
	if l.volumeName == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-"+l.volumeName+".sock")
}

func (l *Launcher) startFrontend() error {
	if l.scsiDevice == nil {
		bsOpts := fmt.Sprintf("size=%v", l.size)
		scsiDev, err := iscsi.NewScsiDevice(l.volumeName, l.GetSocketPath(), "longhorn", bsOpts)
		if err != nil {
			return err
		}
		l.scsiDevice = scsiDev
	}
	switch l.frontend {
	case FrontendTGTBlockDev:
		if err := iscsi.StartScsi(l.scsiDevice); err != nil {
			return err
		}
		if err := l.createDev(); err != nil {
			return err
		}
		logrus.Infof("launcher: SCSI device %s created", l.scsiDevice.Device)
		break
	case FrontendTGTISCSI:
		if err := iscsi.SetupTarget(l.scsiDevice); err != nil {
			return err
		}
		logrus.Infof("launcher: iSCSI target %s created", l.scsiDevice.Target)
		break
	default:
		return fmt.Errorf("unknown frontend %v", l.frontend)
	}

	return nil
}

func (l *Launcher) createDev() error {
	if err := os.MkdirAll(DevPath, 0700); err != nil {
		logrus.Fatalln("launcher: Cannot create directory ", DevPath)
	}

	dev := l.getDev()
	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	if err := util.DuplicateDevice(l.scsiDevice.Device, dev); err != nil {
		return err
	}
	logrus.Infof("launcher: Device %s is ready", dev)
	return nil
}

func (l *Launcher) WaitForShutdown() error {
	select {
	/*
		case controllerError = <-l.currentControllerShutdownCh:
			logrus.Warnf("Receive controller shutdown: %v", controllerError)
			return controllerError
	*/
	case rpcError := <-l.rpcShutdownCh:
		logrus.Warnf("launcher: Receive rpc shutdown: %v", rpcError)
		return rpcError
	}
	return nil
}

func (l *Launcher) Shutdown() {
	l.ShutdownController(l.currentController)
	controllerError := <-l.currentControllerShutdownCh
	logrus.Info("launcher: Longhorn Engine has been shutdown")
	if controllerError != nil {
		logrus.Warnf("launcher: Engine returns %v", controllerError)
	}
	l.rpcService.Stop()
	logrus.Info("launcher: Longhorn Engine Launcher has been shutdown")
}

func (l *Launcher) StartRPCServer() error {
	listen, err := net.Listen("tcp", l.listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	l.rpcService = grpc.NewServer()
	rpc.RegisterLonghornLauncherServiceServer(l.rpcService, l)
	reflection.Register(l.rpcService)

	l.rpcShutdownCh = make(chan error)
	go func() {
		l.rpcShutdownCh <- l.rpcService.Serve(listen)
	}()
	return nil
}

func (l *Launcher) UpgradeEngine(cxt context.Context, engine *rpc.Engine) (*rpc.Empty, error) {
	oldController := l.currentController
	//oldShutdownCh := l.currentControllerShutdownCh

	if err := oldController.BackupBinary(); err != nil {
		return nil, errors.Wrap(err, "failed to backup old controller binary")
	}
	if err := oldController.SwitchToBackupListenPort(); err != nil {
		return nil, errors.Wrap(err, "failed to ask old controller to switch listening port")
	}

	binary := oldController.Binary
	if err := l.updateControllerBinary(binary, engine.Binary); err != nil {
		return nil, errors.Wrap(err, "failed to update controller binary")
	}
	if err := rm(l.GetSocketPath()); err != nil {
		return nil, errors.Wrapf(err, "failed to remove socket %v", l.GetSocketPath())
	}
	newController := NewController(binary, l.volumeName, oldController.Listen, oldController.Backends, engine.Replicas)

	newShutdownCh := newController.Start()
	newSocket := false
	for i := 0; i < WaitCount; i++ {
		if _, err := os.Stat(l.GetSocketPath()); err == nil {
			newSocket = true
			break
		}
		logrus.Infof("launcher: wait for new controller to be up")
		time.Sleep(WaitInterval)
	}
	if !newSocket {
		logrus.Errorf("launcher: waiting for the new controller timed out")
		// TODO rollback
		return nil, fmt.Errorf("wait for the new controller timed out")
	}
	if err := l.reloadSocketConnection(); err != nil {
		return nil, errors.Wrap(err, "failed to reload socket connection")
	}
	l.currentController = newController
	l.currentControllerShutdownCh = newShutdownCh

	oldController.RemoveBackupBinary()
	oldController.Stop()
	return &rpc.Empty{}, nil
}

func (l *Launcher) GetInfo(cxt context.Context, empty *rpc.Empty) (*rpc.Info, error) {
	if l.frontend == FrontendTGTBlockDev {
		return &rpc.Info{
			Volume:   l.volumeName,
			Frontend: l.frontend,
			Endpoint: l.getDev(),
		}, nil
	} else if l.frontend == FrontendTGTISCSI {
		return &rpc.Info{
			Volume:   l.volumeName,
			Frontend: l.frontend,
			Endpoint: iscsi.GetTargetName(l.volumeName),
		}, nil
	}
	return nil, fmt.Errorf("unsupported frontend %v", l.frontend)
}

func (l *Launcher) updateControllerBinary(binary, newBinary string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to update controller binary %v to %v", binary, newBinary)
	}()
	if err := rm(binary); err != nil {
		return err
	}
	if err := cp(newBinary, binary); err != nil {
		return err
	}
	return nil
}

func (l *Launcher) reloadSocketConnection() error {
	cmd := exec.Command("sg_raw", l.getDev(), "a6", "00", "00", "00", "00", "00")
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to reload socket connection")
	}
	return nil
}
