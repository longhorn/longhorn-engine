package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine/iscsi"
	"github.com/longhorn/longhorn-engine/util"
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
	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI && frontend != "" {
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
	l.currentControllerShutdownCh = c.Start(l.listen)
	logrus.Infof("launcher: controller %v started", c.ID)
	return nil
}

func (l *Launcher) ShutdownController(c *Controller) error {
	c.Stop()
	logrus.Infof("launcher: controller %v stopping", c.ID)
	return nil
}

func (l *Launcher) shutdownFrontend() error {
	if l.scsiDevice == nil {
		return nil
	}
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
	case "":
		logrus.Infof("launcher: skip shutdown frontend since it's not enabeld")
		break
	default:
		return fmt.Errorf("unknown frontend %v", l.frontend)
	}
	l.scsiDevice = nil
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
	// not going to use it
	stopCh := make(chan struct{})
	if err := <-l.waitForSocket(stopCh); err != nil {
		return err
	}
	if l.scsiDevice == nil {
		bsOpts := fmt.Sprintf("size=%v", l.size)
		scsiDev, err := iscsi.NewScsiDevice(l.volumeName, l.GetSocketPath(), "longhorn", bsOpts)
		if err != nil {
			return err
		}
		l.scsiDevice = scsiDev

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
	}

	return nil
}

func (l *Launcher) createDev() error {
	if err := os.MkdirAll(DevPath, 0755); err != nil {
		logrus.Fatalln("launcher: Cannot create directory ", DevPath)
	}

	dev := l.getDev()
	if _, err := os.Stat(dev); err == nil {
		logrus.Warnf("Device %s already exists, clean it up", dev)
		if err := util.RemoveDevice(dev); err != nil {
			return errors.Wrapf(err, "cannot cleanup block device file %v", dev)
		}
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
	if err := l.shutdownFrontend(); err != nil {
		return
	}
	logrus.Info("launcher: frontend has been shutdown")
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

func (l *Launcher) UpgradeEngine(cxt context.Context, engine *rpc.Engine) (ret *rpc.Empty, err error) {
	oldController := l.currentController
	//oldShutdownCh := l.currentControllerShutdownCh

	if oldController.Binary == engine.Binary {
		return nil, fmt.Errorf("cannot upgrade with the same binary")
	}

	if _, err := os.Stat(engine.Binary); err != nil {
		return nil, errors.Wrap(err, "cannot find the binary to be upgraded")
	}

	if err := oldController.PrepareUpgrade(); err != nil {
		return nil, errors.Wrap(err, "failed to prepare for switch over")
	}

	defer func() {
		if err != nil {
			logrus.Errorf("failed to upgrade: %v", err)
			if err := oldController.RollbackUpgrade(); err != nil {
				logrus.Errorf("failed to rollback upgrade: %v", err)
			}
		}
	}()

	binary := oldController.Binary
	if err := l.updateControllerBinary(binary, engine.Binary); err != nil {
		return nil, errors.Wrap(err, "failed to update controller binary")
	}
	if err := rm(l.GetSocketPath()); err != nil {
		return nil, errors.Wrapf(err, "failed to remove socket %v", l.GetSocketPath())
	}
	newController := NewController(util.UUID(), binary, l.volumeName, oldController.Listen, l.frontend, oldController.Backends, engine.Replicas)

	newShutdownCh := newController.Start(l.listen)
	stopCh := make(chan struct{})
	socketError := l.waitForSocket(stopCh)
	select {
	case err = <-newShutdownCh:
		if err != nil {
			logrus.Errorf("error starting new controller %v", err)
			err = errors.Wrapf(err, "error starting new controller")
		}
		break
	case err = <-socketError:
		if err != nil {
			logrus.Errorf("error waiting for the socket %v", err)
			err = errors.Wrapf(err, "error waiting for the socket")
		}
		break
	}
	close(stopCh)

	if err != nil {
		newController.Stop()
		return nil, errors.Wrapf(err, "cannot start new controller")
	}
	if err := l.reloadSocketConnection(); err != nil {
		newController.Stop()
		return nil, errors.Wrap(err, "failed to reload socket connection")
	}
	l.currentController = newController
	l.currentControllerShutdownCh = newShutdownCh

	oldController.FinalizeUpgrade()
	return &rpc.Empty{}, nil
}

func (l *Launcher) waitForSocket(stopCh chan struct{}) chan error {
	errCh := make(chan error)
	go func(errCh chan error, stopCh chan struct{}) {
		socket := l.GetSocketPath()
		timeout := time.After(time.Duration(WaitCount) * WaitInterval)
		tick := time.Tick(WaitInterval)
		for {
			select {
			case <-timeout:
				errCh <- fmt.Errorf("launcher: wait for socket %v timed out", socket)
			case <-tick:
				if _, err := os.Stat(socket); err == nil {
					errCh <- nil
					return
				}
				logrus.Infof("launcher: wait for socket %v to show up", socket)
			case <-stopCh:
				logrus.Infof("launcher: stop wait for socket routine")
				return
			}
		}
	}(errCh, stopCh)

	return errCh
}

func (l *Launcher) GetInfo(cxt context.Context, empty *rpc.Empty) (*rpc.Info, error) {
	info := &rpc.Info{
		Volume:   l.volumeName,
		Frontend: l.frontend,
	}
	switch l.frontend {
	case FrontendTGTBlockDev:
		info.Endpoint = l.getDev()
		return info, nil
	case FrontendTGTISCSI:
		info.Endpoint = iscsi.GetTargetName(l.volumeName)
		return info, nil
	case "":
		return info, nil
	}
	return nil, fmt.Errorf("BUG: unsupported frontend %v", l.frontend)
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

func (l *Launcher) startEngineFrontend(frontend string) error {
	if l.frontend == frontend {
		logrus.Debugf("Engine frontend %v is already up", frontend)
		return nil
	}
	if l.frontend != "" {
		return fmt.Errorf("cannot set frontend if it's already set")
	}
	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI {
		return fmt.Errorf("Invalid frontend %v", frontend)
	}
	l.frontend = frontend
	// the controller will call back to launcher to start the frontend
	if err := l.currentController.StartFrontend("socket"); err != nil {
		return err
	}
	logrus.Infof("Engine frontend %v has been started", frontend)
	return nil
}

func (l *Launcher) shutdownEngineFrontend() error {
	if l.frontend == "" {
		logrus.Debugf("Engine frontend is already down")
		return nil
	}
	// the controller will call back to launcher to shutdown the frontend
	if err := l.currentController.ShutdownFrontend(); err != nil {
		return err
	}
	l.frontend = ""
	logrus.Info("Engine frontend has been shut down")
	return nil
}

func (l *Launcher) StartFrontend(cxt context.Context, identity *rpc.Identity) (*rpc.Empty, error) {
	if identity.ID != l.currentController.ID {
		logrus.Infof("launcher: Ignore start frontend from %v since it's not the current controller", identity.ID)
		return &rpc.Empty{}, nil
	}
	if l.frontend == "" {
		return &rpc.Empty{}, fmt.Errorf("no frontend started since it's not specified")
	}
	if err := l.startFrontend(); err != nil {
		return &rpc.Empty{}, err
	}
	return &rpc.Empty{}, nil
}

func (l *Launcher) ShutdownFrontend(cxt context.Context, identity *rpc.Identity) (*rpc.Empty, error) {
	if identity.ID != l.currentController.ID {
		logrus.Infof("launcher: Ignore shutdown frontend from %v since it's not the current controller", identity.ID)
		return &rpc.Empty{}, nil
	}
	if err := l.shutdownFrontend(); err != nil {
		return nil, err
	}
	return &rpc.Empty{}, nil
}

func (l *Launcher) StartEngineFrontend(cxt context.Context, frontend *rpc.Frontend) (*rpc.Empty, error) {
	if err := l.startEngineFrontend(frontend.Frontend); err != nil {
		return nil, err
	}
	return &rpc.Empty{}, nil
}

func (l *Launcher) ShutdownEngineFrontend(cxt context.Context, empty *rpc.Empty) (*rpc.Empty, error) {
	if err := l.shutdownEngineFrontend(); err != nil {
		return nil, err
	}
	return &rpc.Empty{}, nil
}
