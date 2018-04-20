package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/rancher/longhorn-engine/iscsi"
	"github.com/rancher/longhorn-engine/util"
	"github.com/yasker/longhorn-engine-launcher/rpc"
)

const (
	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"
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
	if frontend != "tgt-blockdev" && frontend != "tgt-iscsi" {
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
	if err := l.startScsiDevice(); err != nil {
		return err
	}

	if err := l.createDev(); err != nil {
		return err
	}

	return nil
}

func (l *Launcher) ShutdownController(c *Controller) error {
	dev := l.getDev()
	if err := util.RemoveDevice(dev); err != nil {
		return fmt.Errorf("Fail to remove device %s: %v", dev, err)
	}
	if err := iscsi.StopScsi(l.volumeName); err != nil {
		return fmt.Errorf("Fail to stop SCSI device: %v", err)
	}
	c.Stop()
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

func (l *Launcher) startScsiDevice() error {
	if l.scsiDevice == nil {
		bsOpts := fmt.Sprintf("size=%v", l.size)
		scsiDev, err := iscsi.NewScsiDevice(l.volumeName, l.GetSocketPath(), "longhorn", bsOpts)
		if err != nil {
			return err
		}
		l.scsiDevice = scsiDev
	}
	if err := iscsi.StartScsi(l.scsiDevice); err != nil {
		return err
	}
	logrus.Infof("SCSI device %s created", l.scsiDevice.Device)
	return nil
}

func (l *Launcher) createDev() error {
	if err := os.MkdirAll(DevPath, 0700); err != nil {
		logrus.Fatalln("Cannot create directory ", DevPath)
	}

	dev := l.getDev()
	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	if err := util.DuplicateDevice(l.scsiDevice.Device, dev); err != nil {
		return err
	}
	logrus.Infof("Device %s is ready", dev)
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
		logrus.Warnf("Receive rpc shutdown: %v", rpcError)
		return rpcError
	}
	return nil
}

func (l *Launcher) Shutdown() {
	l.ShutdownController(l.currentController)
	controllerError := <-l.currentControllerShutdownCh
	logrus.Info("Longhorn Engine has been shutdown")
	if controllerError != nil {
		logrus.Warnf("Engine returns %v", controllerError)
	}
	l.rpcService.Stop()
	logrus.Info("Longhorn Engine Launcher has been shutdown")
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
	newController := NewController(engine.Binary, l.volumeName, engine.Listen, engine.EnableBackends, engine.Replicas)
	if newController == nil {
		return &rpc.Empty{}, fmt.Errorf("error")
	}
	return &rpc.Empty{}, nil
}
