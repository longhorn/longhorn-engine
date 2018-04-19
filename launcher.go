package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/yasker/longhorn-engine-launcher/rpc"
)

type Launcher struct {
	listen         string
	longhornBinary string
	frontend       string
	volumeName     string

	rpcShutdownCh chan error

	currentController           *Controller
	currentControllerShutdownCh chan error
}

func NewLauncher(listen, longhornBinary, frontend, volumeName string) (*Launcher, error) {
	if frontend != "tgt-blockdev" && frontend != "tgt-iscsi" {
		return nil, fmt.Errorf("Invalid frontend %v", frontend)
	}
	return &Launcher{
		listen:         listen,
		longhornBinary: longhornBinary,
		frontend:       frontend,
		volumeName:     volumeName,
	}, nil
}

func (l *Launcher) StartController(c *Controller) error {
	l.currentController = c
	l.currentControllerShutdownCh = c.Start()
	return nil
}

func (l *Launcher) WaitForShutdown() error {
	var controllerError, rpcError error
	select {
	case controllerError = <-l.currentControllerShutdownCh:
		logrus.Warnf("Receive controller shutdown: %v", controllerError)
		return controllerError
	case rpcError = <-l.rpcShutdownCh:
		logrus.Warnf("Receive rpc shutdown: %v", rpcError)
		return rpcError
	}
	return nil
}

func (l *Launcher) StartRPCServer() error {
	listen, err := net.Listen("tcp", l.listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	s := grpc.NewServer()
	rpc.RegisterLonghornLauncherServiceServer(s, l)
	reflection.Register(s)

	l.rpcShutdownCh = make(chan error)
	go func() {
		l.rpcShutdownCh <- s.Serve(listen)
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

type Controller struct {
	volumeName string

	binary   string
	listen   string
	backends []string
	replicas []string
}

func NewController(binary, volumeName, listen string, backends, replicas []string) *Controller {
	return &Controller{
		binary:     binary,
		volumeName: volumeName,
		listen:     listen,
		backends:   backends,
		replicas:   replicas,
	}
}

func (c *Controller) Start() chan error {
	resp := make(chan error)

	exe, err := exec.LookPath(c.binary)
	if err != nil {
		resp <- err
		return resp
	}

	exe, err = filepath.Abs(exe)
	if err != nil {
		resp <- err
		return resp
	}

	go func() {
		args := []string{
			"controller", c.volumeName,
			"--listen", c.listen,
			"--frontend", "socket",
		}
		for _, b := range c.backends {
			args = append(args, "--enable-backend", b)
		}
		for _, r := range c.replicas {
			args = append(args, "--replica", r)
		}
		cmd := exec.Command(exe, args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Pdeathsig: syscall.SIGKILL,
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		resp <- cmd.Run()
	}()

	return resp
}
