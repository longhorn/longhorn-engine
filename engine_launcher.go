package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
)

/* Lock order
   1. EngineLauncher.lock
   2. EngineProcess.lock
*/

type EngineLauncher struct {
	listen string

	rpcService    *grpc.Server
	rpcShutdownCh chan error

	lock            *sync.RWMutex
	processes       map[string]*Process
	processUpdateCh chan *Process
	shutdownCh      chan struct{}
}

type EngineStatus string

const (
	EngineStatusRunning = EngineStatus("running")
	EngineStatusStopped = EngineStatus("stopped")
	EngineStatusError   = EngineStatus("error")
)

type Process struct {
	Name          string
	Binary        string
	Args          []string
	ReservedPorts []int32

	Status EngineStatus

	lock     *sync.RWMutex
	cmd      *exec.Cmd
	ErrorMsg string
	UpdateCh chan *Process
}

func NewEngineLauncher(listen string) (*EngineLauncher, error) {
	l := &EngineLauncher{
		listen:        listen,
		rpcShutdownCh: make(chan error),

		lock:            &sync.RWMutex{},
		processes:       map[string]*Process{},
		processUpdateCh: make(chan *Process),

		shutdownCh: make(chan struct{}),
	}
	go l.StartMonitoring()
	return l, nil
}

func (l *EngineLauncher) StartMonitoring() {
	for {
		done := false
		select {
		case <-l.shutdownCh:
			logrus.Infof("Launcher is shutting down")
			done = true
			break
		case p := <-l.processUpdateCh:
			//TODO need lock if going to modify engine process
			logrus.Infof("Process update: %+v", p)
		}
		if done {
			break
		}
	}
}

func (l *EngineLauncher) Shutdown() {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.rpcService.Stop()
	close(l.shutdownCh)
}

func (l *EngineLauncher) WaitForShutdown() error {
	select {
	case rpcError := <-l.rpcShutdownCh:
		logrus.Warnf("launcher: Receive rpc shutdown: %v", rpcError)
		return rpcError
	}
	return nil
}

func (l *EngineLauncher) StartRPCServer() error {
	listen, err := net.Listen("tcp", l.listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	l.rpcService = grpc.NewServer()
	rpc.RegisterLonghornEngineLauncherServiceServer(l.rpcService, l)
	reflection.Register(l.rpcService)

	l.rpcShutdownCh = make(chan error)
	go func() {
		l.rpcShutdownCh <- l.rpcService.Serve(listen)
	}()
	return nil
}

func (l *EngineLauncher) StartEngine(ctx context.Context, req *rpc.StartEngineRequest) (ret *rpc.EngineResponse, err error) {
	p := &Process{
		Name:          req.Spec.Name,
		Binary:        req.Spec.Binary,
		Args:          req.Spec.Args,
		ReservedPorts: req.Spec.ReservedPorts,

		Status: EngineStatusStopped,

		lock: &sync.RWMutex{},
	}

	if err := l.registerProcess(p); err != nil {
		return nil, err
	}

	// get response before
	resp := p.RPCResponse()
	p.Start()

	return resp, nil
}

func (l *EngineLauncher) StopEngine(ctx context.Context, req *rpc.StopEngineRequest) (ret *rpc.EngineResponse, err error) {
	p := l.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	p.Stop()

	return p.RPCResponse(), nil
}

func (l *EngineLauncher) registerProcess(p *Process) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	_, exists := l.processes[p.Name]
	if exists {
		return fmt.Errorf("engine process %v already exists", p.Name)
	}

	p.UpdateCh = l.processUpdateCh
	l.processes[p.Name] = p

	return nil
}

func (l *EngineLauncher) findProcess(name string) *Process {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.processes[name]
}

func (l *EngineLauncher) GetEngine(ctx context.Context, req *rpc.GetEngineRequest) (*rpc.EngineResponse, error) {
	p := l.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	return p.RPCResponse(), nil
}

func (l *EngineLauncher) ListEngines(ctx context.Context, req *rpc.Empty) (*rpc.ListEnginesResponse, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	resp := &rpc.ListEnginesResponse{
		Engines: []*rpc.EngineResponse{},
	}
	for _, p := range l.processes {
		resp.Engines = append(resp.Engines, p.RPCResponse())
	}
	return resp, nil
}

func (p *Process) Start() error {
	binary, err := exec.LookPath(p.Binary)
	if err != nil {
		return err
	}

	binary, err = filepath.Abs(binary)
	if err != nil {
		return err
	}

	go func() {
		cmd := exec.Command(binary, p.Args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Pdeathsig: syscall.SIGKILL,
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		p.lock.Lock()
		p.cmd = cmd
		p.lock.Unlock()

		if err := cmd.Run(); err != nil {
			p.lock.Lock()
			p.Status = EngineStatusError
			p.ErrorMsg = err.Error()
			p.lock.Unlock()

			p.UpdateCh <- p
			return
		}
		p.lock.Lock()
		p.Status = EngineStatusStopped
		p.lock.Unlock()

		p.UpdateCh <- p
	}()

	return nil
}

func (p *Process) RPCResponse() *rpc.EngineResponse {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &rpc.EngineResponse{
		Spec: &rpc.EngineSpec{
			Name:          p.Name,
			Binary:        p.Binary,
			Args:          p.Args,
			ReservedPorts: p.ReservedPorts,
		},

		Status: &rpc.EngineStatus{
			Status: string(p.Status),
		},
	}
}

func (p *Process) Stop() {
	// We don't neeed lock here since cmd will deal with concurrency
	p.cmd.Process.Signal(syscall.SIGINT)
	for i := 0; i < WaitCount; i++ {
		if p.IsStopped() {
			return
		}
		logrus.Infof("launcher: wait for process %v to shutdown", p.Name)
		time.Sleep(WaitInterval)
	}
	p.cmd.Process.Signal(syscall.SIGKILL)
}

func (p *Process) IsStopped() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.Status == EngineStatusStopped
}
