package process

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
   1. Launcher.lock
   2. Process.lock
*/

type Launcher struct {
	listen string

	rpcService    *grpc.Server
	rpcShutdownCh chan error

	lock            *sync.RWMutex
	processes       map[string]*Process
	processUpdateCh chan *Process
	shutdownCh      chan struct{}
}

const (
	WaitInterval = time.Second
	WaitCount    = 60
)

type State string

const (
	StateRunning = State("running")
	StateStopped = State("stopped")
	StateError   = State("error")
)

type Process struct {
	Name          string
	Binary        string
	Args          []string
	ReservedPorts []int32

	State    State
	ErrorMsg string

	lock     *sync.RWMutex
	cmd      *exec.Cmd
	UpdateCh chan *Process
}

func NewLauncher(listen string) (*Launcher, error) {
	l := &Launcher{
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

func (l *Launcher) StartMonitoring() {
	for {
		done := false
		select {
		case <-l.shutdownCh:
			logrus.Infof("Launcher is shutting down")
			done = true
			break
		case p := <-l.processUpdateCh:
			p.lock.RLock()
			logrus.Infof("Process update: %v: state %v: Error: %v", p.Name, p.State, p.ErrorMsg)
			p.lock.RUnlock()
		}
		if done {
			break
		}
	}
}

func (l *Launcher) Shutdown() {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.rpcService.Stop()
	close(l.shutdownCh)
}

func (l *Launcher) WaitForShutdown() error {
	select {
	case rpcError := <-l.rpcShutdownCh:
		logrus.Warnf("launcher: Receive rpc shutdown: %v", rpcError)
		return rpcError
	}
}

func (l *Launcher) StartRPCServer() error {
	listen, err := net.Listen("tcp", l.listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	l.rpcService = grpc.NewServer()
	rpc.RegisterLonghornProcessLauncherServiceServer(l.rpcService, l)
	reflection.Register(l.rpcService)

	l.rpcShutdownCh = make(chan error)
	go func() {
		l.rpcShutdownCh <- l.rpcService.Serve(listen)
	}()
	return nil
}

func (l *Launcher) ProcessCreate(ctx context.Context, req *rpc.ProcessCreateRequest) (ret *rpc.ProcessResponse, err error) {
	if req.Spec.Name == "" || req.Spec.Binary == "" {
		return nil, fmt.Errorf("missing required argument")
	}

	p := &Process{
		Name:          req.Spec.Name,
		Binary:        req.Spec.Binary,
		Args:          req.Spec.Args,
		ReservedPorts: req.Spec.ReservedPorts,

		State: StateRunning,

		lock: &sync.RWMutex{},
	}

	if err := l.registerProcess(p); err != nil {
		return nil, err
	}

	p.Start()

	return p.RPCResponse(), nil
}

func (l *Launcher) ProcessDelete(ctx context.Context, req *rpc.ProcessDeleteRequest) (ret *rpc.ProcessResponse, err error) {
	p := l.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	if !p.IsStopped() {
		p.Stop()
	}

	resp := p.RPCResponse()

	if err := l.unregisterProcess(p); err != nil {
		return nil, err
	}

	return resp, nil
}

func (l *Launcher) registerProcess(p *Process) error {
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

func (l *Launcher) unregisterProcess(p *Process) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	_, exists := l.processes[p.Name]
	if !exists {
		return nil
	}

	if !p.IsStopped() {
		return fmt.Errorf("cannot unregister running process")
	}

	delete(l.processes, p.Name)

	return nil
}

func (l *Launcher) findProcess(name string) *Process {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.processes[name]
}

func (l *Launcher) ProcessGet(ctx context.Context, req *rpc.ProcessGetRequest) (*rpc.ProcessResponse, error) {
	p := l.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	return p.RPCResponse(), nil
}

func (l *Launcher) ProcessList(ctx context.Context, req *rpc.ProcessListRequest) (*rpc.ProcessListResponse, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	resp := &rpc.ProcessListResponse{
		Processes: map[string]*rpc.ProcessResponse{},
	}
	for _, p := range l.processes {
		resp.Processes[p.Name] = p.RPCResponse()
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
			p.State = StateError
			p.ErrorMsg = err.Error()
			p.lock.Unlock()

			p.UpdateCh <- p
			return
		}
		p.lock.Lock()
		p.State = StateStopped
		p.lock.Unlock()

		p.UpdateCh <- p
	}()

	return nil
}

func (p *Process) RPCResponse() *rpc.ProcessResponse {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &rpc.ProcessResponse{
		Spec: &rpc.ProcessSpec{
			Name:          p.Name,
			Binary:        p.Binary,
			Args:          p.Args,
			ReservedPorts: p.ReservedPorts,
		},

		Status: &rpc.ProcessStatus{
			State:    string(p.State),
			ErrorMsg: p.ErrorMsg,
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
	return p.State == StateStopped || p.State == StateError
}
