package process

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/types"
	"github.com/longhorn/longhorn-engine-launcher/util"
)

/* Lock order
   1. Launcher.lock
   2. Process.lock
*/

type Launcher struct {
	portRangeMin int32
	portRangeMax int32

	rpcService    *grpc.Server
	rpcShutdownCh chan error

	lock            *sync.RWMutex
	processes       map[string]*Process
	processUpdateCh chan *Process
	shutdownCh      chan error

	availablePorts *util.Bitmap
}

type State string

const (
	StateStarting = State(types.ProcessStateStarting)
	StateRunning  = State(types.ProcessStateRunning)
	StateStopping = State(types.ProcessStateStopping)
	StateStopped  = State(types.ProcessStateStopped)
	StateError    = State(types.ProcessStateError)
)

type Process struct {
	Name      string
	Binary    string
	Args      []string
	PortCount int32
	PortArgs  []string

	State     State
	ErrorMsg  string
	PortStart int32
	PortEnd   int32

	lock     *sync.RWMutex
	cmd      *exec.Cmd
	UpdateCh chan *Process
}

func NewLauncher(portRange string, shutdownCh chan error) (*Launcher, error) {
	start, end, err := ParsePortRange(portRange)
	if err != nil {
		return nil, err
	}
	l := &Launcher{
		portRangeMin: start,
		portRangeMax: end,

		rpcShutdownCh: make(chan error),

		lock:            &sync.RWMutex{},
		processes:       map[string]*Process{},
		processUpdateCh: make(chan *Process),
		availablePorts:  util.NewBitmap(start, end),

		shutdownCh: shutdownCh,
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

func (l *Launcher) ProcessCreate(ctx context.Context, req *rpc.ProcessCreateRequest) (ret *rpc.ProcessResponse, err error) {
	if req.Spec.Name == "" || req.Spec.Binary == "" {
		return nil, fmt.Errorf("missing required argument")
	}

	p := &Process{
		Name:      req.Spec.Name,
		Binary:    req.Spec.Binary,
		Args:      req.Spec.Args,
		PortCount: req.Spec.PortCount,
		PortArgs:  req.Spec.PortArgs,

		State: StateStarting,

		lock: &sync.RWMutex{},
	}

	if err := l.registerProcess(p); err != nil {
		return nil, err
	}

	p.Start()

	return p.RPCResponse(), nil
}

func (l *Launcher) ProcessDelete(ctx context.Context, req *rpc.ProcessDeleteRequest) (ret *rpc.ProcessResponse, err error) {
	logrus.Debugf("launcher: prepare to delete process %v", req.Name)

	p := l.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	p.lock.Lock()
	if p.State != StateStopping && p.State != StateStopped && p.State != StateError {
		p.State = StateStopping
		go p.Stop()
	}
	p.lock.Unlock()

	resp := p.RPCResponse()

	go l.unregisterProcess(p)

	return resp, nil
}

func (l *Launcher) registerProcess(p *Process) error {
	var err error

	l.lock.Lock()
	defer l.lock.Unlock()

	_, exists := l.processes[p.Name]
	if exists {
		return fmt.Errorf("engine process %v already exists", p.Name)
	}

	if len(p.PortArgs) > int(p.PortCount) {
		return fmt.Errorf("too many port args %v for port count %v", p.PortArgs, p.PortCount)
	}

	p.PortStart, p.PortEnd, err = l.allocatePorts(p.PortCount)
	if err != nil {
		return errors.Wrapf(err, "cannot allocate %v ports for %v", p.PortCount, p.Name)
	}

	if len(p.PortArgs) != 0 {
		for i, arg := range p.PortArgs {
			if p.PortStart+int32(i) > p.PortEnd {
				return fmt.Errorf("cannot fit port args %v", arg)
			}
			p.Args = append(p.Args, strings.Split(arg+strconv.Itoa(int(p.PortStart)+i), ",")...)
		}
	}

	p.UpdateCh = l.processUpdateCh
	l.processes[p.Name] = p

	return nil
}

func (l *Launcher) unregisterProcess(p *Process) {
	l.lock.RLock()
	_, exists := l.processes[p.Name]
	if !exists {
		l.lock.RUnlock()
		return
	}
	l.lock.RUnlock()

	for i := 0; i < types.WaitCount; i++ {
		if p.IsStopped() {
			break
		}
		logrus.Debugf("launcher: wait for process %v to shutdown before unregistering process", p.Name)
		time.Sleep(types.WaitInterval)
	}

	if p.IsStopped() {
		l.lock.Lock()
		defer l.lock.Unlock()
		if err := l.releasePorts(p.PortStart, p.PortEnd); err != nil {
			logrus.Errorf("launcher: cannot deallocate %v ports (%v-%v) for %v: %v",
				p.PortCount, p.PortStart, p.PortEnd, p.Name, err)
		}
		logrus.Infof("launcher: successfully unregistered process %v", p.Name)
		delete(l.processes, p.Name)
	} else {
		logrus.Errorf("launcher: failed to unregister process %v since it is state %v rather than stopped", p.Name, p.State)
	}

	return
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

func (l *Launcher) allocatePorts(portCount int32) (int32, int32, error) {
	if portCount < 0 {
		return 0, 0, fmt.Errorf("invalid port count %v", portCount)
	}
	start, end, err := l.availablePorts.AllocateRange(portCount)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "fail to allocate %v ports", portCount)
	}
	return int32(start), int32(end), nil
}

func (l *Launcher) releasePorts(start, end int32) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("invalid start/end port %v %v", start, end)
	}
	return l.availablePorts.ReleaseRange(start, end)
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
		p.lock.Lock()
		cmd := exec.Command(binary, p.Args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Pdeathsig: syscall.SIGKILL,
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		p.cmd = cmd
		p.lock.Unlock()

		if err := cmd.Run(); err != nil {
			p.lock.Lock()
			p.State = StateError
			p.ErrorMsg = err.Error()
			logrus.Infof("launcher: process %v error out, error msg: %v", p.Name, p.ErrorMsg)
			p.lock.Unlock()

			p.UpdateCh <- p
			return
		}
		p.lock.Lock()
		p.State = StateStopped
		logrus.Infof("launcher: process %v stopped")
		p.lock.Unlock()

		p.UpdateCh <- p
	}()

	go func() {
		if p.PortStart != 0 {
			address := util.GetURL("localhost", int(p.PortStart))
			for i := 0; i < types.WaitCount; i++ {
				if util.GRPCServiceReadinessProbe(address) {
					p.lock.Lock()
					p.State = StateRunning
					p.lock.Unlock()
					return
				}
				logrus.Infof("launcher: wait for gRPC service of process %v to start", p.Name)
				time.Sleep(types.WaitInterval)
			}
			// fail to start the process, then try to stop it.
			if !p.IsStopped() {
				p.Stop()
			}
		} else {
			// launcher doesn't know the grpc address. directly set running state
			p.lock.Lock()
			p.State = StateRunning
			p.lock.Unlock()
		}
		return
	}()

	return nil
}

func (p *Process) RPCResponse() *rpc.ProcessResponse {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &rpc.ProcessResponse{
		Spec: &rpc.ProcessSpec{
			Name:      p.Name,
			Binary:    p.Binary,
			Args:      p.Args,
			PortCount: p.PortCount,
			PortArgs:  p.PortArgs,
		},

		Status: &rpc.ProcessStatus{
			State:     string(p.State),
			ErrorMsg:  p.ErrorMsg,
			PortStart: p.PortStart,
			PortEnd:   p.PortEnd,
		},
	}
}

func (p *Process) Stop() {
	// We don't neeed lock here since cmd will deal with concurrency
	logrus.Debugf("launcher: send SIGINT to stop process %v", p.Name)
	p.cmd.Process.Signal(syscall.SIGINT)
	for i := 0; i < types.WaitCount; i++ {
		if p.IsStopped() {
			return
		}
		logrus.Infof("launcher: wait for process %v to shutdown", p.Name)
		time.Sleep(types.WaitInterval)
	}
	logrus.Debugf("launcher: cannot graceful stop process %v in %v seconds, will send SIGKILL to force stopping it", p.Name, types.WaitCount)
	p.cmd.Process.Signal(syscall.SIGKILL)
}

func (p *Process) IsStopped() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.State == StateStopped || p.State == StateError
}

func ParsePortRange(portRange string) (int32, int32, error) {
	if portRange == "" {
		return 0, 0, fmt.Errorf("Empty port range")
	}
	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("Invalid format for range: %s", portRange)
	}
	portStart, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("Invalid start port for range: %s", err)
	}
	portEnd, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("Invalid end port for range: %s", err)
	}
	return int32(portStart), int32(portEnd), nil
}
