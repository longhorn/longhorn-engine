package process

import (
	"fmt"
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

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/util"
)

/* Lock order
   1. Manager.lock
   2. Process.lock
*/

type Manager struct {
	portRangeMin int32
	portRangeMax int32

	rpcService    *grpc.Server
	rpcShutdownCh chan error

	lock            *sync.RWMutex
	processes       map[string]*Process
	processUpdateCh chan *Process
	shutdownCh      chan error

	availablePorts *util.Bitmap

	logsDir string
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

	logger *util.LonghornWriter
}

func NewManager(portRange string, logsDir string, shutdownCh chan error) (*Manager, error) {
	start, end, err := ParsePortRange(portRange)
	if err != nil {
		return nil, err
	}
	pm := &Manager{
		portRangeMin: start,
		portRangeMax: end,

		rpcShutdownCh: make(chan error),

		lock:            &sync.RWMutex{},
		processes:       map[string]*Process{},
		processUpdateCh: make(chan *Process),
		availablePorts:  util.NewBitmap(start, end),

		shutdownCh: shutdownCh,

		logsDir: logsDir,
	}
	go pm.StartMonitoring()
	return pm, nil
}

func (pm *Manager) StartMonitoring() {
	for {
		done := false
		select {
		case <-pm.shutdownCh:
			logrus.Infof("Process Manager is shutting down")
			done = true
			break
		case p := <-pm.processUpdateCh:
			p.lock.RLock()
			logrus.Infof("Process update: %v: state %v: Error: %v", p.Name, p.State, p.ErrorMsg)
			p.lock.RUnlock()
		}
		if done {
			break
		}
	}
}

func (pm *Manager) Shutdown() {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	pm.rpcService.Stop()
	close(pm.shutdownCh)
}

func (pm *Manager) ProcessCreate(ctx context.Context, req *rpc.ProcessCreateRequest) (ret *rpc.ProcessResponse, err error) {
	if req.Spec.Name == "" || req.Spec.Binary == "" {
		return nil, fmt.Errorf("missing required argument")
	}

	logger, err := util.NewLonghornWriter(req.Spec.Name, pm.logsDir)
	if err != nil {
		return nil, err
	}

	p := &Process{
		Name:      req.Spec.Name,
		Binary:    req.Spec.Binary,
		Args:      req.Spec.Args,
		PortCount: req.Spec.PortCount,
		PortArgs:  req.Spec.PortArgs,

		State: StateStarting,

		lock: &sync.RWMutex{},

		logger: logger,
	}

	if err := pm.registerProcess(p); err != nil {
		return nil, err
	}

	p.Start()

	return p.RPCResponse(), nil
}

func (pm *Manager) ProcessDelete(ctx context.Context, req *rpc.ProcessDeleteRequest) (ret *rpc.ProcessResponse, err error) {
	logrus.Debugf("Process Manager: prepare to delete process %v", req.Name)

	p := pm.findProcess(req.Name)
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

	go pm.unregisterProcess(p)

	if err := p.logger.Close(); err != nil {
		return nil, err
	}

	return resp, nil
}

func (pm *Manager) registerProcess(p *Process) error {
	var err error

	pm.lock.Lock()
	defer pm.lock.Unlock()

	_, exists := pm.processes[p.Name]
	if exists {
		return fmt.Errorf("engine process %v already exists", p.Name)
	}

	if len(p.PortArgs) > int(p.PortCount) {
		return fmt.Errorf("too many port args %v for port count %v", p.PortArgs, p.PortCount)
	}

	p.PortStart, p.PortEnd, err = pm.allocatePorts(p.PortCount)
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

	p.UpdateCh = pm.processUpdateCh
	pm.processes[p.Name] = p
	p.UpdateCh <- p

	return nil
}

func (pm *Manager) unregisterProcess(p *Process) {
	pm.lock.RLock()
	_, exists := pm.processes[p.Name]
	if !exists {
		pm.lock.RUnlock()
		return
	}
	pm.lock.RUnlock()

	for i := 0; i < types.WaitCount; i++ {
		if p.IsStopped() {
			break
		}
		logrus.Debugf("Process Manager: wait for process %v to shutdown before unregistering process", p.Name)
		time.Sleep(types.WaitInterval)
	}

	if p.IsStopped() {
		pm.lock.Lock()
		defer pm.lock.Unlock()
		if err := pm.releasePorts(p.PortStart, p.PortEnd); err != nil {
			logrus.Errorf("Process Manager: cannot deallocate %v ports (%v-%v) for %v: %v",
				p.PortCount, p.PortStart, p.PortEnd, p.Name, err)
		}
		logrus.Infof("Process Manager: successfully unregistered process %v", p.Name)
		delete(pm.processes, p.Name)
	} else {
		logrus.Errorf("Process Manager: failed to unregister process %v since it is state %v rather than stopped", p.Name, p.State)
	}

	return
}

func (pm *Manager) findProcess(name string) *Process {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	return pm.processes[name]
}

func (pm *Manager) ProcessGet(ctx context.Context, req *rpc.ProcessGetRequest) (*rpc.ProcessResponse, error) {
	p := pm.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	return p.RPCResponse(), nil
}

func (pm *Manager) ProcessList(ctx context.Context, req *rpc.ProcessListRequest) (*rpc.ProcessListResponse, error) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	resp := &rpc.ProcessListResponse{
		Processes: map[string]*rpc.ProcessResponse{},
	}
	for _, p := range pm.processes {
		resp.Processes[p.Name] = p.RPCResponse()
	}
	return resp, nil
}

func (pm *Manager) ProcessLog(req *rpc.LogRequest, srv rpc.ProcessManagerService_ProcessLogServer) error {
	p := pm.findProcess(req.Name)
	if p == nil {
		return fmt.Errorf("cannot find process %v", req.Name)
	}
	doneChan := make(chan struct{})
	logChan, err := p.logger.StreamLog(doneChan)
	if err != nil {
		return err
	}
	for logLine := range logChan {
		if err := srv.Send(&rpc.LogResponse{Line: logLine}); err != nil {
			doneChan <- struct{}{}
			close(doneChan)
			return err
		}
	}
	return nil
}

func (pm *Manager) allocatePorts(portCount int32) (int32, int32, error) {
	if portCount < 0 {
		return 0, 0, fmt.Errorf("invalid port count %v", portCount)
	}
	start, end, err := pm.availablePorts.AllocateRange(portCount)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "fail to allocate %v ports", portCount)
	}
	return int32(start), int32(end), nil
}

func (pm *Manager) releasePorts(start, end int32) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("invalid start/end port %v %v", start, end)
	}
	return pm.availablePorts.ReleaseRange(start, end)
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
		cmd.Stdout = p.logger
		cmd.Stderr = p.logger
		p.cmd = cmd
		p.lock.Unlock()

		if err := cmd.Run(); err != nil {
			p.lock.Lock()
			p.State = StateError
			p.ErrorMsg = err.Error()
			logrus.Infof("Process Manager: process %v error out, error msg: %v", p.Name, p.ErrorMsg)
			p.lock.Unlock()

			p.UpdateCh <- p
			return
		}
		p.lock.Lock()
		p.State = StateStopped
		logrus.Infof("Process Manager: process %v stopped", p.Name)
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
					p.UpdateCh <- p
					return
				}
				logrus.Infof("wait for gRPC service of process %v to start", p.Name)
				time.Sleep(types.WaitInterval)
			}
			// fail to start the process, then try to stop it.
			if !p.IsStopped() {
				p.Stop()
			}
		} else {
			// Process Manager doesn't know the grpc address. directly set running state
			p.lock.Lock()
			p.State = StateRunning
			p.lock.Unlock()
			p.UpdateCh <- p
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
	logrus.Debugf("Process Manager: send SIGINT to stop process %v", p.Name)
	p.lock.RLock()
	cmdp := p.cmd.Process
	p.lock.RUnlock()

	if cmdp == nil {
		logrus.Errorf("Process Manager: no process for cmd of %v", p.Name)
		return
	}
	cmdp.Signal(syscall.SIGINT)
	for i := 0; i < types.WaitCount; i++ {
		if p.IsStopped() {
			return
		}
		logrus.Infof("wait for process %v to shutdown", p.Name)
		time.Sleep(types.WaitInterval)
	}
	logrus.Debugf("Process Manager: cannot graceful stop process %v in %v seconds, will send SIGKILL to force stopping it", p.Name, types.WaitCount)
	cmdp.Signal(syscall.SIGKILL)
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
