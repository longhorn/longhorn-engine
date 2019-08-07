package process

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
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
	rpcWatchers   map[chan<- *rpc.ProcessResponse]<-chan struct{}

	lock            *sync.RWMutex
	processes       map[string]*Process
	processUpdateCh chan *Process
	shutdownCh      chan error

	availablePorts *util.Bitmap

	logsDir string

	Executor types.Executor
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
		rpcWatchers:   make(map[chan<- *rpc.ProcessResponse]<-chan struct{}),

		lock:            &sync.RWMutex{},
		processes:       map[string]*Process{},
		processUpdateCh: make(chan *Process),
		availablePorts:  util.NewBitmap(start, end),

		shutdownCh: shutdownCh,

		logsDir: logsDir,

		Executor: &types.BinaryExecutor{},
	}
	go pm.startMonitoring()
	return pm, nil
}

func (pm *Manager) startMonitoring() {
	for {
		done := false
		select {
		case <-pm.shutdownCh:
			logrus.Infof("Process Manager is shutting down")
			done = true
			pm.lock.RLock()
			for stream := range pm.rpcWatchers {
				close(stream)
			}
			pm.lock.RUnlock()
			logrus.Infof("Process Manager has closed all gRPC watchers")
			break
		case p := <-pm.processUpdateCh:
			resp := p.RPCResponse()
			pm.lock.RLock()
			// Modify response to indicate deletion.
			if _, exists := pm.processes[p.Name]; !exists {
				resp.Deleted = true
			}
			for stream, stop := range pm.rpcWatchers {
				select {
				case <-stop:
					continue
				case stream <- resp:
				}
			}
			pm.lock.RUnlock()
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
		UUID:      req.Spec.Uuid,
		Name:      req.Spec.Name,
		Binary:    req.Spec.Binary,
		Args:      req.Spec.Args,
		PortCount: req.Spec.PortCount,
		PortArgs:  req.Spec.PortArgs,

		State:           StateStarting,
		ResourceVersion: 1,

		lock: &sync.RWMutex{},

		logger: logger,

		executor: pm.Executor,
	}

	if err := pm.registerProcess(p); err != nil {
		return nil, err
	}
	p.UpdateCh <- p
	p.Start()

	return p.RPCResponse(), nil
}

func (pm *Manager) ProcessDelete(ctx context.Context, req *rpc.ProcessDeleteRequest) (ret *rpc.ProcessResponse, err error) {
	logrus.Debugf("Process Manager: prepare to delete process %v", req.Name)

	p := pm.findProcess(req.Name)
	if p == nil {
		return nil, fmt.Errorf("cannot find process %v", req.Name)
	}

	p.Stop()

	resp := p.RPCResponse()
	resp.Deleted = true

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
		if err := pm.releasePorts(p.PortStart, p.PortEnd); err != nil {
			logrus.Errorf("Process Manager: cannot deallocate %v ports (%v-%v) for %v: %v",
				p.PortCount, p.PortStart, p.PortEnd, p.Name, err)
		}
		logrus.Infof("Process Manager: successfully unregistered process %v", p.Name)
		delete(pm.processes, p.Name)
		pm.lock.Unlock()
		p.UpdateCh <- p
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

func (pm *Manager) ProcessWatch(req *empty.Empty, srv rpc.ProcessManagerService_ProcessWatchServer) (err error) {
	responseChan := make(chan *rpc.ProcessResponse)
	stopCh := make(chan struct{})
	pm.lock.Lock()
	pm.rpcWatchers[responseChan] = stopCh
	pm.lock.Unlock()
	defer func() {
		close(stopCh)
		pm.lock.Lock()
		delete(pm.rpcWatchers, responseChan)
		pm.lock.Unlock()

		if err != nil {
			logrus.Errorf("process manager update watch errored out: %v", err)
		} else {
			logrus.Debugf("process manager update watch ended successfully")
		}
	}()
	logrus.Debugf("started new process manager update watch")

	for resp := range responseChan {
		if err := srv.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (pm *Manager) allocatePorts(portCount int32) (int32, int32, error) {
	if portCount < 0 {
		return 0, 0, fmt.Errorf("invalid port count %v", portCount)
	}
	if portCount == 0 {
		return 0, 0, nil
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
