package engine

import (
	"fmt"
	"os"
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

type Manager struct {
	lock           *sync.RWMutex
	processManager rpc.ProcessManagerServiceServer
	pStreamWrapper *ProcessStreamWrapper
	rpcWatchers    map[chan<- *rpc.EngineResponse]struct{}
	listen         string

	elUpdateCh      chan *Launcher
	shutdownCh      chan error
	engineLaunchers map[string]*Launcher
	tIDAllocator    *util.Bitmap
}

type ProcessStreamWrapper struct {
	grpc.ServerStream
	*sync.RWMutex

	updateChs map[chan<- *rpc.ProcessResponse]struct{}
}

const (
	MaxTgtTargetNumber = 4096
)

func NewProcessStreamWrapper() *ProcessStreamWrapper {
	return &ProcessStreamWrapper{
		RWMutex:   &sync.RWMutex{},
		updateChs: make(map[chan<- *rpc.ProcessResponse]struct{}),
	}
}

func (sw ProcessStreamWrapper) Send(response *rpc.ProcessResponse) error {
	sw.RLock()
	for ch := range sw.updateChs {
		ch <- response
	}
	sw.RUnlock()
	return nil
}

func (sw *ProcessStreamWrapper) AddLauncherStream(updateCh chan<- *rpc.ProcessResponse) {
	sw.Lock()
	sw.updateChs[updateCh] = struct{}{}
	sw.Unlock()
}

func (sw *ProcessStreamWrapper) RemoveLauncherStream(updateCh chan<- *rpc.ProcessResponse) {
	sw.Lock()
	delete(sw.updateChs, updateCh)
	sw.Unlock()
	close(updateCh)
}

func NewEngineManager(pm rpc.ProcessManagerServiceServer, listen string, shutdownCh chan error) (*Manager, error) {
	em := &Manager{
		lock:           &sync.RWMutex{},
		processManager: pm,
		pStreamWrapper: NewProcessStreamWrapper(),
		rpcWatchers:    make(map[chan<- *rpc.EngineResponse]struct{}),
		listen:         listen,

		elUpdateCh:      make(chan *Launcher),
		shutdownCh:      shutdownCh,
		engineLaunchers: map[string]*Launcher{},
		tIDAllocator:    util.NewBitmap(1, MaxTgtTargetNumber),
	}
	go em.StartMonitoring()
	return em, nil
}

func (em *Manager) StartMonitoring() {
	go func() {
		if err := em.processManager.ProcessWatch(nil, em.pStreamWrapper); err != nil {
			logrus.Errorf("could not start process monitoring from engine manager: %v", err)
			return
		}
		logrus.Infof("Stopped process update watch from engine manager")
	}()
	for {
		done := false
		select {
		case <-em.shutdownCh:
			logrus.Infof("Engine Manager is shutting down")
			done = true
			em.lock.RLock()
			for stream := range em.rpcWatchers {
				close(stream)
			}
			em.lock.RUnlock()
			logrus.Infof("Engine Manager has closed all gRPC watchers")
			break
		case el := <-em.elUpdateCh:
			el.lock.RLock()
			name := el.currentEngine.EngineName
			el.lock.RUnlock()
			response, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
				Name: name,
			})
			if err != nil {
				logrus.Errorf("Could not get Process for current Engine of Launcher %v: %v", el.LauncherName,
					err)
			}
			resp := el.RPCResponse(response)
			em.lock.RLock()
			for stream := range em.rpcWatchers {
				stream <- resp
			}
			em.lock.RUnlock()
		}
		if done {
			break
		}
	}
}

func (em *Manager) EngineCreate(ctx context.Context, req *rpc.EngineCreateRequest) (ret *rpc.EngineResponse, err error) {
	logrus.Infof("Engine Manager starts to create engine of volume %v", req.Spec.VolumeName)

	el := NewEngineLauncher(req.Spec)
	if err := em.registerEngineLauncher(el); err != nil {
		return nil, errors.Wrapf(err, "failed to register engine launcher %v", el.LauncherName)
	}
	if err := el.createEngineProcess(em.listen, em.processManager); err != nil {
		go em.unregisterEngineLauncher(req.Spec.Name)
		return nil, errors.Wrapf(err, "failed to start engine %v", req.Spec.Name)
	}

	el.lock.RLock()
	response, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: el.currentEngine.EngineName,
	})
	el.lock.RUnlock()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get process info for engine %v", req.Spec.Name)
	}

	logrus.Infof("Engine Manager has successfully created engine %v", req.Spec.Name)

	return el.RPCResponse(response), nil
}

func (em *Manager) registerEngineLauncher(el *Launcher) error {
	em.lock.Lock()
	defer em.lock.Unlock()

	_, exists := em.engineLaunchers[el.LauncherName]
	if exists {
		return fmt.Errorf("engine launcher %v already exists", el.LauncherName)
	}

	em.pStreamWrapper.AddLauncherStream(el.pUpdateCh)
	el.UpdateCh = em.elUpdateCh
	em.engineLaunchers[el.LauncherName] = el
	el.UpdateCh <- el
	return nil
}

func (em *Manager) unregisterEngineLauncher(launcherName string) {
	logrus.Debugf("Engine Manager starts to unregistered engine launcher %v", launcherName)

	em.lock.RLock()
	el, exists := em.engineLaunchers[launcherName]
	em.lock.RUnlock()
	if !exists {
		return
	}

	// Stop Process monitoring for the Engine update streaming.
	em.pStreamWrapper.RemoveLauncherStream(el.pUpdateCh)

	el.lock.RLock()
	processName := el.currentEngine.EngineName
	el.lock.RUnlock()

	for i := 0; i < types.WaitCount; i++ {
		if _, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: processName,
		}); err != nil && strings.Contains(err.Error(), "cannot find process") {
			break
		}
		logrus.Infof("Engine Manager is waiting for engine %v to shutdown before unregistering the engine launcher", processName)
		time.Sleep(types.WaitInterval)
	}

	if _, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: processName,
	}); err != nil && strings.Contains(err.Error(), "cannot find process") {
		// cannot depend on engine process's callback to cleanup frontend. need to double check here
		em.lock.Lock()
		defer em.lock.Unlock()
		el, exists := em.engineLaunchers[launcherName]
		if !exists {
			return
		}

		el.lock.RLock()
		needCleanup := false
		if el.scsiDevice != nil {
			needCleanup = true
		}
		el.lock.RUnlock()

		if needCleanup {
			logrus.Warnf("Engine Manager need to cleanup frontend before unregistering engine launcher %v", launcherName)
			if err = em.cleanupFrontend(el); err != nil {
				// cleanup failed. cannot unregister engine launcher.
				logrus.Errorf("Engine Manager fails to cleanup frontend before unregistering engine launcher %v", launcherName)
				return
			}
		}
		delete(em.engineLaunchers, launcherName)

		logrus.Infof("Engine Manager had successfully unregistered engine launcher %v, deletion completed", launcherName)
	} else {
		logrus.Errorf("Engine Manager fails to unregister engine launcher %v", launcherName)
	}

	return
}

func (em *Manager) EngineDelete(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.EngineResponse, err error) {
	logrus.Infof("Engine Manager starts to deleted engine %v", req.Name)

	em.lock.Lock()
	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		em.lock.Unlock()
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}
	em.lock.Unlock()

	el.lock.Lock()
	processName := el.currentEngine.EngineName
	deletionRequired := !el.isDeleting
	el.isDeleting = true
	el.lock.Unlock()

	var processResp *rpc.ProcessResponse
	if deletionRequired {
		processResp, err = el.deleteEngine(em.processManager, processName)
		if err != nil {
			return nil, err
		}

		go em.unregisterEngineLauncher(req.Name)
	} else {
		logrus.Debugf("Engine Manager is already deleting engine %v", req.Name)
		processResp, err = em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: processName,
		})
		if err != nil {
			if strings.Contains(err.Error(), "cannot find process") {
				processResp = nil
			} else {
				return nil, err
			}
		}
	}

	ret = el.RPCResponse(processResp)

	logrus.Infof("Engine Manager is deleting engine %v", req.Name)

	return ret, nil
}

func (em *Manager) EngineGet(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.EngineResponse, err error) {
	logrus.Debugf("Engine Manager starts to get engine %v", req.Name)

	em.lock.RLock()
	defer em.lock.RUnlock()

	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	el.lock.RLock()
	response, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: el.currentEngine.EngineName,
	})
	el.lock.RUnlock()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot find engine %v since failed to get related process info: %v",
			req.Name, err)
	}

	logrus.Debugf("Engine Manager has successfully get engine %v", req.Name)

	return el.RPCResponse(response), nil
}

func (em *Manager) EngineList(ctx context.Context, req *empty.Empty) (ret *rpc.EngineListResponse, err error) {
	logrus.Debugf("Engine Manager starts to list engines")

	em.lock.RLock()
	defer em.lock.RUnlock()

	ret = &rpc.EngineListResponse{
		Engines: map[string]*rpc.EngineResponse{},
	}

	for _, el := range em.engineLaunchers {
		el.lock.RLock()
		processName := el.currentEngine.EngineName
		el.lock.RUnlock()
		response, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: processName,
		})
		if err != nil {
			if strings.Contains(err.Error(), "cannot find process") {
				response = nil
			} else {
				return nil, errors.Wrapf(err, "failed to get process info for engine %v", el.LauncherName)
			}
		}
		ret.Engines[el.LauncherName] = el.RPCResponse(response)
	}

	logrus.Debugf("Engine Manager has successfully list all engines")

	return ret, nil
}

func (em *Manager) EngineUpgrade(ctx context.Context, req *rpc.EngineUpgradeRequest) (ret *rpc.EngineResponse, err error) {
	logrus.Infof("Engine Manager starts to upgrade engine %v for volume %v", req.Spec.Name, req.Spec.VolumeName)

	el, err := em.validateBeforeUpgrade(req.Spec)
	if err != nil {
		return nil, err
	}

	if err := el.prepareUpgrade(req.Spec); err != nil {
		return nil, errors.Wrapf(err, "failed to prepare to upgrade engine to %v", req.Spec.Name)
	}

	defer func() {
		if err != nil {
			logrus.Errorf("failed to upgrade: %v", err)
			if err := el.rollbackUpgrade(em.processManager); err != nil {
				logrus.Errorf("failed to rollback upgrade: %v", err)
			}
		}
	}()

	if err := el.createEngineProcess(em.listen, em.processManager); err != nil {
		return nil, errors.Wrapf(err, "failed to create upgrade engine %v", req.Spec.Name)
	}

	if err = em.checkUpgradedEngineSocket(el); err != nil {
		return nil, errors.Wrapf(err, "failed to reload socket connection for new engine %v", req.Spec.Name)
	}

	el.finalizeUpgrade(em.processManager)

	el.lock.RLock()
	response, err := em.processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: el.currentEngine.EngineName,
	})
	el.lock.RUnlock()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get process info for upgraded engine %v", req.Spec.Name)
	}

	logrus.Infof("Engine Manager has successfully upgraded engine %v with binary %v", req.Spec.Name, req.Spec.Binary)

	return el.RPCResponse(response), nil
}

func (em *Manager) EngineLog(req *rpc.LogRequest, srv rpc.EngineManagerService_EngineLogServer) error {
	logrus.Debugf("Engine Manager getting logs for engine %v", req.Name)

	em.lock.RLock()
	defer em.lock.RUnlock()

	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		return fmt.Errorf("cannot find engine %v", req.Name)
	}

	el.lock.RLock()
	err := el.engineLog(&rpc.LogRequest{
		Name: el.currentEngine.EngineName,
	}, srv, em.processManager)
	el.lock.RUnlock()
	if err != nil {
		return err
	}

	logrus.Debugf("Engine Manager has successfully retrieved logs for engine %v", req.Name)

	return nil
}

func (em *Manager) EngineWatch(req *empty.Empty, srv rpc.EngineManagerService_EngineWatchServer) error {
	responseChan := make(chan *rpc.EngineResponse)
	em.lock.Lock()
	em.rpcWatchers[responseChan] = struct{}{}
	em.lock.Unlock()
	defer func() {
		em.lock.Lock()
		delete(em.rpcWatchers, responseChan)
		em.lock.Unlock()
	}()

	for resp := range responseChan {
		if err := srv.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (em *Manager) validateBeforeUpgrade(spec *rpc.EngineSpec) (*Launcher, error) {
	if _, err := os.Stat(spec.Binary); os.IsNotExist(err) {
		return nil, errors.Wrap(err, "cannot find the binary to be upgraded")
	}

	em.lock.RLock()
	defer em.lock.RUnlock()

	el, exists := em.engineLaunchers[spec.Name]
	if !exists {
		return nil, fmt.Errorf("cannot find engine %v", spec.Name)
	}

	el.lock.RLock()
	defer el.lock.RUnlock()

	if el.Binary == spec.Binary || el.LauncherName != spec.Name {
		return nil, fmt.Errorf("cannot upgrade with the same binary or the different engine")
	}

	return el, nil
}

func (em *Manager) checkUpgradedEngineSocket(el *Launcher) (err error) {
	el.lock.RLock()
	defer el.lock.RUnlock()

	stopCh := make(chan struct{})
	socketError := el.WaitForSocket(stopCh)
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

	if err = el.ReloadSocketConnection(); err != nil {
		return err
	}

	return nil
}

func (em *Manager) FrontendStart(ctx context.Context, req *rpc.FrontendStartRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to start frontend %v for engine %v", req.Frontend, req.Name)

	em.lock.Lock()
	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		em.lock.Unlock()
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}
	em.lock.Unlock()

	// the controller will call back to engine manager later. be careful about deadlock
	if err := el.startFrontend(req.Frontend); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager has successfully start frontend %v for engine %v", req.Frontend, req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) FrontendShutdown(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to shutdown frontend for engine %v", req.Name)

	em.lock.Lock()
	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		em.lock.Unlock()
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}
	em.lock.Unlock()

	// the controller will call back to engine manager later. be careful about deadlock
	if err := el.shutdownFrontend(); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager has successfully shutdown frontend for engine %v", req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) FrontendStartCallback(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to process FrontendStartCallback of engine %v", req.Name)

	em.lock.Lock()
	defer em.lock.Unlock()

	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	tID := int32(0)

	el.lock.RLock()
	if el.scsiDevice == nil {
		tID, _, err = em.tIDAllocator.AllocateRange(1)
		if err != nil || tID == 0 {
			el.lock.RUnlock()
			return nil, fmt.Errorf("cannot get available tid for frontend start")
		}
	}
	el.lock.RUnlock()

	logrus.Debugf("Engine Manager allocated TID %v for frontend start callback", tID)

	if err := el.finishFrontendStart(int(tID)); err != nil {
		return nil, errors.Wrapf(err, "failed to callback for engine %v frontend start", req.Name)
	}

	logrus.Infof("Engine Manager finished engine %v frontend start callback", req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) FrontendShutdownCallback(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to process FrontendShutdownCallback of engine %v", req.Name)

	em.lock.Lock()
	defer em.lock.Unlock()

	el, exists := em.engineLaunchers[req.Name]
	if !exists {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	el.lock.RLock()
	if el.backupEngine != nil {
		el.lock.RUnlock()
		logrus.Infof("ignores the callback since engine launcher %v is deleting old engine for engine upgrade", req.Name)
		return &empty.Empty{}, nil
	}
	el.lock.RUnlock()

	if err = em.cleanupFrontend(el); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager finished engine %v frontend shutdown callback", req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) cleanupFrontend(el *Launcher) error {
	tID, err := el.finishFrontendShutdown()
	if err != nil {
		return errors.Wrapf(err, "failed to callback for engine %v frontend shutdown", el.LauncherName)
	}

	if err = em.tIDAllocator.ReleaseRange(int32(tID), int32(tID)); err != nil {
		return errors.Wrapf(err, "failed to release tid for engine %v frontend shutdown", el.LauncherName)
	}

	logrus.Debugf("Engine Manager released TID %v for frontend shutdown callback", tID)
	return nil
}
