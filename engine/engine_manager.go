package engine

import (
	"fmt"
	"os"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ibuildthecloud/kine/pkg/broadcaster"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/util"
)

/* Lock order
   1. Manager.lock
   2. Launcher.lock
*/

type Manager struct {
	pm rpc.ProcessManagerServiceServer

	lock   *sync.RWMutex
	listen string

	broadcaster     *broadcaster.Broadcaster
	broadcastCh     chan interface{}
	processUpdateCh <-chan interface{}

	elUpdateCh      chan *Launcher
	shutdownCh      chan error
	engineLaunchers map[string]*Launcher
	tIDAllocator    *util.Bitmap
}

const (
	MaxTgtTargetNumber = 4096
)

func NewEngineManager(pm rpc.ProcessManagerServiceServer, processUpdateCh <-chan interface{}, listen string, shutdownCh chan error) (*Manager, error) {
	em := &Manager{
		pm: pm,

		lock:   &sync.RWMutex{},
		listen: listen,

		broadcaster:     &broadcaster.Broadcaster{},
		broadcastCh:     make(chan interface{}),
		processUpdateCh: processUpdateCh,

		elUpdateCh:      make(chan *Launcher),
		shutdownCh:      shutdownCh,
		engineLaunchers: map[string]*Launcher{},
		tIDAllocator:    util.NewBitmap(1, MaxTgtTargetNumber),
	}
	// help to kickstart the broadcaster
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := em.broadcaster.Subscribe(c, em.broadcastConnector); err != nil {
		return nil, err
	}
	go em.StartMonitoring()
	return em, nil
}

func (em *Manager) StartMonitoring() {
	done := false
	for {
		select {
		case <-em.shutdownCh:
			logrus.Infof("Engine Manager is shutting down")
			done = true
			break
		case el := <-em.elUpdateCh:
			resp := el.RPCResponse()
			em.lock.RLock()
			// Modify response to indicate deletion.
			if _, exists := em.engineLaunchers[el.GetLauncherName()]; !exists {
				resp.Deleted = true
			}
			em.lock.RUnlock()

			em.broadcastCh <- interface{}(resp)
		}
		if done {
			break
		}
	}
}

// EngineCreate will create an engine according to the request
// If the specified engine name exists already, the creation will fail.
func (em *Manager) EngineCreate(ctx context.Context, req *rpc.EngineCreateRequest) (ret *rpc.EngineResponse, err error) {
	logrus.Infof("Engine Manager starts to create engine of volume %v", req.Spec.VolumeName)

	el := NewEngineLauncher(req.Spec, em.listen, em.processUpdateCh, em.elUpdateCh, em.pm)
	if err := em.registerEngineLauncher(el); err != nil {
		return nil, errors.Wrapf(err, "failed to register engine launcher %v", el.GetLauncherName())
	}
	if err := el.Start(); err != nil {
		go em.unregisterEngineLauncher(req.Spec.Name)
		return nil, errors.Wrapf(err, "failed to start engine %v", req.Spec.Name)
	}

	logrus.Infof("Engine Manager has successfully created engine %v", req.Spec.Name)

	return el.RPCResponse(), nil
}

func (em *Manager) registerEngineLauncher(el *Launcher) error {
	em.lock.Lock()
	defer em.lock.Unlock()

	_, exists := em.engineLaunchers[el.GetLauncherName()]
	if exists {
		return fmt.Errorf("engine launcher %v already exists", el.GetLauncherName())
	}

	em.engineLaunchers[el.GetLauncherName()] = el
	el.Update()

	return nil
}

func (em *Manager) unregisterEngineLauncher(launcherName string) {
	logrus.Debugf("Engine Manager starts to unregistered engine launcher %v", launcherName)

	el := em.getLauncher(launcherName)
	if el == nil {
		return
	}

	if err := el.WaitForState(types.ProcessStateNotFound); err != nil {
		logrus.Errorf("Engine Manager fails to unregister engine launcher %v: %v", launcherName, err)
	}

	if el.IsSCSIDeviceEnabled() {
		logrus.Warnf("Engine Manager need to cleanup frontend before unregistering engine launcher %v", launcherName)
		if err := em.cleanupFrontend(el); err != nil {
			// cleanup failed. cannot unregister engine launcher.
			logrus.Errorf("Engine Manager fails to cleanup frontend before unregistering engine launcher %v", launcherName)
			return
		}
	}
	em.lock.Lock()
	delete(em.engineLaunchers, launcherName)
	em.lock.Unlock()

	logrus.Infof("Engine Manager had successfully unregistered engine launcher %v, deletion completed", launcherName)
	el.Update()
	return
}

// EngineDelete will delete the engine named by the request
// If the specified engine doesn't exist, the deletion will return with ProcessStateNotFound set in the response
func (em *Manager) EngineDelete(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.EngineResponse, err error) {
	logrus.Infof("Engine Manager starts to deleted engine %v", req.Name)

	el := em.getLauncher(req.Name)
	if el == nil {
		return &rpc.EngineResponse{
			Spec: &rpc.EngineSpec{
				Name: req.Name,
			},
			Status: &rpc.EngineStatus{
				ProcessStatus: &rpc.ProcessStatus{
					State: types.ProcessStateNotFound,
				},
			},
		}, nil
	}

	if err := el.Stop(); err != nil {
		return nil, err
	}

	go em.unregisterEngineLauncher(req.Name)

	logrus.Infof("Engine Manager is deleting engine %v", req.Name)

	return el.RPCResponse(), nil
}

// EngineGet will get the engine named by the request
// If the specified engine doesn't exist, the deletion will return with ProcessStateNotFound set in the response
func (em *Manager) EngineGet(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.EngineResponse, err error) {
	el := em.getLauncher(req.Name)
	if el == nil {
		return &rpc.EngineResponse{
			Spec: &rpc.EngineSpec{
				Name: req.Name,
			},
			Status: &rpc.EngineStatus{
				ProcessStatus: &rpc.ProcessStatus{
					State: types.ProcessStateNotFound,
				},
			},
		}, nil
	}

	return el.RPCResponse(), nil
}

func (em *Manager) EngineList(ctx context.Context, req *empty.Empty) (ret *rpc.EngineListResponse, err error) {
	em.lock.RLock()
	defer em.lock.RUnlock()

	ret = &rpc.EngineListResponse{
		Engines: map[string]*rpc.EngineResponse{},
	}

	for _, el := range em.engineLaunchers {
		ret.Engines[el.GetLauncherName()] = el.RPCResponse()
	}

	return ret, nil
}

// EngineUpgrade will upgrade the engine according to the request
// If the specified engine doesn't exist, the upgrade will return error
func (em *Manager) EngineUpgrade(ctx context.Context, req *rpc.EngineUpgradeRequest) (ret *rpc.EngineResponse, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to upgrade engine for %v(%v) ", req.Spec.Name, req.Spec.VolumeName)
	}()

	logrus.Infof("Engine Manager starts to upgrade engine %v for volume %v", req.Spec.Name, req.Spec.VolumeName)

	el, err := em.validateBeforeUpgrade(req.Spec)
	if err != nil {
		return nil, err
	}

	if err := el.Upgrade(req.Spec); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager has successfully upgraded engine %v with binary %v", req.Spec.Name, req.Spec.Binary)

	return el.RPCResponse(), nil
}

func (em *Manager) EngineLog(req *rpc.LogRequest, srv rpc.EngineManagerService_EngineLogServer) error {
	logrus.Debugf("Engine Manager getting logs for engine %v", req.Name)

	el := em.getLauncher(req.Name)
	if el == nil {
		return fmt.Errorf("cannot find engine %v", req.Name)
	}

	if err := el.Log(srv); err != nil {
		return err
	}

	logrus.Debugf("Engine Manager has successfully retrieved logs for engine %v", req.Name)

	return nil
}

func (em *Manager) broadcastConnector() (chan interface{}, error) {
	return em.broadcastCh, nil
}

func (em *Manager) EngineWatch(req *empty.Empty, srv rpc.EngineManagerService_EngineWatchServer) (err error) {
	responseChan, err := em.broadcaster.Subscribe(context.TODO(), em.broadcastConnector)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.Errorf("engine manager update watch errored out: %v", err)
		} else {
			logrus.Debugf("engine manager update watch ended successfully")
		}
	}()
	logrus.Debugf("started new engine manager update watch")

	for resp := range responseChan {
		r, ok := resp.(*rpc.EngineResponse)
		if !ok {
			return fmt.Errorf("BUG: cannot get ProcessResponse from channel")
		}
		if err := srv.Send(r); err != nil {
			return err
		}
	}

	return nil
}

func (em *Manager) validateBeforeUpgrade(spec *rpc.EngineSpec) (*Launcher, error) {
	if _, err := os.Stat(spec.Binary); os.IsNotExist(err) {
		return nil, errors.Wrap(err, "cannot find the binary to be upgraded")
	}

	el := em.getLauncher(spec.Name)
	if el == nil {
		return nil, fmt.Errorf("cannot find engine %v", spec.Name)
	}

	return el, nil
}

func (em *Manager) FrontendStart(ctx context.Context, req *rpc.FrontendStartRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to start frontend %v for engine %v", req.Frontend, req.Name)

	el := em.getLauncher(req.Name)
	if el == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	// the controller will call back to engine manager later. be careful about deadlock
	if err := el.startFrontend(req.Frontend); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager has successfully start frontend %v for engine %v", req.Frontend, req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) FrontendShutdown(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to shutdown frontend for engine %v", req.Name)

	el := em.getLauncher(req.Name)
	if el == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	// the controller will call back to engine manager later. be careful about deadlock
	if err := el.shutdownFrontend(); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager has successfully shutdown frontend for engine %v", req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) FrontendStartCallback(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to process FrontendStartCallback of engine %v", req.Name)

	el := em.getLauncher(req.Name)
	if el == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	tID := int32(0)

	if el.IsUpgrading() {
		logrus.Infof("ignores the FrontendStartCallback since engine launcher %v is starting a new engine for engine upgrade", req.Name)
		return &empty.Empty{}, nil
	}

	if !el.IsSCSIDeviceEnabled() {
		tID, _, err = em.tIDAllocator.AllocateRange(1)
		if err != nil {
			return nil, fmt.Errorf("cannot get available tid for frontend start")
		}
	}

	logrus.Debugf("Engine Manager allocated TID %v for frontend start callback", tID)

	if err := el.finishFrontendStart(int(tID)); err != nil {
		return nil, errors.Wrapf(err, "failed to callback for engine %v frontend start", req.Name)
	}

	logrus.Infof("Engine Manager finished engine %v frontend start callback", req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) FrontendShutdownCallback(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	logrus.Infof("Engine Manager starts to process FrontendShutdownCallback of engine %v", req.Name)

	el := em.getLauncher(req.Name)
	if el == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	if el.IsUpgrading() {
		logrus.Infof("Ignores the FrontendShutdownCallback since engine launcher %v is deleting old engine for engine upgrade", req.Name)
		return &empty.Empty{}, nil
	}

	if err = em.cleanupFrontend(el); err != nil {
		return nil, err
	}

	logrus.Infof("Engine Manager finished engine %v frontend shutdown callback", req.Name)

	return &empty.Empty{}, nil
}

func (em *Manager) cleanupFrontend(el *Launcher) error {
	tID, err := el.finishFrontendShutdown()
	if err != nil {
		return errors.Wrapf(err, "failed to callback for engine %v frontend shutdown", el.GetLauncherName())
	}

	if err = em.tIDAllocator.ReleaseRange(int32(tID), int32(tID)); err != nil {
		return errors.Wrapf(err, "failed to release tid for engine %v frontend shutdown", el.GetLauncherName())
	}

	logrus.Debugf("Engine Manager released TID %v for frontend shutdown callback", tID)
	return nil
}

func (em *Manager) getLauncher(name string) *Launcher {
	em.lock.RLock()
	defer em.lock.RUnlock()
	return em.engineLaunchers[name]
}
