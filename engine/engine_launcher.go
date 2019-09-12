package engine

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/longhorndev"
	"github.com/longhorn/longhorn-instance-manager/rpc"
)

const (
	engineLauncherSuffix = "-launcher"
	engineLauncherName   = "%s" + engineLauncherSuffix
)

type Launcher struct {
	lock     *sync.RWMutex
	updateCh chan<- *Launcher
	doneCh   chan struct{}

	UUID         string
	LauncherName string
	LauncherAddr string
	VolumeName   string
	ListenIP     string
	Size         int64
	Backends     []string

	dev longhorndev.DeviceService

	currentEngine *Engine
	pendingEngine *Engine

	isStopping bool
	// Set it before creating new engine spec
	// Unset it after waiting for old engine process deletion
	isUpgrading bool

	pm rpc.ProcessManagerServiceServer

	ec VolumeClientService
}

func NewEngineLauncher(spec *rpc.EngineSpec, launcherAddr string,
	engineUpdateCh chan *Launcher,
	processManager rpc.ProcessManagerServiceServer,
	dc longhorndev.DeviceCreator,
	ec VolumeClientService) (*Launcher, error) {
	var err error

	el := &Launcher{
		LauncherName: spec.Name,
		LauncherAddr: launcherAddr,
		VolumeName:   spec.VolumeName,
		Size:         spec.Size,
		ListenIP:     spec.ListenIp,
		Backends:     spec.Backends,

		currentEngine: NewEngine(spec, launcherAddr, processManager, ec),
		pendingEngine: nil,

		lock:     &sync.RWMutex{},
		updateCh: engineUpdateCh,
		doneCh:   make(chan struct{}),

		pm: processManager,

		ec: ec,
	}
	el.dev, err = dc.NewDevice(el.VolumeName, el.Size, spec.Frontend)
	if err != nil {
		return nil, err
	}
	return el, nil
}

func (el *Launcher) RPCResponse() *rpc.EngineResponse {
	el.lock.RLock()
	defer el.lock.RUnlock()

	resp := &rpc.EngineResponse{
		Spec: &rpc.EngineSpec{
			Name:       el.LauncherName,
			VolumeName: el.VolumeName,
			Binary:     el.currentEngine.Binary,
			Listen:     el.currentEngine.GetListen(),
			ListenIp:   el.ListenIP,
			Size:       el.Size,
			Frontend:   el.dev.GetFrontend(),
			Backends:   el.Backends,
			Replicas:   el.currentEngine.Replicas,
		},
		Status: &rpc.EngineStatus{
			Endpoint: el.dev.GetEndpoint(),
		},
	}

	processStatus := el.currentEngine.ProcessStatus()
	resp.Status.ProcessStatus = processStatus

	return resp
}

// Start will result in frontendStartCallback() being called automatically.
func (el *Launcher) Start() error {
	logrus.Debugf("engine launcher %v: prepare to start engine %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.GetListen())

	if err := el.currentEngine.Start(); err != nil {
		return err
	}

	el.updateCh <- el

	logrus.Debugf("engine launcher %v: succeed to start engine %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.GetListen())

	return nil
}

// Stop will result in frontendShutdownCallback() being called automatically.
func (el *Launcher) Stop() error {
	logrus.Debugf("engine launcher %v: prepare to stop engine %v",
		el.LauncherName, el.currentEngine.EngineName)

	if _, err := el.currentEngine.Stop(); err != nil {
		return err
	}
	el.lock.Lock()
	if !el.isStopping {
		close(el.doneCh)
	}
	el.isStopping = true
	el.lock.Unlock()

	el.updateCh <- el

	logrus.Debugf("engine launcher %v: succeed to stop engine %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.GetListen())
	return nil

}

func (el *Launcher) Upgrade() error {
	if el.pendingEngine == nil {
		return fmt.Errorf("pending engine is missing, no PrepareUpgrade was done")
	}
	if err := el.pendingEngine.Start(); err != nil {
		return errors.Wrapf(err, "failed to create upgrade engine %v", el.pendingEngine.EngineName)
	}

	if err := el.pendingEngine.WaitForRunning(); err != nil {
		return errors.Wrapf(err, "failed to wait for new engine running")
	}

	if err := el.finalizeUpgrade(); err != nil {
		return errors.Wrapf(err, "failed to finalize engine upgrade")
	}
	return nil
}

func (el *Launcher) PrepareUpgrade(spec *rpc.EngineSpec) (string, error) {
	if _, err := os.Stat(spec.Binary); os.IsNotExist(err) {
		return "", errors.Wrapf(err, "cannot find the binary %v to be upgraded", spec.Binary)
	}

	el.lock.Lock()
	defer el.lock.Unlock()

	if el.currentEngine.Binary == spec.Binary || el.LauncherName != spec.Name {
		return "", fmt.Errorf("cannot upgrade with the same binary or the different engine")
	}

	logrus.Debugf("engine launcher %v: prepare for upgrade", el.LauncherName)

	el.isUpgrading = true

	el.pendingEngine = NewEngine(spec, el.LauncherAddr, el.pm, el.ec)
	// refill missing fields from spec
	el.pendingEngine.Size = el.currentEngine.Size
	el.pendingEngine.VolumeName = el.VolumeName
	el.pendingEngine.LauncherName = el.LauncherName
	el.pendingEngine.ListenIP = el.ListenIP
	el.pendingEngine.Frontend = el.dev.GetFrontend()
	el.pendingEngine.Backends = el.Backends

	if err := el.dev.PrepareUpgrade(); err != nil {
		return "", err
	}

	logrus.Debugf("engine launcher %v: preparation completed", el.LauncherName)

	return el.pendingEngine.EngineName, nil
}

func (el *Launcher) finalizeUpgrade() error {
	logrus.Debugf("engine launcher %v: finalize upgrade", el.LauncherName)

	if err := el.dev.FinishUpgrade(); err != nil {
		return err
	}

	el.lock.Lock()

	oldEngine := el.currentEngine
	el.currentEngine = el.pendingEngine
	el.pendingEngine = nil

	el.lock.Unlock()

	if _, err := oldEngine.Stop(); err != nil {
		logrus.Warnf("failed to delete old engine process %v: %v", oldEngine.EngineName, err)
	}

	// We need to wait for old engine process deletion before unset el.isUpgrading
	// Typically engine process deletion will trigger frontend shutdown callback.
	// But we don't want to shutdown frontend here since it's live upgrade.
	// Hence frontend shutdown callback will check el.isUpgrading to skip unexpected frontend down.
	// And we need to block process here keep el.isUpgrading before frontend shutdown callback complete.
	if err := oldEngine.WaitForDeletion(); err != nil {
		logrus.Warnf("engine launcher %v: failed to deleted old engine %v: %v", el.LauncherName, oldEngine.EngineName, err)
	}

	el.lock.Lock()
	el.isUpgrading = false
	el.lock.Unlock()

	el.updateCh <- el
	return nil
}

func (el *Launcher) WaitForRunning() error {
	return el.currentEngine.WaitForRunning()
}

func (el *Launcher) WaitForDeletion() error {
	return el.currentEngine.WaitForDeletion()
}

func (el *Launcher) Log(srv rpc.EngineManagerService_EngineLogServer) error {
	return el.currentEngine.Log(srv)
}

func (el *Launcher) FrontendStart(frontend string) error {
	logrus.Debugf("engine launcher %v: prepare to start frontend %v", el.LauncherName, frontend)

	if err := el.dev.SetFrontend(frontend); err != nil {
		return err
	}
	el.updateCh <- el

	// the controller will call back to launcher. be careful about deadlock
	if err := el.currentEngine.startFrontend("socket"); err != nil {
		return err
	}

	logrus.Debugf("engine launcher %v: frontend %v has been started", el.LauncherName, frontend)

	return nil
}

func (el *Launcher) FrontendShutdown() error {
	logrus.Debugf("engine launcher %v: prepare to shutdown frontend", el.LauncherName)

	if err := el.dev.UnsetFrontendCheck(); err != nil {
		return err
	}

	// the controller will call back to launcher. be careful about deadlock
	if err := el.currentEngine.shutdownFrontend(); err != nil {
		return err
	}

	el.dev.UnsetFrontend()

	el.updateCh <- el

	logrus.Debugf("engine launcher %v: frontend has been shutdown", el.LauncherName)

	return nil
}

func (el *Launcher) FrontendStartCallback(tID int) error {
	logrus.Debugf("engine launcher %v: finishing frontend start", el.LauncherName)

	if err := el.dev.Start(tID); err != nil {
		return err
	}

	el.updateCh <- el

	logrus.Debugf("engine launcher %v: frontend start succeed", el.LauncherName)

	return nil
}

func (el *Launcher) FrontendShutdownCallback() (int, error) {
	logrus.Debugf("engine launcher %v: finishing frontend shutdown", el.LauncherName)

	tID, err := el.dev.Shutdown()
	if err != nil {
		return 0, err
	}

	el.updateCh <- el

	logrus.Debugf("engine launcher %v: frontend shutdown succeed", el.LauncherName)

	return tID, nil
}

func (el *Launcher) IsSCSIDeviceEnabled() bool {
	return el.dev.Enabled()
}

func (el *Launcher) IsUpgrading() bool {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.isUpgrading
}

func (el *Launcher) GetLauncherName() string {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.LauncherName
}

func (el *Launcher) Update() {
	el.updateCh <- el
}

func (el *Launcher) GetEngineName() string {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.currentEngine.EngineName
}
