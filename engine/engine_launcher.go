package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/iscsiblk"
	eutil "github.com/longhorn/longhorn-engine/util"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/util"
)

const (
	engineLauncherSuffix = "-launcher"
	engineLauncherName   = "%s" + engineLauncherSuffix

	FrontendTGTBlockDev = "tgt-blockdev"
	FrontendTGTISCSI    = "tgt-iscsi"

	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"

	WaitInterval = time.Second
	WaitCount    = 60

	SwitchWaitInterval = time.Second
	SwitchWaitCount    = 15

	ResourceVersionBufferValue = 100
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
	Frontend     string
	Backends     []string

	Endpoint string

	// This field reflects engine launcher version only.
	// The final resource version of engine process should combine
	// the version of engine launcher and that of related process.
	ResourceVersion int64

	scsiDevice *iscsiblk.ScsiDevice

	currentEngine *Engine
	pendingEngine *Engine

	isStopping bool
	// Set it before creating new engine spec
	// Unset it after waiting for old engine process deletion
	isUpgrading bool

	pm rpc.ProcessManagerServiceServer
}

func NewEngineLauncher(spec *rpc.EngineSpec, launcherAddr string,
	processUpdateCh <-chan interface{}, engineUpdateCh chan *Launcher,
	processManager rpc.ProcessManagerServiceServer) *Launcher {

	el := &Launcher{
		UUID:         spec.Uuid,
		LauncherName: spec.Name,
		LauncherAddr: launcherAddr,
		VolumeName:   spec.VolumeName,
		Size:         spec.Size,
		ListenIP:     spec.ListenIp,
		Frontend:     spec.Frontend,
		Backends:     spec.Backends,

		Endpoint: "",

		ResourceVersion: 1,

		currentEngine: NewEngine(spec, launcherAddr, processManager),
		pendingEngine: nil,

		lock:     &sync.RWMutex{},
		updateCh: engineUpdateCh,
		doneCh:   make(chan struct{}),

		pm: processManager,
	}
	go el.StartMonitoring(processUpdateCh)
	return el
}

func (el *Launcher) StartMonitoring(processUpdateCh <-chan interface{}) {
	done := false
	for {
		select {
		case resp := <-processUpdateCh:
			p, ok := resp.(*rpc.ProcessResponse)
			if !ok {
				logrus.Errorf("BUG: engine launcher: cannot get ProcessResponse from channel")
			}
			if p.Spec.Name == el.currentEngine.EngineName {
				el.updateCh <- el
			}
		case <-el.doneCh:
			done = true
			break
		}
		if done {
			break
		}
	}
	logrus.Debugf("Stopped process monitoring on engine launcher %v", el.GetLauncherName())
}

func (el *Launcher) RPCResponse() *rpc.EngineResponse {
	el.lock.RLock()
	defer el.lock.RUnlock()

	resp := &rpc.EngineResponse{
		Spec: &rpc.EngineSpec{
			Uuid:       el.UUID,
			Name:       el.LauncherName,
			VolumeName: el.VolumeName,
			Binary:     el.currentEngine.Binary,
			Listen:     el.currentEngine.Listen,
			ListenIp:   el.ListenIP,
			Size:       el.Size,
			Frontend:   el.Frontend,
			Backends:   el.Backends,
			Replicas:   el.currentEngine.Replicas,
		},
		Status: &rpc.EngineStatus{
			Endpoint:        el.Endpoint,
			ResourceVersion: el.ResourceVersion,
		},
	}

	processStatus := el.currentEngine.ProcessStatus()
	resp.Status.ProcessStatus = processStatus
	resp.Status.ResourceVersion += processStatus.ResourceVersion

	return resp
}

// Start will result in frontendStartCallback() being called automatically.
func (el *Launcher) Start() error {
	logrus.Debugf("engine launcher %v: prepare to start engine %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.Listen)

	if err := el.currentEngine.Start(); err != nil {
		return err
	}

	el.updateCh <- el

	logrus.Debugf("engine launcher %v: succeed to start engine %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.Listen)

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
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.Listen)
	return nil

}

func (el *Launcher) Upgrade(spec *rpc.EngineSpec) error {
	if err := el.prepareUpgrade(spec); err != nil {
		return errors.Wrapf(err, "failed to prepare to upgrade engine to %v", spec.Name)
	}

	if err := el.pendingEngine.Start(); err != nil {
		return errors.Wrapf(err, "failed to create upgrade engine %v", spec.Name)
	}

	if err := el.checkUpgradedEngineSocket(); err != nil {
		return errors.Wrapf(err, "failed to reload socket connection for new engine %v", spec.Name)
	}

	if err := el.pendingEngine.WaitForState(types.ProcessStateRunning); err != nil {
		return errors.Wrapf(err, "failed to wait for new engine running")
	}

	if err := el.finalizeUpgrade(); err != nil {
		return errors.Wrapf(err, "failed to finalize engine upgrade")
	}
	return nil
}

func (el *Launcher) prepareUpgrade(spec *rpc.EngineSpec) error {
	if _, err := os.Stat(spec.Binary); os.IsNotExist(err) {
		return errors.Wrapf(err, "cannot find the binary %v to be upgraded", spec.Binary)
	}

	el.lock.Lock()
	defer el.lock.Unlock()

	if el.currentEngine.Binary == spec.Binary || el.LauncherName != spec.Name {
		return fmt.Errorf("cannot upgrade with the same binary or the different engine")
	}

	el.ResourceVersion++

	logrus.Debugf("engine launcher %v: prepare for upgrade", el.LauncherName)

	el.isUpgrading = true

	el.pendingEngine = NewEngine(spec, el.LauncherAddr, el.pm)
	// refill missing fields from spec
	el.pendingEngine.Size = el.currentEngine.Size
	el.pendingEngine.VolumeName = el.VolumeName
	el.pendingEngine.LauncherName = el.LauncherName
	el.pendingEngine.ListenIP = el.ListenIP
	el.pendingEngine.Frontend = el.Frontend
	el.pendingEngine.Backends = el.Backends

	if err := util.RemoveFile(el.GetSocketPath()); err != nil {
		return errors.Wrapf(err, "failed to remove socket %v", el.GetSocketPath())
	}

	logrus.Debugf("engine launcher %v: preparation completed", el.LauncherName)

	return nil
}

func (el *Launcher) finalizeUpgrade() error {
	logrus.Debugf("engine launcher %v: finalize upgrade", el.LauncherName)

	defer func() { el.updateCh <- el }()

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
	if err := oldEngine.WaitForState(types.ProcessStateNotFound); err != nil {
		logrus.Warnf("engine launcher %v: failed to deleted old engine %v: %v", el.LauncherName, oldEngine.EngineName, err)
	}

	el.lock.Lock()
	el.ResourceVersion++
	el.isUpgrading = false
	el.lock.Unlock()

	return nil
}

func (el *Launcher) WaitForState(state string) error {
	return el.currentEngine.WaitForState(state)
}

func (el *Launcher) Log(srv rpc.EngineManagerService_EngineLogServer) error {
	return el.currentEngine.Log(srv)
}

func (el *Launcher) setFrontend(frontend string) error {
	el.lock.Lock()
	defer el.lock.Unlock()

	if el.Frontend != "" && el.scsiDevice != nil {
		if el.Frontend != frontend {
			return fmt.Errorf("engine frontend %v is already up and cannot be set to %v", el.Frontend, frontend)
		}
		logrus.Infof("Engine frontend %v is already up", frontend)
		return nil
	}

	if el.Frontend != "" && el.scsiDevice == nil {
		if el.Frontend != frontend {
			return fmt.Errorf("engine frontend %v cannot be set to %v and its frontend cannot be started before engine manager shutdown its frontend", el.Frontend, frontend)
		}
		return fmt.Errorf("engine frontend had been set to %v, but its frontend cannot be started before engine manager shutdown its frontend", frontend)
	}

	if el.Frontend == "" && el.scsiDevice != nil {
		return fmt.Errorf("BUG: engine launcher frontend is empty but scsi device hasn't been cleanup in frontend start")
	}

	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI {
		return fmt.Errorf("invalid frontend %v", frontend)
	}

	el.Frontend = frontend
	el.ResourceVersion++

	return nil
}

func (el *Launcher) FrontendStart(frontend string) error {
	logrus.Debugf("engine launcher %v: prepare to start frontend %v", el.LauncherName, frontend)

	if err := el.setFrontend(frontend); err != nil {
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

	el.lock.Lock()
	el.ResourceVersion++
	if el.scsiDevice == nil {
		el.Frontend = ""
		el.lock.Unlock()
		logrus.Debugf("Engine frontend is already down")
		return nil
	}

	if el.Frontend == "" {
		el.lock.Unlock()
		return fmt.Errorf("BUG: engine launcher frontend is empty but scsi device hasn't been cleanup in frontend shutdown")
	}
	el.lock.Unlock()

	// the controller will call back to launcher. be careful about deadlock
	if err := el.currentEngine.shutdownFrontend(); err != nil {
		return err
	}

	el.lock.Lock()
	el.Frontend = ""
	el.lock.Unlock()
	el.updateCh <- el

	logrus.Debugf("engine launcher %v: frontend has been shutdown", el.LauncherName)

	return nil
}

func (el *Launcher) FrontendStartCallback(tID int) error {
	logrus.Debugf("engine launcher %v: finishing frontend start", el.LauncherName)

	el.lock.Lock()
	defer func() { el.updateCh <- el }()
	defer el.lock.Unlock()

	el.ResourceVersion++

	// not going to use it
	stopCh := make(chan struct{})
	if err := <-el.WaitForSocket(stopCh); err != nil {
		return err
	}

	if el.scsiDevice != nil {
		return nil
	}

	bsOpts := fmt.Sprintf("size=%v", el.Size)
	scsiDev, err := iscsiblk.NewScsiDevice(el.VolumeName, el.GetSocketPath(), "longhorn", bsOpts, tID)
	if err != nil {
		return err
	}
	el.scsiDevice = scsiDev

	switch el.Frontend {
	case FrontendTGTBlockDev:
		if err := iscsiblk.StartScsi(el.scsiDevice); err != nil {
			return err
		}
		if err := el.createDev(); err != nil {
			return err
		}

		el.Endpoint = el.getDev()

		logrus.Infof("engine launcher %v: SCSI device %s created", el.LauncherName, el.scsiDevice.Device)
		break
	case FrontendTGTISCSI:
		if err := iscsiblk.SetupTarget(el.scsiDevice); err != nil {
			return err
		}

		el.Endpoint = el.scsiDevice.Target

		logrus.Infof("engine launcher %v: iSCSI target %s created", el.LauncherName, el.scsiDevice.Target)
		break
	default:
		return fmt.Errorf("unknown frontend %v", el.Frontend)
	}

	logrus.Debugf("engine launcher %v: frontend start succeed", el.LauncherName)

	return nil
}

func (el *Launcher) FrontendShutdownCallback() (int, error) {
	logrus.Debugf("engine launcher %v: finishing frontend shutdown", el.LauncherName)

	el.lock.Lock()
	defer func() { el.updateCh <- el }()
	defer el.lock.Unlock()

	el.ResourceVersion++

	if el.scsiDevice == nil {
		return 0, nil
	}

	switch el.Frontend {
	case FrontendTGTBlockDev:
		dev := el.getDev()
		if err := eutil.RemoveDevice(dev); err != nil {
			return 0, fmt.Errorf("engine launcher %v: fail to remove device %s: %v", el.LauncherName, dev, err)
		}
		if err := iscsiblk.StopScsi(el.VolumeName, el.scsiDevice.TargetID); err != nil {
			return 0, fmt.Errorf("engine launcher %v: fail to stop SCSI device: %v", el.LauncherName, err)
		}
		logrus.Infof("engine launcher %v: SCSI device %v shutdown", el.LauncherName, dev)
		break
	case FrontendTGTISCSI:
		if err := iscsiblk.DeleteTarget(el.scsiDevice.Target, el.scsiDevice.TargetID); err != nil {
			return 0, fmt.Errorf("engine launcher %v: fail to delete target %v", el.LauncherName, el.scsiDevice.Target)
		}
		logrus.Infof("engine launcher %v: SCSI target %v ", el.LauncherName, el.scsiDevice.Target)
		break
	case "":
		logrus.Infof("engine launcher %v: skip shutdown frontend since it's not enabled", el.LauncherName)
		break
	default:
		return 0, fmt.Errorf("engine launcher %v: unknown frontend %v", el.LauncherName, el.Frontend)
	}

	tID := el.scsiDevice.TargetID
	el.scsiDevice = nil
	el.Endpoint = ""

	logrus.Debugf("engine launcher %v: frontend shutdown succeed", el.LauncherName)

	return tID, nil
}

func (el *Launcher) GetSocketPath() string {
	if el.VolumeName == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-"+el.VolumeName+".sock")
}

func (el *Launcher) WaitForSocket(stopCh chan struct{}) chan error {
	errCh := make(chan error)
	go func(errCh chan error, stopCh chan struct{}) {
		socket := el.GetSocketPath()
		timeout := time.After(time.Duration(WaitCount) * WaitInterval)
		tick := time.Tick(WaitInterval)
		for {
			select {
			case <-timeout:
				errCh <- fmt.Errorf("engine launcher %v: wait for socket %v timed out", el.LauncherName, socket)
			case <-tick:
				if _, err := os.Stat(socket); err == nil {
					errCh <- nil
					return
				}
				logrus.Infof("engine launcher %v: wait for socket %v to show up", el.LauncherName, socket)
			case <-stopCh:
				logrus.Infof("engine launcher %v: stop wait for socket routine", el.LauncherName)
				return
			}
		}
	}(errCh, stopCh)

	return errCh
}

func (el *Launcher) ReloadSocketConnection() error {
	cmd := exec.Command("sg_raw", el.getDev(), "a6", "00", "00", "00", "00", "00")
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to reload socket connection")
	}
	return nil
}

func (el *Launcher) getDev() string {
	return filepath.Join(DevPath, el.VolumeName)
}

func (el *Launcher) createDev() error {
	if _, err := os.Stat(DevPath); os.IsNotExist(err) {
		if err := os.MkdirAll(DevPath, 0755); err != nil {
			logrus.Fatalf("engine launcher %v: Cannot create directory %v", el.LauncherName, DevPath)
		}
	}

	dev := el.getDev()
	if _, err := os.Stat(dev); err == nil {
		logrus.Warnf("Device %s already exists, clean it up", dev)
		if err := eutil.RemoveDevice(dev); err != nil {
			return errors.Wrapf(err, "cannot cleanup block device file %v", dev)
		}
	}

	if err := eutil.DuplicateDevice(el.scsiDevice.Device, dev); err != nil {
		return err
	}

	logrus.Debugf("engine launcher %v: Device %s is ready", el.LauncherName, dev)

	return nil
}

func (el *Launcher) IsSCSIDeviceEnabled() bool {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.scsiDevice != nil
}

func (el *Launcher) checkUpgradedEngineSocket() (err error) {
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
