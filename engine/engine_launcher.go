package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	lock      *sync.RWMutex
	pUpdateCh chan *rpc.ProcessResponse
	UpdateCh  chan<- *Launcher

	UUID         string
	LauncherName string
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

	isDeleting bool
	// Set it before creating new engine spec
	// Unset it after waiting for old engine process deletion
	isUpgrading bool
}

func NewEngineLauncher(spec *rpc.EngineSpec) (*Launcher, *Engine) {
	el := &Launcher{
		UUID:         spec.Uuid,
		LauncherName: spec.Name,
		VolumeName:   spec.VolumeName,
		Size:         spec.Size,
		ListenIP:     spec.ListenIp,
		Frontend:     spec.Frontend,
		Backends:     spec.Backends,

		Endpoint: "",

		ResourceVersion: 1,

		currentEngine: NewEngine(spec),
		pendingEngine: nil,

		lock:      &sync.RWMutex{},
		pUpdateCh: make(chan *rpc.ProcessResponse),
	}
	go el.StartMonitoring()
	return el, el.currentEngine
}

func (el *Launcher) StartMonitoring() {
	for p := range el.pUpdateCh {
		if p.Spec.Name == el.currentEngine.EngineName {
			el.UpdateCh <- el
		}
	}
	el.lock.RLock()
	launcherName := el.LauncherName
	el.lock.RUnlock()
	logrus.Debugf("Stopped process monitoring on engine launcher %v", launcherName)
}

func (el *Launcher) RPCResponse(processManager rpc.ProcessManagerServiceServer, mayBeDeleting bool) (*rpc.EngineResponse, error) {
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

	process, err := processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: el.currentEngine.EngineName,
	})
	if err != nil {
		// if the engine is in deletion, we may fail to get the related process info
		// and `process` can be nil
		if !mayBeDeleting || !strings.Contains(err.Error(), "cannot find process") {
			return nil, errors.Wrapf(err, "failed to get the related process info for engine %v",
				el.LauncherName)
		}
	}

	// `process` can be nil only when the engine is in deletion.
	// Race condition: try to get the engine whose related process has been deleted
	// but the engine launcher hasn't been unregistered from Engine Manager. Hence we
	// will manually set process status for this kind of engine.
	if process == nil {
		resp.Status.ProcessStatus = &rpc.ProcessStatus{
			State: types.ProcessStateStopped,
		}
		resp.Deleted = true
	} else {
		resp.Status.ProcessStatus = process.Status
		// the response should reflect the final resource version
		resp.Status.ResourceVersion += process.Status.ResourceVersion
	}

	return resp, nil
}

// During running this function, frontendStartCallback() will be called automatically.
// Hence need to be careful about deadlock
// engineSpec should be el.currentEngine or el.pendingEngine
func (el *Launcher) createEngineProcess(engineSpec *Engine, engineManagerListen string,
	processManager rpc.ProcessManagerServiceServer) error {

	logrus.Debugf("engine launcher %v: prepare to create engine process %v at %v",
		el.LauncherName, engineSpec.EngineName, engineSpec.Listen)

	el.lock.Lock()
	el.ResourceVersion++

	portCount := 0

	args := []string{
		"controller", el.VolumeName,
		"--launcher", engineManagerListen,
		"--launcher-id", el.LauncherName,
	}

	portArgs := []string{}
	if engineSpec.Listen != "" {
		args = append(args, "--listen", engineSpec.Listen)
	} else {
		if el.ListenIP == "" {
			el.lock.Unlock()
			return fmt.Errorf("neither arg listen nor arg listenIP is provided for engine %v", engineSpec.EngineName)
		}
		portArgs = append(portArgs, fmt.Sprintf("--listen,%s:", el.ListenIP))
		portCount = portCount + 1
	}

	if el.Frontend != "" {
		args = append(args, "--frontend", "socket")
	}
	for _, b := range el.Backends {
		args = append(args, "--enable-backend", b)
	}

	for _, r := range engineSpec.Replicas {
		args = append(args, "--replica", r)
	}

	req := &rpc.ProcessCreateRequest{
		Spec: &rpc.ProcessSpec{
			Name:      engineSpec.EngineName,
			Binary:    engineSpec.Binary,
			Args:      args,
			PortArgs:  portArgs,
			PortCount: int32(portCount),
		},
	}
	el.lock.Unlock()

	// engine process creation may involve in FrontendStart. be careful about deadlock
	ret, err := processManager.ProcessCreate(nil, req)
	if err != nil {
		return err
	}

	el.lock.Lock()
	el.ResourceVersion++
	if engineSpec.Listen == "" {
		engineSpec.Listen = util.GetURL(el.ListenIP, int(ret.Status.PortStart))
	}

	el.lock.Unlock()
	el.UpdateCh <- el

	logrus.Debugf("engine launcher %v: succeed to create engine process %v at %v",
		el.LauncherName, engineSpec.EngineName, engineSpec.Listen)

	return nil
}

// During running this function, frontendShutdownCallback() will be called automatically.
// Hence need to be careful about deadlock
func (el *Launcher) deleteEngine(processManager rpc.ProcessManagerServiceServer, processName string) (*rpc.ProcessResponse, error) {
	logrus.Debugf("engine launcher %v: prepare to delete engine process %v",
		el.LauncherName, el.currentEngine.EngineName)

	response, err := processManager.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: processName,
	})
	if err != nil {
		if strings.Contains(err.Error(), "cannot find process") {
			return nil, nil
		}
		return nil, err
	}

	return response, nil
}

func (el *Launcher) prepareUpgrade(spec *rpc.EngineSpec) (*Engine, error) {
	el.lock.Lock()
	defer el.lock.Unlock()

	el.ResourceVersion++

	logrus.Debugf("engine launcher %v: prepare for upgrade", el.LauncherName)

	el.isUpgrading = true
	el.pendingEngine = NewEngine(spec)
	el.pendingEngine.Size = el.currentEngine.Size

	if err := util.RemoveFile(el.GetSocketPath()); err != nil {
		return nil, errors.Wrapf(err, "failed to remove socket %v", el.GetSocketPath())
	}

	logrus.Debugf("engine launcher %v: preparation completed", el.LauncherName)

	return el.pendingEngine, nil
}

func (el *Launcher) finalizeUpgrade(processManager rpc.ProcessManagerServiceServer) error {
	logrus.Debugf("engine launcher %v: finalize upgrade", el.LauncherName)

	defer func() { el.UpdateCh <- el }()

	el.lock.Lock()
	processName := el.currentEngine.EngineName

	process, err := processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: processName,
	})
	if err != nil {
		el.lock.Unlock()
		return errors.Wrapf(err, "failed to get old engine process before switching to the new engine process")
	}
	// Since the sum of process resource version and engine launcher resource version cannot decrease,
	// we need to coalesce the resource version of old process into that of engine launcher.
	// Besides, the final resource version of the old engine process can be greater than
	// `process.Status.ResourceVersion`, we need to add a buffer value to compensate it.
	el.ResourceVersion = el.ResourceVersion + process.Status.ResourceVersion + ResourceVersionBufferValue

	el.currentEngine = el.pendingEngine
	el.pendingEngine = nil

	el.lock.Unlock()

	if _, err := el.deleteEngine(processManager, processName); err != nil {
		logrus.Warnf("failed to delete old engine process %v: %v", processName, err)
	}

	// We need to wait for old engine process deletion before unset el.isUpgrading
	// Typically engine process deletion will trigger frontend shutdown callback.
	// But we don't want to shutdown frontend here since it's live upgrade.
	// Hence frontend shutdown callback will check el.isUpgrading to skip unexpected frontend down.
	// And we need to block process here keep el.isUpgrading before frontend shutdown callback complete.
	isDeleted := el.waitForEngineProcessDeletion(processManager, processName)
	if !isDeleted {
		logrus.Warnf("engine launcher %v: failed to deleted old engine process", el.LauncherName)
	}

	el.lock.Lock()
	el.ResourceVersion++
	el.isUpgrading = false
	el.lock.Unlock()

	return nil
}

func (el *Launcher) waitForEngineProcessDeletion(processManager rpc.ProcessManagerServiceServer, processName string) bool {
	for i := 0; i < types.WaitCount; i++ {
		if _, err := processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: processName,
		}); err != nil && strings.Contains(err.Error(), "cannot find process") {
			break
		}
		logrus.Infof("engine launcher %v: waiting for engine process %v to shutdown before unregistering the engine launcher", el.LauncherName, processName)
		time.Sleep(types.WaitInterval)
	}

	if _, err := processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: processName,
	}); err != nil && strings.Contains(err.Error(), "cannot find process") {
		logrus.Infof("engine launcher %v: successfully deleted engine process", el.LauncherName)
		return true
	}
	logrus.Errorf("engine launcher %v: failed to deleted engine process", el.LauncherName)
	return false
}

func (el *Launcher) waitForEngineProcessRunning(processManager rpc.ProcessManagerServiceServer, processName string) error {
	for i := 0; i < types.WaitCount; i++ {
		process, err := processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: processName,
		})
		if err != nil && !strings.Contains(err.Error(), "cannot find process") {
			return err
		}
		if process != nil && process.Status.State == types.ProcessStateRunning {
			break
		}
		logrus.Infof("engine launcher %v: waiting for engine process %v running", el.LauncherName, processName)
		time.Sleep(types.WaitInterval)
	}

	process, err := processManager.ProcessGet(nil, &rpc.ProcessGetRequest{
		Name: processName,
	})
	if err != nil || process == nil || process.Status.State != types.ProcessStateRunning {
		return fmt.Errorf("engine launcher %v: failed to wait for engine process %v running", el.LauncherName, processName)
	}
	return nil
}

func (el *Launcher) engineLog(req *rpc.LogRequest, srv rpc.EngineManagerService_EngineLogServer,
	processManager rpc.ProcessManagerServiceServer) error {

	if err := processManager.ProcessLog(req, srv); err != nil {
		return err
	}

	return nil
}

func (el *Launcher) startFrontend(frontend string) error {
	logrus.Debugf("engine launcher %v: prepare to start frontend %v", el.LauncherName, frontend)

	el.lock.Lock()
	el.ResourceVersion++

	if el.Frontend != "" && el.scsiDevice != nil {
		if el.Frontend != frontend {
			el.lock.Unlock()
			return fmt.Errorf("engine frontend %v is already up and cannot be set to %v", el.Frontend, frontend)
		}
		el.lock.Unlock()
		logrus.Infof("Engine frontend %v is already up", frontend)
		return nil
	}

	if el.Frontend != "" && el.scsiDevice == nil {
		if el.Frontend != frontend {
			el.lock.Unlock()
			return fmt.Errorf("engine frontend %v cannot be set to %v and its frontend cannot be started before engine manager shutdown its frontend", el.Frontend, frontend)
		}
		el.lock.Unlock()
		return fmt.Errorf("engine frontend had been set to %v, but its frontend cannot be started before engine manager shutdown its frontend", frontend)
	}

	if el.Frontend == "" && el.scsiDevice != nil {
		el.lock.Unlock()
		return fmt.Errorf("BUG: engine launcher frontend is empty but scsi device hasn't been cleanup in frontend start")
	}

	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI {
		el.lock.Unlock()
		return fmt.Errorf("invalid frontend %v", frontend)
	}

	el.Frontend = frontend
	el.lock.Unlock()
	el.UpdateCh <- el

	// the controller will call back to launcher. be careful about deadlock
	if err := el.currentEngine.startFrontend("socket"); err != nil {
		return err
	}

	logrus.Debugf("engine launcher %v: frontend %v has been started", el.LauncherName, frontend)

	return nil
}

func (el *Launcher) shutdownFrontend() error {
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
	el.UpdateCh <- el

	logrus.Debugf("engine launcher %v: frontend has been shutdown", el.LauncherName)

	return nil
}

func (el *Launcher) finishFrontendStart(tID int) error {
	logrus.Debugf("engine launcher %v: finishing frontend start", el.LauncherName)

	el.lock.Lock()
	defer func() { el.UpdateCh <- el }()
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

func (el *Launcher) finishFrontendShutdown() (int, error) {
	logrus.Debugf("engine launcher %v: finishing frontend shutdown", el.LauncherName)

	el.lock.Lock()
	defer func() { el.UpdateCh <- el }()
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

func (el *Launcher) GetCurrentEngineName() string {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.currentEngine.EngineName
}

func (el *Launcher) IsSCSIDeviceEnabled() bool {
	el.lock.RLock()
	defer el.lock.RUnlock()
	return el.scsiDevice != nil
}

func (el *Launcher) ValidateNameAndBinary(name, binary string) error {
	el.lock.RLock()
	defer el.lock.RUnlock()

	if el.currentEngine.Binary == binary || el.LauncherName != name {
		return fmt.Errorf("cannot upgrade with the same binary or the different engine")
	}
	return nil
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

func (el *Launcher) SetAndCheckIsDeleting() bool {
	el.lock.Lock()
	defer el.lock.Unlock()

	old := el.isDeleting
	el.isDeleting = true
	return old
}
