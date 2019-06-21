package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/longhorn/longhorn-engine-launcher/util"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/iscsiblk"
	eutil "github.com/longhorn/longhorn-engine/util"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
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
)

type Launcher struct {
	lock *sync.RWMutex

	LauncherName string
	VolumeName   string
	Binary       string
	ListenIP     string
	Size         int64
	Frontend     string
	Backends     []string

	Endpoint string

	scsiDevice   *iscsiblk.ScsiDevice
	binaryBackup string

	currentEngine *Engine
	backupEngine  *Engine
}

func NewEngineLauncher(spec *rpc.EngineSpec) *Launcher {
	el := &Launcher{
		LauncherName: spec.Name,
		VolumeName:   spec.VolumeName,
		Binary:       spec.Binary,
		Size:         spec.Size,
		ListenIP:     spec.ListenIp,
		Frontend:     spec.Frontend,
		Backends:     spec.Backends,

		Endpoint: "",

		binaryBackup: "",

		currentEngine: NewEngine(spec),
		backupEngine:  nil,

		lock: &sync.RWMutex{},
	}
	return el
}

func (el *Launcher) RPCResponse(processStatus *rpc.ProcessStatus) *rpc.EngineResponse {
	el.lock.RLock()
	defer el.lock.RUnlock()

	return &rpc.EngineResponse{
		Spec: &rpc.EngineSpec{
			Name:       el.LauncherName,
			VolumeName: el.VolumeName,
			Binary:     el.Binary,
			Listen:     el.currentEngine.Listen,
			ListenIp:   el.ListenIP,
			Size:       el.Size,
			Frontend:   el.Frontend,
			Backends:   el.Backends,
			Replicas:   el.currentEngine.Replicas,
		},
		Status: &rpc.EngineStatus{
			ProcessStatus: processStatus,
			Endpoint:      el.Endpoint,
		},
	}
}

// During running this function, frontendStartCallback() will be called automatically.
// Hence need to be careful about deadlock
func (el *Launcher) createEngineProcess(engineManagerListen string,
	processLauncher rpc.LonghornProcessLauncherServiceServer) error {

	logrus.Debugf("engine launcher %v: prepare to create engine process %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.Listen)

	el.lock.Lock()

	portCount := 0

	args := []string{
		"controller", el.VolumeName,
		"--launcher", engineManagerListen,
		"--launcher-id", el.LauncherName,
	}

	portArgs := []string{}
	if el.currentEngine.Listen != "" {
		args = append(args, "--listen", el.currentEngine.Listen)
	} else {
		if el.ListenIP == "" {
			el.lock.Unlock()
			return fmt.Errorf("neither arg listen nor arg listenIP is provided for engine %v", el.currentEngine.EngineName)
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

	for _, r := range el.currentEngine.Replicas {
		args = append(args, "--replica", r)
	}

	req := &rpc.ProcessCreateRequest{
		Spec: &rpc.ProcessSpec{
			Name:      el.currentEngine.EngineName,
			Binary:    el.Binary,
			Args:      args,
			PortArgs:  portArgs,
			PortCount: int32(portCount),
		},
	}
	el.lock.Unlock()

	// engine process creation may involve in FrontendStart. be careful about deadlock
	ret, err := processLauncher.ProcessCreate(nil, req)
	if err != nil {
		return err
	}

	el.lock.Lock()
	if el.currentEngine.Listen == "" {
		el.currentEngine.Listen = util.GetURL(el.ListenIP, int(ret.Status.PortStart))
	}

	el.lock.Unlock()

	logrus.Debugf("engine launcher %v: succeed to create engine process %v at %v",
		el.LauncherName, el.currentEngine.EngineName, el.currentEngine.Listen)

	return nil
}

// During running this function, frontendShutdownCallback() will be called automatically.
// Hence need to be careful about deadlock
func (el *Launcher) deleteEngine(processLauncher rpc.LonghornProcessLauncherServiceServer) (*rpc.ProcessResponse, error) {
	logrus.Debugf("engine launcher %v: prepare to delete engine process %v",
		el.LauncherName, el.currentEngine.EngineName)

	response, err := processLauncher.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: el.currentEngine.EngineName,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

// During running this function, frontendShutdownCallback() will be called automatically.
// Hence need to be careful about deadlock
func (el *Launcher) deleteBackupEngine(processLauncher rpc.LonghornProcessLauncherServiceServer) error {
	logrus.Debugf("engine launcher %v: prepare to delete backup engine process %v",
		el.LauncherName, el.backupEngine.EngineName)

	if el.backupEngine == nil {
		return fmt.Errorf("cannot delete, backup engine doesn't exist")
	}

	if _, err := processLauncher.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: el.backupEngine.EngineName,
	}); err != nil {
		return err
	}

	return nil
}

func (el *Launcher) prepareUpgrade(spec *rpc.EngineSpec) error {
	el.lock.Lock()
	defer el.lock.Unlock()

	logrus.Debugf("engine launcher %v: prepare for upgrade", el.LauncherName)

	if err := el.backupBinary(spec.Binary); err != nil {
		return errors.Wrap(err, "failed to backup old engine binary")
	}

	el.backupEngine = el.currentEngine
	el.currentEngine = NewEngine(spec)
	el.currentEngine.Size = el.backupEngine.Size

	if err := util.RemoveFile(el.GetSocketPath()); err != nil {
		return errors.Wrapf(err, "failed to remove socket %v", el.GetSocketPath())
	}

	logrus.Debugf("engine launcher %v: preparation completed", el.LauncherName)

	return nil
}

func (el *Launcher) rollbackUpgrade(processLauncher rpc.LonghornProcessLauncherServiceServer) error {
	el.lock.Lock()
	defer el.lock.Unlock()

	logrus.Debugf("engine launcher %v: rolling back upgrade", el.LauncherName)

	if err := el.restoreBackupBinary(); err != nil {
		return errors.Wrap(err, "failed to restore old controller binary")
	}

	if el.backupEngine != nil {
		el.lock.Unlock()
		el.deleteEngine(processLauncher)
		el.lock.Lock()

		el.currentEngine = el.backupEngine
		el.backupEngine = nil
	}

	logrus.Debugf("engine launcher %v: rollback completed", el.LauncherName)

	return nil
}

func (el *Launcher) finalizeUpgrade(processLauncher rpc.LonghornProcessLauncherServiceServer) {
	logrus.Debugf("engine launcher %v: finalize upgrade", el.LauncherName)

	if err := el.deleteBackupEngine(processLauncher); err != nil {
		logrus.Warnf("failed to delete backup engine process %v: %v", el.backupEngine.EngineName, err)
	}

	el.lock.Lock()
	defer el.lock.Unlock()

	el.binaryBackup = ""
	el.backupEngine = nil

	logrus.Debugf("engine launcher %v: finalize completed", el.LauncherName)

	return
}

func (el *Launcher) backupBinary(newBinary string) error {
	if el.binaryBackup != "" {
		logrus.Warnf("binary backup %v already exists", el.binaryBackup)
		return nil
	}

	el.binaryBackup = el.Binary
	el.Binary = newBinary

	logrus.Debugf("backup binary %v and ready to use new binary %v", el.Binary, el.binaryBackup)

	return nil
}

func (el *Launcher) restoreBackupBinary() error {
	if el.binaryBackup == "" {
		return fmt.Errorf("cannot restore, binary backup doesn't exist")
	}

	el.Binary = el.binaryBackup
	el.binaryBackup = ""

	logrus.Debugf("backup binary restored to %v", el.Binary)

	return nil
}

func (el *Launcher) startFrontend(frontend string) error {
	logrus.Debugf("engine launcher %v: prepare to start frontend %v", el.LauncherName, frontend)

	el.lock.Lock()

	if el.Frontend == frontend && el.Frontend != "" && el.scsiDevice != nil {
		logrus.Debugf("Engine frontend %v is already up", el.Frontend)
		el.lock.Unlock()
		return nil
	}

	if el.Frontend != "" && el.scsiDevice == nil {
		el.lock.Unlock()
		return fmt.Errorf("BUG: frontend of engine launcher %v is set to %v but not scsi device", el.LauncherName, el.Frontend)
	}

	if el.Frontend != "" {
		el.lock.Unlock()
		return fmt.Errorf("cannot set frontend since it's already set to %v", el.Frontend)
	}

	if frontend != FrontendTGTBlockDev && frontend != FrontendTGTISCSI {
		el.lock.Unlock()
		return fmt.Errorf("invalid frontend %v", frontend)
	}

	el.Frontend = frontend
	el.lock.Unlock()

	// the controller will call back to launcher. be careful about deadlock
	if err := el.currentEngine.startFrontend("socket"); err != nil {
		return err
	}

	logrus.Debugf("engine launcher %v: frontend %v has been started", el.LauncherName, frontend)

	return nil
}

func (el *Launcher) shutdownFrontend() error {
	logrus.Debugf("engine launcher %v: prepare to shutdown frontend", el.LauncherName)

	el.lock.RLock()
	if el.Frontend == "" || el.scsiDevice == nil {
		el.lock.RUnlock()
		logrus.Debugf("Engine frontend is already down")
		return nil
	}
	el.lock.RUnlock()

	// the controller will call back to launcher. be careful about deadlock
	if err := el.currentEngine.shutdownFrontend(); err != nil {
		return err
	}

	el.lock.Lock()
	el.Frontend = ""
	el.lock.Unlock()

	logrus.Debugf("engine launcher %v: frontend has been shutdown", el.LauncherName)

	return nil
}

func (el *Launcher) finishFrontendStart(tID int) error {
	logrus.Debugf("engine launcher %v: finishing frontend start", el.LauncherName)

	el.lock.Lock()
	defer el.lock.Unlock()

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
	defer el.lock.Unlock()

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
