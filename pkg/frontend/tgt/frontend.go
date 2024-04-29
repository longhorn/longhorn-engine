package tgt

import (
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/mount-utils"

	lhexec "github.com/longhorn/go-common-libs/exec"

	"github.com/longhorn/go-iscsi-helper/longhorndev"
	"github.com/longhorn/longhorn-engine/pkg/frontend/socket"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

const (
	DevPath = "/dev/longhorn/"

	DefaultTargetID = 1
)

type Tgt struct {
	s *socket.Socket

	isUp                      bool
	dev                       longhorndev.DeviceService
	frontendName              string
	scsiTimeout               time.Duration
	iscsiAbortTimeout         time.Duration
	iscsiTargetRequestTimeout time.Duration
}

func New(frontendName string, scsiTimeout, iscsiAbortTimeout, iscsiTargetRequestTimeout time.Duration) types.Frontend {
	s := socket.New()
	return &Tgt{s, false, nil, frontendName, scsiTimeout, iscsiAbortTimeout, iscsiTargetRequestTimeout}
}

func (t *Tgt) FrontendName() string {
	return t.frontendName
}

func (t *Tgt) Init(name string, size, sectorSize int64) error {
	if err := t.s.Init(name, size, sectorSize); err != nil {
		return err
	}

	ldc := longhorndev.LonghornDeviceCreator{}
	dev, err := ldc.NewDevice(name, size, t.frontendName,
		int64(t.scsiTimeout.Seconds()),
		int64(t.iscsiAbortTimeout.Seconds()),
		int64(t.iscsiTargetRequestTimeout.Seconds()))
	if err != nil {
		return err
	}
	t.dev = dev
	if err := t.dev.InitDevice(); err != nil {
		return err
	}

	t.isUp = false

	return nil
}

func (t *Tgt) Startup(rwu types.ReaderWriterUnmapperAt) error {
	if err := t.s.Startup(rwu); err != nil {
		return err
	}

	if err := t.dev.Start(); err != nil {
		return err
	}

	t.isUp = true

	// If the engine failed during a snapshot, we may have left a frozen filesystem. This is a good opportunity to
	// attempt to unfreeze it.
	t.attemptUnfreezeFilesystem()

	return nil
}

func (t *Tgt) Shutdown() error {
	// If the engine is shutting down during a snapshot (in the preparation phase, before the snapshot operation obtains
	// a lock) we may have left a frozen filesystem. This is a good opportunity to unfreeze it.
	t.attemptUnfreezeFilesystem()

	if t.dev != nil {
		if err := t.dev.Shutdown(); err != nil {
			return err
		}
	}
	if err := t.s.Shutdown(); err != nil {
		return err
	}
	t.isUp = false

	return nil
}

func (t *Tgt) State() types.State {
	if t.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (t *Tgt) Endpoint() string {
	if t.isUp {
		return t.dev.GetEndpoint()
	}
	return ""
}

func (t *Tgt) Upgrade(name string, size, sectorSize int64, rwu types.ReaderWriterUnmapperAt) error {
	ldc := longhorndev.LonghornDeviceCreator{}
	dev, err := ldc.NewDevice(name, size, t.frontendName,
		int64(t.scsiTimeout.Seconds()),
		int64(t.iscsiAbortTimeout.Seconds()),
		int64(t.iscsiTargetRequestTimeout.Seconds()))
	if err != nil {
		return err
	}
	t.dev = dev

	if err := t.dev.PrepareUpgrade(); err != nil {
		return err
	}

	if err := t.s.Init(name, size, sectorSize); err != nil {
		return err
	}
	if err := t.s.Startup(rwu); err != nil {
		return err
	}

	if err := t.dev.FinishUpgrade(); err != nil {
		return err
	}
	t.isUp = true
	logrus.Infof("engine: Finish upgrading for %v", name)

	return nil
}

func (t *Tgt) Expand(size int64) error {
	if t.dev != nil {
		return t.dev.Expand(size)
	}
	return nil
}

// attemptUnfreezeFilesystem attempts to identify a mountPoint for the Longhorn device and unfreeze it. Under normal
// conditions, it will not find a filesystem, and if it finds a filesystem, it will not be frozen.
// attemptUnfreezeFilesystem is only relevant for volumes run with a tgt-blockdev frontend, as only these volumes have a
// Longhorn device on the node to format and mount.
func (t *Tgt) attemptUnfreezeFilesystem() {
	// We do not need to switch to the host mount namespace to get mount points here. Usually, longhorn-engine runs in a
	// container that has / bind mounted to /host with at least HostToContainer (rslave) propagation.
	// - If it does not, we likely can't do a namespace swap anyway, since we don't have access to /host/proc.
	// - If it does, we just need to know where in the container we can access the mount points to unfreeze.
	if t.frontendName == types.EngineFrontendBlockDev {
		mounter := mount.New("")
		mountPoints, err := mounter.List()
		if err != nil {
			logrus.WithError(err).Warn("Failed to list mount points while deciding whether or not unfreeze")
		}
		endpoint := t.Endpoint()
		for _, mountPoint := range mountPoints {
			if mountPoint.Device == endpoint {
				util.AttemptUnfreezeFilesystem(mountPoint.Path, lhexec.NewExecutor(), false, logrus.New())
				break
			}
		}
	}
}
