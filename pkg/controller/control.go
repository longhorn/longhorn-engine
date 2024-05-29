package controller

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"k8s.io/mount-utils"

	lhexec "github.com/longhorn/go-common-libs/exec"
	lhns "github.com/longhorn/go-common-libs/ns"
	lhutils "github.com/longhorn/go-common-libs/utils"
	"github.com/longhorn/types/pkg/generated/enginerpc"

	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type Controller struct {
	sync.RWMutex
	VolumeName                string
	size                      int64
	sectorSize                int64
	replicas                  []types.Replica
	factory                   types.BackendFactory
	backend                   *replicator
	frontend                  types.Frontend
	isUpgrade                 bool
	iscsiTargetRequestTimeout time.Duration
	engineReplicaTimeout      time.Duration
	DataServerProtocol        types.DataServerProtocol

	isExpanding             bool
	revisionCounterDisabled bool
	salvageRequested        bool

	unmapMarkSnapChainRemoved bool

	snapshotFreezeLock sync.Mutex
	snapshotMaxCount   int
	SnapshotMaxSize    int64

	GRPCAddress string
	GRPCServer  *grpc.Server

	ShutdownWG sync.WaitGroup
	lastError  error

	metricsLock   sync.RWMutex
	latestMetrics *types.Metrics
	metrics       *types.Metrics

	// lastExpansionFailedAt indicates if the error belongs to the recent expansion
	lastExpansionFailedAt string
	// lastExpansionError indicates the error message.
	// It may exist even if the expansion succeeded. For example, some of the replica expansions failed.
	lastExpansionError string

	fileSyncHTTPClientTimeout int
}

const (
	lastModifyCheckPeriod = 5 * time.Second
)

func NewController(name string, factory types.BackendFactory, frontend types.Frontend, isUpgrade, disableRevCounter, salvageRequested, unmapMarkSnapChainRemoved bool,
	iscsiTargetRequestTimeout, engineReplicaTimeout time.Duration, dataServerProtocol types.DataServerProtocol, fileSyncHTTPClientTimeout, snapshotMaxCount int, snapshotMaxSize int64) *Controller {
	c := &Controller{
		factory:       factory,
		VolumeName:    name,
		frontend:      frontend,
		metrics:       &types.Metrics{},
		latestMetrics: &types.Metrics{},

		isUpgrade:                 isUpgrade,
		revisionCounterDisabled:   disableRevCounter,
		salvageRequested:          salvageRequested,
		unmapMarkSnapChainRemoved: unmapMarkSnapChainRemoved,
		snapshotMaxCount:          snapshotMaxCount,
		SnapshotMaxSize:           snapshotMaxSize,

		iscsiTargetRequestTimeout: iscsiTargetRequestTimeout,
		engineReplicaTimeout:      engineReplicaTimeout,
		DataServerProtocol:        dataServerProtocol,

		fileSyncHTTPClientTimeout: fileSyncHTTPClientTimeout,
	}
	c.reset()
	c.metricsStart()
	return c
}

func (c *Controller) StartGRPCServer() error {
	if c.GRPCServer == nil {
		return fmt.Errorf("cannot find grpc server")
	}

	grpcPort, err := util.GetPortFromAddress(c.GRPCAddress)
	if err != nil {
		return err
	}

	if c.GRPCAddress == "" || grpcPort == 0 {
		return fmt.Errorf("cannot find grpc address or port")
	}

	c.ShutdownWG.Add(1)
	go func() {
		defer c.ShutdownWG.Done()

		grpcAddress := c.GRPCAddress
		listener, err := net.Listen("tcp", c.GRPCAddress)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to listen %v", grpcAddress)
			c.lastError = err
			return
		}

		logrus.Infof("Listening on gRPC Controller server: %v", grpcAddress)
		err = c.GRPCServer.Serve(listener)
		logrus.WithError(err).Errorf("GRPC server at %v is down", grpcAddress)
		c.lastError = err
	}()

	return nil
}

func (c *Controller) WaitForShutdown() error {
	c.ShutdownWG.Wait()
	return c.lastError
}

func (c *Controller) AddReplica(address string, snapshotRequired bool, mode types.Mode) error {
	return c.addReplica(address, snapshotRequired, mode)
}

func (c *Controller) hasWOReplica() bool {
	for _, i := range c.replicas {
		if i.Mode == types.WO {
			return true
		}
	}
	return false
}

func (c *Controller) canAdd(address string) (bool, error) {
	if c.hasReplica(address) {
		return false, nil
	}
	if c.hasWOReplica() {
		return false, fmt.Errorf("can only have one WO replica at a time")
	}
	if c.isExpanding {
		return false, fmt.Errorf("cannot add WO replica during expansion")
	}
	return true, nil
}

func (c *Controller) addReplica(address string, snapshotRequired bool, mode types.Mode) error {
	c.Lock()
	defer c.Unlock()
	if ok, err := c.canAdd(address); !ok {
		return err
	}

	newBackend, err := c.factory.Create(c.VolumeName, address, c.DataServerProtocol, c.engineReplicaTimeout)
	if err != nil {
		return err
	}

	replicaSize, err := newBackend.Size()
	if err != nil {
		return errors.Wrap(err, "failed to get the size before adding a replica")
	}
	if c.size == 0 && len(c.replicas) == 0 {
		c.size = replicaSize
	}

	return c.addReplicaNoLock(newBackend, address, snapshotRequired, mode)
}

// If shouldFreeze, Snapshot attempts to freeze the mounted filesystem on the volume's root partition. If it fails to
// do so, Snapshot attempts to clean up and returns an error. If not shouldFreeze or if a mounted filesystem is not
// detected on the volume's root partition, Snapshot does a best effort sync and takes a snapshot.
func (c *Controller) Snapshot(inputName string, labels map[string]string, shouldFreeze bool) (name string, err error) {
	name = inputName
	if name == "" {
		name = lhutils.UUID()
	}

	log := logrus.WithFields(logrus.Fields{"volume": c.VolumeName, "snapshot": name})
	log.Info("Starting snapshot")

	defer func() {
		if err != nil {
			// The gRPC server returns, but does not log, errors.
			log.WithError(err).Errorf("Failed to snapshot")
		}
	}()

	var endpoint string
	c.RLock()
	// Check now to avoid freezing or syncing unnecessarily. Also check again later as originally designed.
	err = c.canDoSnapshot()
	if c.frontend.FrontendName() == types.EngineFrontendBlockDev {
		// It is meaningless to try to freeze filesystems for a tgt-iscsi endpoint.
		endpoint = c.Endpoint()
	}
	c.RUnlock()
	if err != nil {
		return "", err
	}

	var mounted, frozen bool
	freezePoint := util.GetFreezePointFromDevicePath(endpoint)
	mounter := mount.New("")
	exec := lhexec.NewExecutor()
	defer func() {
		if frozen {
			log.Infof("Unfreezing filesystem mounted at %v", freezePoint)
			if _, err := util.UnfreezeFilesystem(freezePoint, exec); err != nil {
				log.WithError(err).Warnf("Failed to unfreeze filesystem mounted at %v", freezePoint)
			}
		}
		if mounted {
			log.Debugf("Unmounting filesystem mounted at %v", freezePoint)
			if err := mount.CleanupMountPoint(freezePoint, mounter, false); err != nil {
				log.WithError(err).Warnf("Failed to unmount filesystem mounted at %v", freezePoint)
			}
		}
	}()

	// We create a bind mount of one of the discovered mount points (it does not matter which one) inside the container
	// mount namespace (assuming longhorn-engine is running in a container). The mount point cannot be unmounted except
	// by us, so we can be confident it is safe to freeze. This approach is preferable to locking some source mount
	// point with an open file descriptor because it cannot be circumvented by a lazy unmount at the source.
	if shouldFreeze && endpoint != "" {
		if !c.snapshotFreezeLock.TryLock() {
			// Two simultaneous freeze attempts could cause weird behaviors (e.g. the second operation could bind mount
			// on top of the bind mount created by the first). There is no particular reason we should support multiple
			// snapshots in quick succession, so just return.
			return "", errors.New("filesystem is already being frozen for a different snapshot")
		}
		defer c.snapshotFreezeLock.Unlock()
		mounted, frozen, err = tryFreeze(endpoint, freezePoint, mounter, exec, log)
		if err != nil {
			return "", err
		}
		if !frozen {
			log.Debug("Did not detect mounted filesystem while snapshotting; continuing without freeze")
		}
	}
	if !frozen {
		// Revert to the previous/default behavior of syncing before taking a snapshot.
		log.Info("Requesting system sync before snapshot")
		if err := lhns.Sync(); err != nil {
			// Sync should never fail, so it is likely due to the nsenter. To maintain existing behavior, we do not
			// refuse to take a snapshot if sync fails.
			log.WithError(err).Errorf("WARNING: failed to sync before snapshot; continuing without freeze or sync")
		}
	}

	c.Lock()
	defer c.Unlock()
	if err = c.canDoSnapshot(); err != nil {
		return "", err
	}

	created := util.Now()
	if err = c.handleErrorNoLock(c.backend.Snapshot(name, true, created, labels)); err != nil {
		return "", err
	}
	log.Info("Finished snapshot")
	return name, nil
}

func (c *Controller) canDoSnapshot() error {
	countUsage, sizeUsage, err := c.backend.GetSnapshotCountAndSizeUsage()
	if err != nil {
		return err
	}
	if countUsage >= c.snapshotMaxCount {
		return fmt.Errorf("snapshot count usage %d is equal or larger than snapshotMaxCount %d", countUsage, c.snapshotMaxCount)
	}
	// if SnapshotMaxSize is 0, it means no limit
	if c.SnapshotMaxSize == 0 {
		return nil
	}

	headFileSize, err := c.backend.GetHeadFileSize()
	if err != nil {
		return err
	}
	remainSize := c.SnapshotMaxSize - sizeUsage
	if headFileSize > remainSize {
		return fmt.Errorf("snapshot free space %d is not enough for head file %d", remainSize, headFileSize)
	}
	return nil
}

func (c *Controller) Expand(size int64) error {
	if err := c.startExpansion(size); err != nil {
		logrus.WithError(err).Error("Controller failed to start expansion")
		return err
	}

	go func(size int64) {
		expanded := false
		defer func() {
			// Frontend expansion involves in the iSCSI session rescanning, which will wait for the in-fly io requests complete.
			// Hence there will be a deadlock once we use the lock to protect this frontend expansion.
			if c.frontend != nil && expanded {
				if err := c.frontend.Expand(size); err != nil {
					logrus.WithError(err).Error("Failed to expand the frontend")
					expanded = false
				}
			}
			c.finishExpansion(expanded, size)
		}()

		// We perform a system level sync without the lock. Cannot block read/write
		// Can be improved to only sync the filesystem on the block device later
		if err := lhns.Sync(); err != nil {
			// sync should never fail though, so it more like due to the nsenter
			logrus.WithError(err).Errorf("WARNING: continue to expand to size %v for %v, but sync failed", size, c.VolumeName)
		}

		// Should block R/W during the expansion.
		c.Lock()
		defer c.Unlock()
		if err := c.canDoSnapshot(); err != nil {
			logrus.WithError(err).Error("Cannot get remain snapshot count before expansion")
			return
		}

		expansionSuccess, errsNeedToBeHandled, errsForRecording := c.backend.Expand(size)
		if errsForRecording != nil {
			c.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)
			if expansionSuccess {
				c.lastExpansionError = fmt.Sprintf("the expansion succeeded, but some replica expansion failed: %v", errsForRecording)
			} else {
				c.lastExpansionError = fmt.Sprintf("the expansion failed since all replica expansion failed: %v", errsForRecording)
			}
			if err := c.handleErrorNoLock(errsNeedToBeHandled); err != nil {
				logrus.WithError(err).Error("Failed to handle the backend expansion errors")
			}
			// If there is expansion failure, controller cannot continue expanding the frontend
			if !expansionSuccess {
				return
			}
		}

		expanded = true
	}(size)

	return nil
}

func (c *Controller) startExpansion(size int64) (err error) {
	c.Lock()
	defer c.Unlock()

	if c.isExpanding {
		return fmt.Errorf("controller expansion is in progress")
	}

	defer func() {
		if c.isExpanding {
			c.lastExpansionFailedAt = ""
			c.lastExpansionError = ""
		} else if err != nil {
			c.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339Nano)
			c.lastExpansionError = errors.Wrap(err, "controller failed to start expansion").Error()
		}
	}()

	if size%diskutil.VolumeSectorSize != 0 {
		return fmt.Errorf("requested expansion size %v not multiple of volume sector size %v", size, diskutil.VolumeSectorSize)
	}
	if c.size == size {
		logrus.Infof("controller %v is already expanded to size %v", c.VolumeName, size)
		return nil
	}
	if c.size > size {
		return fmt.Errorf("controller cannot be expanded to a smaller size %v", size)
	}

	c.isExpanding = true

	return nil
}

func (c *Controller) finishExpansion(expanded bool, size int64) {
	c.Lock()
	defer c.Unlock()

	if expanded {
		if c.lastExpansionError != "" {
			logrus.Infof("Controller succeeded to expand from size %v to %v but there are some replica expansion failures: %v", c.size, size, c.lastExpansionError)
		} else {
			logrus.Infof("Controller succeeded to expand from size %v to %v", c.size, size)
		}
		c.size = size
	} else {
		logrus.Infof("Controller failed to expand from size %v to %v", c.size, size)
	}
	c.isExpanding = false
}

func (c *Controller) IsExpanding() bool {
	c.RLock()
	defer c.RUnlock()
	return c.isExpanding
}

func (c *Controller) GetExpansionErrorInfo() (string, string) {
	c.RLock()
	defer c.RUnlock()
	return c.lastExpansionError, c.lastExpansionFailedAt
}

func (c *Controller) addReplicaNoLock(newBackend types.Backend, address string, snapshot bool, mode types.Mode) (err error) {
	defer func() {
		if err != nil && newBackend != nil {
			newBackend.Close()
		}
	}()

	if ok, err := c.canAdd(address); !ok {
		return err
	}

	if snapshot && mode != types.ERR {
		uuid := lhutils.UUID()
		created := util.Now()

		// if there is no replica, we don't need to check whether remaining replica can do snapshot
		if len(c.backend.backends) != 0 {
			if err := c.canDoSnapshot(); err != nil {
				return err
			}
		}

		if err := c.backend.Snapshot(uuid, false, created, nil); err != nil {
			return err
		}
		if err := newBackend.Snapshot(uuid, false, created, nil); err != nil {
			return err
		}
	}

	c.replicas = append(c.replicas, types.Replica{
		Address: address,
		Mode:    mode,
	})

	c.backend.AddBackend(address, newBackend, mode)

	if mode != types.ERR {
		go c.monitoring(address, newBackend)
	}

	return nil
}

func (c *Controller) hasReplica(address string) bool {
	for _, i := range c.replicas {
		if i.Address == address {
			return true
		}
	}
	return false
}

func (c *Controller) RemoveReplica(address string) error {
	c.Lock()
	defer c.Unlock()

	if !c.hasReplica(address) {
		return nil
	}

	for i, r := range c.replicas {
		if r.Address == address {
			if len(c.replicas) == 1 && c.frontend != nil && c.frontend.State() == types.StateUp {
				return fmt.Errorf("cannot remove last replica if volume is up")
			}
			c.replicas = append(c.replicas[:i], c.replicas[i+1:]...)
			c.backend.RemoveBackend(r.Address)
		}
	}

	return nil
}

func (c *Controller) ListReplicas() []types.Replica {
	return c.replicas
}

func (c *Controller) SetReplicaMode(address string, mode types.Mode) error {
	switch mode {
	case types.ERR:
		c.Lock()
		defer c.Unlock()
	case types.RW:
		c.RLock()
		defer c.RUnlock()
	default:
		return fmt.Errorf("cannot set to mode %s", mode)
	}

	c.setReplicaModeNoLock(address, mode)
	return nil
}

func (c *Controller) setReplicaModeNoLock(address string, mode types.Mode) {
	for i, r := range c.replicas {
		if r.Address == address {
			if r.Mode != types.ERR {
				logrus.Infof("Setting replica %v to mode %v", address, mode)
				r.Mode = mode
				c.replicas[i] = r
				c.backend.SetMode(address, mode)
			} else {
				logrus.Infof("Ignore set replica %v to mode %v due to it's ERR", address, mode)
			}
		}
	}
}

func (c *Controller) startFrontend() error {
	if len(c.replicas) > 0 && c.frontend != nil {
		if c.isUpgrade {
			logrus.Info("Upgrading frontend")
			if err := c.frontend.Upgrade(c.VolumeName, c.size, c.sectorSize, c); err != nil {
				logrus.WithError(err).Error("Failed to upgrade frontend")
				return errors.Wrap(err, "failed to upgrade frontend")
			}
			return nil
		}
		if err := c.frontend.Init(c.VolumeName, c.size, c.sectorSize); err != nil {
			logrus.WithError(err).Error("Failed to init frontend")
			return errors.Wrap(err, "failed to init frontend")
		}
		if err := c.frontend.Startup(c); err != nil {
			logrus.WithError(err).Error("Failed to startup frontend")
			return errors.Wrap(err, "failed to start up frontend")
		}
	}
	return nil
}

func (c *Controller) StartFrontend(frontend string) error {
	c.Lock()
	defer c.Unlock()

	if c.isExpanding {
		return fmt.Errorf("cannot start frontend during the engine expansion")
	}
	if frontend == "" {
		return fmt.Errorf("cannot start empty frontend")
	}
	if c.frontend != nil {
		if c.frontend.FrontendName() != frontend && c.frontend.State() != types.StateDown {
			return fmt.Errorf("frontend %v is already started, cannot be set as %v",
				c.frontend.FrontendName(), frontend)
		}
	}

	f, err := NewFrontend(frontend, c.iscsiTargetRequestTimeout)
	if err != nil {
		return errors.Wrapf(err, "failed to find frontend: %s", frontend)
	}
	c.frontend = f
	return c.startFrontend()
}

// Check if all replica revision counter setting match with engine
// controller, and mark unmatch replica to ERR.
func (c *Controller) checkReplicaRevCounterSettingMatch() error {
	for _, r := range c.replicas {
		if r.Mode == types.ERR {
			continue
		}
		revCounterDisabled, err := c.backend.backends[r.Address].backend.IsRevisionCounterDisabled()
		if err != nil {
			return err
		}

		if c.revisionCounterDisabled != revCounterDisabled {
			logrus.Errorf("Revision Counter Disabled setting mismatch at engine %v, replica %v: %v, mark this replica as ERR.",
				c.revisionCounterDisabled, r.Address, revCounterDisabled)
			c.setReplicaModeNoLock(r.Address, types.ERR)
		}
	}

	return nil
}

// salvageRevisionCounterDisabledReplicas will find the best replica
// for salvage recovering, based on lastModifyTime and HeadFileSize.
func (c *Controller) salvageRevisionCounterDisabledReplicas() error {
	var replicaCandidates []*types.ReplicaSalvageInfo
	var lastModifyTime time.Time
	for _, r := range c.replicas {
		if r.Mode == types.ERR {
			continue
		}
		repLastModifyTimeInt, err := c.backend.backends[r.Address].backend.GetLastModifyTime()
		if err != nil {
			return err
		}
		repLastModifyTime := time.Unix(0, repLastModifyTimeInt)

		repHeadFileSize, err := c.backend.backends[r.Address].backend.GetHeadFileSize()
		if err != nil {
			return err
		}

		replicaCandidates = append(replicaCandidates, &types.ReplicaSalvageInfo{
			Address:        r.Address,
			LastModifyTime: repLastModifyTime,
			HeadFileSize:   repHeadFileSize,
		})
		if lastModifyTime.IsZero() || repLastModifyTime.After(lastModifyTime) {
			lastModifyTime = repLastModifyTime
		}
	}

	if len(replicaCandidates) == 0 {
		return fmt.Errorf("cannot find any replica for salvage")
	}
	var bestCandidate *types.ReplicaSalvageInfo
	for _, salvageReplica := range replicaCandidates {
		if salvageReplica.LastModifyTime.Add(lastModifyCheckPeriod).After(lastModifyTime) {
			if bestCandidate == nil ||
				salvageReplica.HeadFileSize > bestCandidate.HeadFileSize ||
				(salvageReplica.HeadFileSize == bestCandidate.HeadFileSize &&
					salvageReplica.LastModifyTime.After(bestCandidate.LastModifyTime)) {
				bestCandidate = salvageReplica
			}
		}
	}

	if bestCandidate == nil {
		return fmt.Errorf("BUG: Should find one candidate for salvage")
	}

	// Only leave bestCandidate replica as good, mark others as ERR.
	for _, r := range c.replicas {
		if r.Address != bestCandidate.Address {
			logrus.Infof("salvageRequested set and mark %v as ERR", r.Address)
			c.setReplicaModeNoLock(r.Address, types.ERR)
		} else {
			logrus.Infof("salvageRequested set and mark %v as RW", r.Address)
			c.setReplicaModeNoLock(r.Address, types.RW)
		}
	}
	return nil
}

// checkReplicasRevisionCounter will check if any replica has unmatched
// revision counter, and mark unmatched replica as 'ERR' state.
func (c *Controller) checkReplicasRevisionCounter() error {
	var expectedRevision int64

	revisionCounters := make(map[string]int64)
	for _, r := range c.replicas {
		// The related backend is nil if the mode is ERR
		if r.Mode == types.ERR {
			continue
		}
		counter, err := c.backend.GetRevisionCounter(r.Address)
		if err != nil {
			return err
		}
		if counter > expectedRevision {
			expectedRevision = counter
		}
		revisionCounters[r.Address] = counter
	}

	for address, counter := range revisionCounters {
		if counter != expectedRevision {
			logrus.Errorf("Revision conflict detected! Expect %v, got %v in replica %v. Mark as ERR",
				expectedRevision, counter, address)
			c.setReplicaModeNoLock(address, types.ERR)
		}
	}

	return nil
}

func (c *Controller) SetUnmapMarkSnapChainRemoved(enabled bool) error {
	c.Lock()
	defer c.Unlock()

	c.unmapMarkSnapChainRemoved = enabled
	return c.checkUnmapMarkSnapChainRemoved()
}

func (c *Controller) GetUnmapMarkSnapChainRemoved() bool {
	c.RLock()
	defer c.RUnlock()
	return c.unmapMarkSnapChainRemoved
}

// checkUnmapMarkSnapChainRemoved will check and correct any replica has
// unmatched flag `UnmapMarkSnapChainRemoved`
func (c *Controller) checkUnmapMarkSnapChainRemoved() error {
	allFailed := true

	expected := c.unmapMarkSnapChainRemoved
	for _, r := range c.replicas {
		// The related backend is nil if the mode is ERR
		if r.Mode == types.ERR {
			continue
		}
		enabled, err := c.backend.GetUnmapMarkSnapChainRemoved(r.Address)
		if err != nil {
			return err
		}
		if enabled != expected {
			err := c.backend.SetUnmapMarkSnapChainRemoved(r.Address, expected)
			if err != nil {
				logrus.Errorf("Failed to correct Unmatched flag UnmapMarkSnapChainRemoved! Expect %v, got %v in replica %v. Mark as ERR",
					expected, enabled, r.Address)
				c.setReplicaModeNoLock(r.Address, types.ERR)
			} else {
				allFailed = false
			}
		} else {
			allFailed = false
		}
	}

	if allFailed {
		return fmt.Errorf("failed to correct Unmatched flag UnmapMarkSnapChainRemoved for all replicas, expect %v", expected)
	}

	logrus.Infof("Controller checked and corrected flag unmapMarkSnapChainRemoved=%v for backend replicas", c.unmapMarkSnapChainRemoved)

	return nil
}

func (c *Controller) SetSnapshotMaxCount(count int) error {
	c.Lock()
	defer c.Unlock()

	countUsage, _, err := c.backend.GetSnapshotCountAndSizeUsage()
	if err != nil {
		return err
	}

	if count < countUsage {
		return fmt.Errorf("cannot set snapshotMaxCount=%d smaller than current snapshot count usage=%d, please purge snapshots first", count, countUsage)
	}

	c.snapshotMaxCount = count

	for _, r := range c.replicas {
		// The related backend is nil if the mode is ERR
		if r.Mode == types.ERR {
			continue
		}
		err := c.backend.SetSnapshotMaxCount(r.Address, count)
		if err != nil {
			logrus.Errorf("failed to set flag SnapshotMaxCount to %d in replica %s, err: %v", count, r.Address, err)
			return fmt.Errorf("failed to set flag SnapshotMaxCount to %d in replica %s, err: %v", count, r.Address, err)
		}
	}

	logrus.Infof("Controller set flag snapshotMaxCount=%d for backend replicas", count)

	return nil
}

func (c *Controller) GetSnapshotMaxCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.snapshotMaxCount
}

func (c *Controller) SetSnapshotMaxSize(size int64) error {
	c.Lock()
	defer c.Unlock()

	_, sizeUsage, err := c.backend.GetSnapshotCountAndSizeUsage()
	if err != nil {
		return err
	}

	if size < sizeUsage {
		return fmt.Errorf("cannot set snapshotMaxSize=%d smaller than current snapshot size usage=%d, please purge snapshots first", size, sizeUsage)
	}

	c.SnapshotMaxSize = size

	for _, r := range c.replicas {
		// The related backend is nil if the mode is ERR
		if r.Mode == types.ERR {
			continue
		}
		err := c.backend.SetSnapshotMaxSize(r.Address, size)
		if err != nil {
			logrus.Errorf("Failed to set flag SnapshotMaxSize to %d in replica %s, err: %v", size, r.Address, err)
			return fmt.Errorf("Failed to set flag SnapshotMaxSize to %d in replica %s, err: %v", size, r.Address, err)
		}
	}

	logrus.Infof("Controller set flag SnapshotMaxSize=%d for backend replicas", size)

	return nil
}

func (c *Controller) GetSnapshotMaxSize() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.SnapshotMaxSize
}

func isReplicaInInvalidState(state string) bool {
	return state != string(types.ReplicaStateOpen) && state != string(types.ReplicaStateDirty)
}

func checkDuplicateAddress(addresses ...string) error {
	checkDuplicate := map[string]struct{}{}

	for _, address := range addresses {
		if _, exist := checkDuplicate[address]; exist {
			return fmt.Errorf("invalid ReplicaAddress: duplicate replica addresses %s", address)
		}
		checkDuplicate[address] = struct{}{}
	}

	return nil
}

func determineCorrectVolumeSize(volumeSize, volumeCurrentSize int64, backendSizes map[int64]struct{}) int64 {
	if volumeCurrentSize == 0 {
		return volumeSize
	}

	if len(backendSizes) == 1 {
		backendSize := int64(0)
		for size := range backendSizes {
			backendSize = size
		}
		if backendSize == volumeSize {
			return volumeSize
		}
	}

	return volumeCurrentSize
}

func isBackendServiceUnavailable(errorCodes map[string]codes.Code) bool {
	for _, code := range errorCodes {
		if code == codes.Unavailable {
			return true
		}
	}
	return false
}

func (c *Controller) Start(volumeSize, volumeCurrentSize int64, addresses ...string) error {
	c.Lock()
	defer c.Unlock()

	if len(addresses) == 0 {
		return nil
	}

	if err := checkDuplicateAddress(addresses...); err != nil {
		return err
	}

	if len(c.replicas) > 0 {
		return nil
	}

	c.reset()

	availableBackends := map[string]types.Backend{}
	backendSizes := map[int64]struct{}{}
	errorCodes := map[string]codes.Code{}
	first := true
	for _, address := range addresses {
		newBackend, err := c.factory.Create(c.VolumeName, address, c.DataServerProtocol, c.engineReplicaTimeout)
		if err != nil {
			if strings.Contains(err.Error(), "rpc error: code = Unavailable") {
				errorCodes[address] = codes.Unavailable
			}
			logrus.WithError(err).Warnf("Failed to create backend with address %v", address)
			continue
		}

		// If the instance manager crashes during the execution of [this code block](https://github.com/longhorn/longhorn-engine/blob/v1.5.1/pkg/sync/sync.go#L435-L446)
		// the volume.meta file will be left with `Rebuilding` set to true. If Longhorn subsequently updates the replica
		// as healthy, then the old replica will be removed. In scenarios involving multiple replicas, Longhorn will
		// remove the replica with illegal values, thereby allowing rebuilding from other healthy replicas. However, in
		// the case of single replicas, we cannot employ the same strategy.
		// As a result, we will make a best-effort attempt to reset the `Rebuilding` flag for single replica cases.
		// Ref: https://github.com/longhorn/longhorn/issues/6626
		if len(addresses) == 1 {
			err = newBackend.ResetRebuild()
			if err != nil {
				logrus.WithError(err).Warnf("Failed to reset invalid rebuild for backend with address %v", address)
			}
		}

		newSize, err := newBackend.Size()
		if err != nil {
			if strings.Contains(err.Error(), "rpc error: code = Unavailable") {
				errorCodes[address] = codes.Unavailable
			}
			logrus.WithError(err).Warnf("Failed to get the size from the backend address %v", address)
			continue
		}

		newSectorSize, err := newBackend.SectorSize()
		if err != nil {
			if strings.Contains(err.Error(), "rpc error: code = Unavailable") {
				errorCodes[address] = codes.Unavailable
			}
			logrus.WithError(err).Warnf("Failed to get the sector size from the backend address %v", address)
			continue
		}

		state, err := newBackend.GetState()
		if err != nil {
			if strings.Contains(err.Error(), "rpc error: code = Unavailable") {
				errorCodes[address] = codes.Unavailable
			}
			logrus.WithError(err).Warnf("Failed to get the state from the backend address %v", address)
			continue
		}
		if isReplicaInInvalidState(state) {
			logrus.Warnf("Backend %v is in the invalid state %v", address, state)
			continue
		}

		if first {
			first = false
			c.sectorSize = newSectorSize
		}

		if c.sectorSize != newSectorSize {
			logrus.Warnf("Backend %v sector size does not match %d != %d in the engine initiation phase", address, c.sectorSize, newSectorSize)
			continue
		}

		availableBackends[address] = newBackend
		backendSizes[newSize] = struct{}{}
	}

	c.size = determineCorrectVolumeSize(volumeSize, volumeCurrentSize, backendSizes)

	for address, backend := range availableBackends {
		size, err := backend.Size()
		if err != nil {
			logrus.WithError(err).Warnf("Failed to get the size from the backend address %v", address)
			delete(availableBackends, address)
			continue
		}

		if c.size != size {
			logrus.Warnf("Backend %v size does not match %d != %d in the engine initiation phase", address, c.size, size)
			delete(availableBackends, address)
		}
	}

	for _, address := range addresses {
		if newBackend, exists := availableBackends[address]; exists {
			// We will validate this later
			if err := c.addReplicaNoLock(newBackend, address, false, types.RW); err != nil {
				return err
			}
		} else {
			if err := c.addReplicaNoLock(nil, address, false, types.ERR); err != nil {
				return err
			}
		}
	}

	if len(availableBackends) == 0 {
		if isBackendServiceUnavailable(errorCodes) {
			return fmt.Errorf(ControllerErrorNoBackendServiceUnavailable+" from the addresses %+v", addresses)
		}
		return fmt.Errorf(ControllerErrorNoBackendReplicaError+" from the addresses %+v", addresses)
	}

	if err := c.checkUnmapMarkSnapChainRemoved(); err != nil {
		return err
	}

	// If the live upgrade is in-progress, the revision counters among replicas can be temporarily
	// out of sync. They will be refreshed after the first write command.
	// For more details, see the following url:
	// https://github.com/longhorn/longhorn/issues/1235
	if !c.isUpgrade {
		if err := c.checkReplicaRevCounterSettingMatch(); err != nil {
			return err
		}

		if c.revisionCounterDisabled {
			if c.salvageRequested {
				if err := c.salvageRevisionCounterDisabledReplicas(); err != nil {
					return err
				}
			}
		} else {
			// For revision counter enabled case, no matter salvageRequested
			// always check the revision counter.
			if err := c.checkReplicasRevisionCounter(); err != nil {
				return err
			}
		}
	}

	return c.startFrontend()
}

func (c *Controller) WriteAt(b []byte, off int64) (int, error) {
	c.RLock()
	l := len(b)
	if off < 0 || off+int64(l) > c.size {
		err := fmt.Errorf("EOF: Write of %v bytes at offset %v is beyond volume size %v", l, off, c.size)
		c.RUnlock()
		return 0, err
	}
	startTime := time.Now()
	var n int
	var err error
	if c.hasWOReplica() {
		n, err = c.writeInWOMode(b, off)
	} else {
		n, err = c.writeInNormalMode(b, off)
	}
	c.RUnlock()
	if err != nil {
		return n, c.handleError(err)
	}
	c.recordMetrics(false, l, time.Since(startTime))
	return n, err
}

func (c *Controller) writeInWOMode(b []byte, off int64) (int, error) {
	bufLen := len(b)
	// buffer b is defaultSectorSize aligned
	if (bufLen == 0) || ((off%diskutil.VolumeSectorSize == 0) && (bufLen%diskutil.VolumeSectorSize == 0)) {
		return c.backend.WriteAt(b, off)
	}

	readOffsetStart := (off / diskutil.VolumeSectorSize) * diskutil.VolumeSectorSize
	var readOffsetEnd int64
	if ((off + int64(bufLen)) % diskutil.VolumeSectorSize) == 0 {
		readOffsetEnd = off + int64(bufLen)
	} else {
		readOffsetEnd = (((off + int64(bufLen)) / diskutil.VolumeSectorSize) + 1) * diskutil.VolumeSectorSize
	}
	readBuf := make([]byte, readOffsetEnd-readOffsetStart)
	if _, err := c.backend.ReadAt(readBuf, readOffsetStart); err != nil {
		return 0, errors.Wrap(err, "failed to retrieve aligned sectors from RW replicas")
	}

	startCut := int(off % diskutil.VolumeSectorSize)
	copy(readBuf[startCut:startCut+bufLen], b)

	if n, err := c.backend.WriteAt(readBuf, readOffsetStart); err != nil {
		if n < startCut {
			return 0, err
		}
		if n-startCut < bufLen {
			return n - startCut, err
		}
		return bufLen, err
	}
	return bufLen, nil
}

func (c *Controller) writeInNormalMode(b []byte, off int64) (int, error) {
	return c.backend.WriteAt(b, off)
}

func (c *Controller) ReadAt(b []byte, off int64) (int, error) {
	c.RLock()
	l := len(b)
	if off < 0 || off+int64(l) > c.size {
		err := fmt.Errorf("EOF: Read of %v bytes at offset %v is beyond volume size %v", l, off, c.size)
		c.RUnlock()
		return 0, err
	}
	startTime := time.Now()
	n, err := c.backend.ReadAt(b, off)
	c.RUnlock()
	if err != nil {
		return n, c.handleError(err)
	}
	c.recordMetrics(true, l, time.Since(startTime))
	return n, err
}

func (c *Controller) UnmapAt(length uint32, off int64) (int, error) {
	// TODO: Need to fail unmap requests
	//  if the volume is purging snapshots or creating backups.
	c.RLock()

	if off < 0 || off+int64(length) > c.size {
		// This is a legitimate error (which is handled by tgt/liblonghorn at a higher level). Since tgt/liblonghorn
		// does not actually print the error, print it here.
		err := fmt.Errorf("EOF: unmap of %v bytes at offset %v is beyond volume size %v", length, off, c.size)
		logrus.WithError(err).Error("Failed to unmap")
		c.RUnlock()
		return 0, err
	}
	if c.hasWOReplica() {
		// Cannot unmap during rebuilding. See https://github.com/longhorn/longhorn/issues/7103 for context. It is legal
		// to not complete the unmap operation, so simply respond that 0 blocks were unmapped. Otherwise, VM workloads
		// will remain paused for the entire duration of the rebuild.
		err := fmt.Errorf("cannot unmap %v bytes at offset %v while rebuilding is in progress", length, off)
		logrus.WithError(err).Warn("Failed to unmap")
		c.RUnlock()
		return 0, nil
	}
	if c.isExpanding {
		// Cannot unmap during expansion. We could return no error (as we do in the rebuilding case), but expansion
		// should be fast. It is better to return the error, letting the filesystem know that blocks have not been
		// trimmed, so it can try again.
		err := fmt.Errorf("cannot unmap %v bytes at offset %v while expansion in is progress", length, off)
		logrus.WithError(err).Error("Failed to unmap")
		c.RUnlock()
		return 0, err
	}

	// startTime := time.Now()
	n, err := c.backend.UnmapAt(length, off)
	c.RUnlock()
	if err != nil {
		return n, c.handleError(err)
	}

	// TODO: Add operation unmap into the metrics
	// c.recordMetrics(false, length, time.Since(startTime))

	return n, nil
}

func isSnapshotDiskExist(err error) bool {
	match, _ := regexp.MatchString("snapshot (.*) is already existing", err.Error())
	return match
}

func (c *Controller) handleErrorNoLock(err error) error {
	if bErr, ok := err.(*BackendError); ok {
		snapshotExistList := make(map[string]struct{})

		if len(bErr.Errors) > 0 {
			for address, replicaErr := range bErr.Errors {
				if isSnapshotDiskExist(replicaErr) {
					// The snapshot request using a existing snapshot's name might be caused by
					// users and callers unexpectedly.
					// We reject the request, so do not set the replica to ERR if the snapshot is already existing.
					snapshotExistList[address] = struct{}{}
				} else {
					logrus.WithError(err).Errorf("Setting replica %s to ERR", address)
					c.setReplicaModeNoLock(address, types.ERR)
				}
			}

			// Always return error if the snapshot is already existing.
			if len(snapshotExistList) == 0 {
				// if we still have a good replica, do not return error
				for _, r := range c.replicas {
					if r.Mode == types.RW {
						logrus.WithError(err).Errorf("Ignoring error because %s is mode RW", r.Address)
						err = nil
						break
					}
				}
			}
		}
	}
	if err != nil {
		logrus.WithError(err).Errorf("I/O error")
	}
	return err
}

func (c *Controller) handleError(err error) error {
	c.Lock()
	defer c.Unlock()
	return c.handleErrorNoLock(err)
}

func (c *Controller) reset() {
	c.replicas = []types.Replica{}
	c.backend = &replicator{}
}

func (c *Controller) Close() error {
	return c.Shutdown()
}

func (c *Controller) shutdownFrontend() error {
	// Make sure writing data won't be blocked
	c.RLock()
	defer c.RUnlock()

	if c.frontend != nil {
		return c.frontend.Shutdown()
	}
	return nil
}

func (c *Controller) ShutdownFrontend() error {
	if err := c.shutdownFrontend(); err != nil {
		return err
	}
	return nil
}

func (c *Controller) shutdownBackend() error {
	c.Lock()
	defer c.Unlock()

	err := c.backend.Close()
	c.reset()

	return err
}

func (c *Controller) Shutdown() error {
	/*
		Need to shutdown frontend first because it will write
		the final piece of data to backend
	*/
	errFrontend := c.shutdownFrontend()
	if errFrontend != nil {
		logrus.WithError(errFrontend).Error("Error when shutting down frontend")
	}
	errBackend := c.shutdownBackend()
	if errBackend != nil {
		logrus.WithError(errBackend).Error("Error when shutting down backend")
	}
	if errFrontend != nil || errBackend != nil {
		return errors.Wrapf(errBackend, "errors when shutting down controller: frontend: %v backend", errFrontend)
	}

	return nil
}

func (c *Controller) Size() int64 {
	return c.size
}

func (c *Controller) monitoring(address string, backend types.Backend) {
	monitorChan := backend.GetMonitorChannel()

	if monitorChan == nil {
		return
	}

	logrus.Infof("Start monitoring %v", address)
	err := <-monitorChan
	if err != nil {
		logrus.WithError(err).Errorf("Backend %v monitoring failed, mark as ERR", address)
		if err = c.SetReplicaMode(address, types.ERR); err != nil {
			logrus.WithError(err).Warnf("Failed to set replica %v to ERR", address)
		}
	}
	logrus.Infof("Monitoring stopped %v", address)
}

func (c *Controller) Endpoint() string {
	if c.frontend != nil {
		return c.frontend.Endpoint()
	}
	return ""
}

func (c *Controller) Frontend() string {
	if c.frontend != nil {
		return c.frontend.FrontendName()
	}
	return ""
}

func (c *Controller) FrontendState() string {
	if c.frontend != nil {
		return string(c.frontend.State())
	}
	return ""
}

func (c *Controller) recordMetrics(isRead bool, dataLength int, latency time.Duration) {
	c.metricsLock.Lock()
	defer c.metricsLock.Unlock()

	if isRead {
		c.metrics.Throughput.Read += uint64(dataLength)
		c.metrics.TotalLatency.Read += uint64(latency.Nanoseconds())
		c.metrics.IOPS.Read++
	} else {
		c.metrics.Throughput.Write += uint64(dataLength)
		c.metrics.TotalLatency.Write += uint64(latency.Nanoseconds())
		c.metrics.IOPS.Write++
	}
}

func (c *Controller) metricsStart() {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			c.metricsLock.Lock()
			c.latestMetrics = c.metrics
			c.metrics = &types.Metrics{}
			c.metricsLock.Unlock()
		}
	}()
}

func (c *Controller) GetLatestMetics() *enginerpc.Metrics {
	c.metricsLock.RLock()
	defer c.metricsLock.RUnlock()

	metrics := &enginerpc.Metrics{}

	metrics.ReadThroughput = c.latestMetrics.Throughput.Read
	metrics.WriteThroughput = c.latestMetrics.Throughput.Write
	metrics.ReadIOPS = c.latestMetrics.IOPS.Read
	metrics.WriteIOPS = c.latestMetrics.IOPS.Write

	if c.latestMetrics.IOPS.Read != 0 {
		metrics.ReadLatency = getAverageLatency(c.latestMetrics.TotalLatency.Read, c.latestMetrics.IOPS.Read)
	}
	if c.latestMetrics.IOPS.Write != 0 {
		metrics.WriteLatency = getAverageLatency(c.latestMetrics.TotalLatency.Write, c.latestMetrics.IOPS.Write)
	}

	return metrics
}

func getAverageLatency(totalLatency, iops uint64) uint64 {
	return totalLatency / iops
}

// tryFreeze attempts to bind mount an existing mount point to freezePoint, then freeze the filesystem from
// freezePoint. tryFreeze returns booleans mounted and frozen so the caller can attempt to clean up depending on its
// progress. tryFreeze does not return an error if there are no eligible mount points to freeze.
func tryFreeze(devicePath string, freezePoint string, mounter mount.Interface, exec lhexec.ExecuteInterface,
	log logrus.FieldLogger) (mounted, frozen bool, err error) {
	// We do not need to switch to the host mount namespace to get mount points here. Usually, longhorn-engine runs in a
	// container that has / bind mounted to /host with at least HostToContainer (rslave) propagation.
	// - If it does not, we likely can't do a namespace swap anyway, since we don't have access to /host/proc.
	// - If it does, we just need to know where in the container we can access the mount points to create a bind mount.
	sourcePoints, err := mounter.List()
	if err != nil {
		log.WithError(err).Warn("Failed to list mount points while deciding whether or not to freeze")
	}

	for _, sourcePoint := range sourcePoints {
		if sourcePoint.Device != devicePath {
			continue // We are iterating through the list of all mounted filesystems.
		}
		log.Debugf("Mounting filesystem to %v", freezePoint)
		if err = attemptBindMountFilesystem(sourcePoint.Path, freezePoint, mounter); err != nil {
			// There's no particular reason to think we will be successful with a different sourcePoint.
			err = errors.Wrapf(err, "failed to bind mount filesystem from %v to %v", sourcePoint, freezePoint)
			return
		}
		mounted = true

		// We must verify it is still safe to freeze the filesystem. If the source mount was unmounted by someone else
		// before we bind mounted, the bind mount could refer to the root filesystem. It does not matter if the source
		// mount is unmounted AFTER our bind mount, as the bind mount is not affected.
		var device string
		device, _, err = mount.GetDeviceNameFromMount(mounter, freezePoint)
		if err != nil || device != devicePath {
			// This would likely only happen if an upper layer unmounted the volume. If that is the case, we are
			// probably being torn down. Trying other sourcePoints will likely lead to a pointless race.
			err = errors.Wrapf(err, "cannot verify mount point %v refers to volume", freezePoint)
			return
		}

		log.Infof("Freezing filesystem mounted at %v", freezePoint)
		if err = util.FreezeFilesystem(freezePoint, exec); err == nil {
			frozen = true
		}
		break
	}

	return
}

// attemptBindMountFilesystem attempts to bind mount sourcePoint to mountPoint. attemptBindMountFilesystem considers it
// an error if mountPoint already has a filesystem mounted. This likely indicates another freeze is in progress. We do
// not want to double mount, etc.
func attemptBindMountFilesystem(sourcePoint, mountPoint string, mounter mount.Interface) error {
	if err := os.MkdirAll(mountPoint, 0700); err != nil {
		return err
	}

	isMountPoint, err := mounter.IsMountPoint(mountPoint)
	if err != nil {
		return err
	}
	if isMountPoint {
		return errors.Wrapf(err, "filesystem is already mounted to %v", mountPoint)
	}

	return mounter.Mount(sourcePoint, mountPoint, "", []string{"bind"})
}
