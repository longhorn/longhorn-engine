package controller

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	iutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

type Controller struct {
	sync.RWMutex
	Name       string
	size       int64
	sectorSize int64
	replicas   []types.Replica
	factory    types.BackendFactory
	backend    *replicator
	frontend   types.Frontend
	isUpgrade  bool

	isExpanding             bool
	revisionCounterDisabled bool
	salvageRequested        bool

	listenAddr string
	listenPort string

	GRPCAddress string
	GRPCServer  *grpc.Server

	ShutdownWG sync.WaitGroup
	lastError  error

	latestMetrics *types.Metrics
	metrics       *types.Metrics

	backupList      map[string]string
	backupListMutex *sync.RWMutex

	lastExpansionError    string
	lastExpansionFailedAt string
}

const (
	freezeTimeout         = 60 * time.Minute // qemu uses 60 minute timeouts for freezing
	syncTimeout           = 60 * time.Minute
	lastModifyCheckPeriod = 5 * time.Second
)

func NewController(name string, factory types.BackendFactory, frontend types.Frontend, isUpgrade bool, disableRevCounter bool, salvageRequested bool) *Controller {
	c := &Controller{
		factory:       factory,
		Name:          name,
		frontend:      frontend,
		metrics:       &types.Metrics{},
		latestMetrics: &types.Metrics{},

		backupList:      map[string]string{},
		backupListMutex: &sync.RWMutex{},

		isUpgrade:               isUpgrade,
		revisionCounterDisabled: disableRevCounter,
		salvageRequested:        salvageRequested,
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
			logrus.Errorf("Failed to listen %v: %v", grpcAddress, err)
			c.lastError = err
			return
		}

		logrus.Infof("Listening on gRPC Controller server: %v", grpcAddress)
		err = c.GRPCServer.Serve(listener)
		logrus.Errorf("GRPC server at %v is down: %v", grpcAddress, err)
		c.lastError = err
		return
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
		return false, fmt.Errorf("Can only have one WO replica at a time")
	}
	if c.isExpanding {
		return false, fmt.Errorf("Cannot add WO replica during expansion")
	}
	return true, nil
}

func (c *Controller) addReplica(address string, snapshotRequired bool, mode types.Mode) error {
	c.Lock()
	defer c.Unlock()
	if ok, err := c.canAdd(address); !ok {
		return err
	}

	newBackend, err := c.factory.Create(address)
	if err != nil {
		return err
	}

	replicaSize, err := newBackend.Size()
	if err != nil {
		return errors.Wrapf(err, "failed to get the size before adding a replica")
	}
	if c.size == 0 && len(c.replicas) == 0 {
		c.size = replicaSize
	}

	return c.addReplicaNoLock(newBackend, address, snapshotRequired, mode)
}

// Snapshot will try to freeze the filesystem of the volume if possible
// and will fallback to a system level sync in all other cases
func (c *Controller) Snapshot(name string, labels map[string]string) (string, error) {
	log := logrus.WithFields(logrus.Fields{"volume": c.Name, "snapshot": name})
	if ne, err := iutil.NewNamespaceExecutor(util.GetInitiatorNS()); err != nil {
		log.WithError(err).Error("WARNING: cannot get namespace executor for volume sync/freeze")
	} else {
		// we always try to unfreeze the volume after finishing the snapshot process
		// if the user manually froze the filesystem this will lead to an unfreeze but it's better than
		// potentially having a left over frozen volume.
		defer unfreezeVolume(ne, c.Name)
		isFrozen, err := freezeVolume(ne, c.Name)
		if err != nil {
			log.WithError(err).Error("WARNING: failed to freeze the volume filesystem falling back to sync")
		}

		if !isFrozen {
			log.Info("Volume is not frozen, requesting system sync before snapshot")
			if _, err := ne.ExecuteWithTimeout(syncTimeout, "sync", []string{}); err != nil {
				// sync should never fail though, so it more like due to the nsenter
				log.WithError(err).Errorf("WARNING: failed to sync continuing with snapshot")
			}
		}
	}

	c.Lock()
	defer c.Unlock()

	if name == "" {
		name = util.UUID()
	}

	if remain, err := c.backend.RemainSnapshots(); err != nil {
		return "", err
	} else if remain <= 0 {
		return "", fmt.Errorf("Too many snapshots created")
	}

	created := util.Now()
	if err := c.handleErrorNoLock(c.backend.Snapshot(name, true, created, labels)); err != nil {
		return "", err
	}
	return name, nil
}

func (c *Controller) Expand(size int64) error {
	if err := c.startExpansion(size); err != nil {
		logrus.Errorf("controller failed to start expansion: %v", err)
		return err
	}

	go func(size int64) {
		expanded := false
		defer func() {
			c.finishExpansion(expanded, size)
		}()

		// We perform a system level sync without the lock. Cannot block read/write
		// Can be improved to only sync the filesystem on the block device later
		if ne, err := iutil.NewNamespaceExecutor(util.GetInitiatorNS()); err != nil {
			logrus.Errorf("WARNING: continue to expand to size %v for %v, but cannot sync due to cannot get the namespace executor: %v", size, c.Name, err)
		} else {
			if _, err := ne.ExecuteWithTimeout(syncTimeout, "sync", []string{}); err != nil {
				// sync should never fail though, so it more like due to the nsenter
				logrus.Errorf("WARNING: continue to expand to size %v for %v, but sync failed: %v", size, c.Name, err)
			}
		}

		// Should block R/W during the expansion.
		c.Lock()
		defer c.Unlock()
		if remain, err := c.backend.RemainSnapshots(); err != nil {
			logrus.Errorf("Cannot get remain snapshot count before expansion: %v", err)
			return
		} else if remain <= 0 {
			logrus.Errorf("Cannot do expansion since too many snapshots created")
			return
		}

		expansionSuccess, errsNeedToBeHandled, errsForRecording := c.backend.Expand(size)
		if errsForRecording != nil {
			c.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339)
			if expansionSuccess {
				c.lastExpansionError = fmt.Sprintf("the expansion succeeded, but some replica expansion failed: %v", errsForRecording)
			} else {
				c.lastExpansionError = fmt.Sprintf("the expansion failed since all replica expansion failed: %v", errsForRecording)
			}
			if err := c.handleErrorNoLock(errsNeedToBeHandled); err != nil {
				logrus.Errorf("failed to handle the backend expansion errors: %v", err)
			}
			// If there is expansion failure, controller cannot continue expanding the frontend
			if !expansionSuccess {
				return
			}
		}

		if c.frontend != nil {
			if err := c.frontend.Expand(size); err != nil {
				logrus.Errorf("failed to expand the frontend: %v", err)
				return
			}
		}

		expanded = true
		return
	}(size)

	return nil
}

func (c *Controller) startExpansion(size int64) (err error) {
	c.Lock()
	defer c.Unlock()

	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to start expansion: %v", err)
			c.lastExpansionFailedAt = time.Now().UTC().Format(time.RFC3339)
			c.lastExpansionError = err.Error()
		}
	}()

	if c.isExpanding {
		return fmt.Errorf("controller expansion is in progress")
	}
	if c.hasWOReplica() {
		return fmt.Errorf("cannot do expansion since there is WO(rebuilding) replica")
	}
	if c.size > size {
		return fmt.Errorf("controller cannot be expanded to a smaller size %v", size)
	} else if c.size == size {
		return fmt.Errorf("controller %v is already expanded to size %v", c.Name, size)
	}
	if c.frontend != nil && c.frontend.State() == types.StateUp {
		return fmt.Errorf("controller %v doesn't support on-line expansion, frontend: %v", c.Name, c.frontend.FrontendName())
	}

	c.isExpanding = true
	return nil
}

func (c *Controller) finishExpansion(expanded bool, size int64) {
	c.Lock()
	c.Unlock()
	if expanded {
		c.size = size
	}
	c.isExpanding = false
	return
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

func (c *Controller) addReplicaNoLock(newBackend types.Backend, address string, snapshot bool, mode types.Mode) error {
	if ok, err := c.canAdd(address); !ok {
		return err
	}

	if snapshot && mode != types.ERR {
		uuid := util.UUID()
		created := util.Now()

		if remain, err := c.backend.RemainSnapshots(); err != nil {
			return err
		} else if remain <= 0 {
			return fmt.Errorf("Too many snapshots created")
		}

		if err := c.backend.Snapshot(uuid, false, created, nil); err != nil {
			newBackend.Close()
			return err
		}
		if err := newBackend.Snapshot(uuid, false, created, nil); err != nil {
			newBackend.Close()
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
				return fmt.Errorf("Cannot remove last replica if volume is up")
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
		return fmt.Errorf("Can not set to mode %s", mode)
	}

	c.setReplicaModeNoLock(address, mode)
	return nil
}

func (c *Controller) setReplicaModeNoLock(address string, mode types.Mode) {
	for i, r := range c.replicas {
		if r.Address == address {
			if r.Mode != types.ERR {
				logrus.Infof("Set replica %v to mode %v", address, mode)
				r.Mode = mode
				c.replicas[i] = r
				c.backend.SetMode(address, mode)
			} else {
				logrus.Infof("Ignore set replica %v to mode %v due to it's ERR",
					address, mode)
			}
		}
	}
}

func (c *Controller) startFrontend() error {
	if len(c.replicas) > 0 && c.frontend != nil {
		if c.isUpgrade {
			logrus.Infof("Upgrading frontend")
			if err := c.frontend.Upgrade(c.Name, c.size, c.sectorSize, c); err != nil {
				logrus.Errorf("Failed to upgrade frontend: %v", err)
				return errors.Wrap(err, "failed to upgrade frontend")
			}
			return nil
		}
		if err := c.frontend.Init(c.Name, c.size, c.sectorSize); err != nil {
			logrus.Errorf("Failed to init frontend: %v", err)
			return errors.Wrap(err, "failed to init frontend")
		}
		if err := c.frontend.Startup(c); err != nil {
			logrus.Errorf("Failed to startup frontend: %v", err)
			return errors.Wrap(err, "failed to start up frontend")
		}
	}
	return nil
}

func (c *Controller) StartFrontend(frontend string) error {
	c.Lock()
	defer c.Unlock()

	if c.isExpanding {
		return fmt.Errorf("Cannot start frontend during the engine expanison")
	}
	if frontend == "" {
		return fmt.Errorf("Cannot start empty frontend")
	}
	if c.frontend != nil {
		if c.frontend.FrontendName() != frontend && c.frontend.State() != types.StateDown {
			return fmt.Errorf("Frontend %v is already started, cannot be set as %v",
				c.frontend.FrontendName(), frontend)
		}
	}
	f, ok := Frontends[frontend]
	if !ok {
		return fmt.Errorf("Failed to find frontend: %s", frontend)
	}
	c.frontend = f
	return c.startFrontend()
}

// Check if all replica revision counter setting match with engine
// controller, and mark unmatch replica to ERR.
func (c *Controller) checkReplicaRevCounterSettingMatch() error {
	for _, r := range c.replicas {
		if r.Mode == "ERR" {
			continue
		}
		revCounterDisabled, err := c.backend.backends[r.Address].backend.IsRevisionCounterDisabled()
		if err != nil {
			return err
		}

		if c.revisionCounterDisabled != revCounterDisabled {
			logrus.Errorf("Revision Counter Disabled setting mismatch at engine %v, replica %v: %v, mark this replica as ERR.", c.revisionCounterDisabled, r.Address, revCounterDisabled)
			c.setReplicaModeNoLock(r.Address, types.ERR)
		}
	}

	return nil
}

// salvageRevisionCounterDisabledReplicas will find best replica
// for salvage recovering, based on lastModifyTime and HeadFileSize.
func (c *Controller) salvageRevisionCounterDisabledReplicas() error {
	replicaCandidates := make(map[types.Replica]types.ReplicaSalvageInfo)
	var lastModifyTime int64
	for _, r := range c.replicas {
		if r.Mode == "ERR" {
			continue
		}
		repLastModifyTime, err := c.backend.backends[r.Address].backend.GetLastModifyTime()
		if err != nil {
			return err
		}

		repHeadFileSize, err := c.backend.backends[r.Address].backend.GetLastModifyTime()
		if err != nil {
			return err
		}

		replicaCandidates[r] = types.ReplicaSalvageInfo{
			LastModifyTime: repLastModifyTime,
			HeadFileSize:   repHeadFileSize,
		}
		if lastModifyTime == 0 ||
			repLastModifyTime > lastModifyTime {
			lastModifyTime = repLastModifyTime
		}
	}

	if len(replicaCandidates) == 0 {
		return fmt.Errorf("Can not find any replica for salvage")
	}
	var bestCandidate types.Replica
	lastTime := time.Unix(0, lastModifyTime)
	var largestSize int64
	for r, salvageReplica := range replicaCandidates {
		t := time.Unix(0, salvageReplica.LastModifyTime)
		// Any replica within 5 seconds before lastModifyTime
		// can be good replica.
		if t.Add(lastModifyCheckPeriod).After(lastTime) {
			if salvageReplica.HeadFileSize >= largestSize {
				bestCandidate = r
			}
		}
	}

	if bestCandidate == (types.Replica{}) {
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

func (c *Controller) Start(addresses ...string) error {
	c.Lock()
	defer c.Unlock()

	if len(addresses) == 0 {
		return nil
	}

	if len(c.replicas) > 0 {
		return nil
	}

	c.reset()

	var fatalErr error
	availableBackends := map[string]types.Backend{}
	first := true
	for _, address := range addresses {
		newBackend, err := c.factory.Create(address)
		if err != nil {
			logrus.Warnf("failed to create backend with address %v: %v", address, err)
			continue
		}
		newSize, err := newBackend.Size()
		if err != nil {
			logrus.Warnf("failed to get the size from the backend address %v: %v", address, err)
			continue
		}
		newSectorSize, err := newBackend.SectorSize()
		if err != nil {
			logrus.Warnf("failed to get the sector size from the backend address %v: %v", address, err)
			continue
		}
		availableBackends[address] = newBackend

		if first {
			first = false
			c.size = newSize
			c.sectorSize = newSectorSize
		} else if c.size != newSize {
			availableBackends = map[string]types.Backend{}
			fatalErr = fmt.Errorf("BUG: Backend sizes do not match %d != %d in the engine initiation phase", c.size, newSize)
			break
		} else if c.sectorSize != newSectorSize {
			availableBackends = map[string]types.Backend{}
			fatalErr = fmt.Errorf("BUG: Backend sector sizes do not match %d != %d in the engine initiation phase", c.sectorSize, newSectorSize)
			break
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

	if fatalErr != nil {
		return fatalErr
	}
	if len(availableBackends) == 0 {
		return fmt.Errorf("cannot create an available backend for the engine from the addresses %+v", addresses)
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
			// alway check the revision counter.
			if err := c.checkReplicasRevisionCounter(); err != nil {
				return err
			}
		}
	}

	if err := c.startFrontend(); err != nil {
		return err
	}

	return nil
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
	n, err := c.backend.WriteAt(b, off)
	c.RUnlock()
	if err != nil {
		return n, c.handleError(err)
	}
	c.recordMetrics(false, l, time.Now().Sub(startTime))
	return n, err
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
	c.recordMetrics(true, l, time.Now().Sub(startTime))
	return n, err
}

func (c *Controller) handleErrorNoLock(err error) error {
	if bErr, ok := err.(*BackendError); ok {
		if len(bErr.Errors) > 0 {
			for address, replicaErr := range bErr.Errors {
				logrus.Errorf("Setting replica %s to ERR due to: %v", address, replicaErr)
				c.setReplicaModeNoLock(address, types.ERR)
			}
			// if we still have a good replica, do not return error
			for _, r := range c.replicas {
				if r.Mode == types.RW {
					logrus.Errorf("Ignoring error because %s is mode RW: %v", r.Address, err)
					err = nil
					break
				}
			}
		}
	}
	if err != nil {
		logrus.Errorf("I/O error: %v", err)
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
		logrus.Error("Error when shutting down frontend:", errFrontend)
	}
	errBackend := c.shutdownBackend()
	if errBackend != nil {
		logrus.Error("Error when shutting down backend:", errBackend)
	}
	if errFrontend != nil || errBackend != nil {
		return fmt.Errorf("errors when shutting down controller: frontend: %v backend: %v",
			errFrontend, errBackend)
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
		logrus.Errorf("Backend %v monitoring failed, mark as ERR: %v", address, err)
		c.SetReplicaMode(address, types.ERR)
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
	latencyInUS := uint64(latency.Nanoseconds() / 1000)
	if isRead {
		atomic.AddUint64(&c.metrics.Bandwidth.Read, uint64(dataLength))
		atomic.AddUint64(&c.metrics.TotalLatency.Read, latencyInUS)
		atomic.AddUint64(&c.metrics.IOPS.Read, 1)
	} else {
		atomic.AddUint64(&c.metrics.Bandwidth.Write, uint64(dataLength))
		atomic.AddUint64(&c.metrics.TotalLatency.Write, latencyInUS)
		atomic.AddUint64(&c.metrics.IOPS.Write, 1)
	}
}

func (c *Controller) metricsStart() {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			c.latestMetrics = c.metrics
			c.metrics = &types.Metrics{}
		}
	}()
}

func (c *Controller) GetLatestMetics() *types.Metrics {
	return c.latestMetrics
}

func (c *Controller) BackupReplicaMappingCreate(id string, replicaAddress string) error {
	c.backupListMutex.Lock()
	defer c.backupListMutex.Unlock()
	c.backupList[id] = replicaAddress
	return nil
}

func (c *Controller) BackupReplicaMappingGet() map[string]string {
	c.backupListMutex.RLock()
	defer c.backupListMutex.RUnlock()
	ret := map[string]string{}
	for k, v := range c.backupList {
		ret[k] = v
	}
	return ret
}

func (c *Controller) BackupReplicaMappingDelete(id string) error {
	c.backupListMutex.Lock()
	defer c.backupListMutex.Unlock()
	if _, present := c.backupList[id]; present == false {
		return fmt.Errorf("backupID not found: %v", id)
	}
	delete(c.backupList, id)
	return nil
}

func freezeVolume(ne *iutil.NamespaceExecutor, volume string) (bool, error) {
	devicePath := "/dev/longhorn/" + volume
	mountPoint, err := findMountPoint(ne, devicePath)
	if err != nil {
		logrus.WithField("volume", volume).Warn("Failed to execute find mount point, cannot freeze potential filesystem")
		return false, err
	} else if mountPoint == "" {
		logrus.WithField("volume", volume).Info("Volume is not mounted, no need to freeze filesystem")
		return false, nil
	}

	if err = freezeFilesystem(ne, mountPoint); err != nil {
		logrus.WithField("volume", volume).WithError(err).Errorf("Failed to freeze the filesystem at %v", mountPoint)
		return false, err
	}

	logrus.WithField("volume", volume).Infof("Froze filesystem of volume mounted at %v", mountPoint)
	return true, nil
}

func unfreezeVolume(ne *iutil.NamespaceExecutor, volume string) error {
	devicePath := "/dev/longhorn/" + volume
	mountPoint, err := findMountPoint(ne, devicePath)
	if err != nil {
		logrus.WithField("volume", volume).Warn("Failed to execute find mount point, cannot unfreeze potential filesystem")
		return err
	} else if mountPoint == "" {
		logrus.WithField("volume", volume).Info("Volume is not mounted, no need to unfreeze filesystem")
		return nil
	}

	if err = unfreezeFilesystem(ne, mountPoint); err != nil {
		logrus.WithField("volume", volume).WithError(err).Errorf("Failed to unfreeze the filesystem at %v", mountPoint)
		return err
	}

	logrus.WithField("volume", volume).Infof("Unfroze filesystem of volume mounted at %v", mountPoint)
	return nil
}

func findMountPoint(ne *iutil.NamespaceExecutor, devicePath string) (string, error) {
	/* Example command output
	   # all of these run with nsenter --mount=/host/proc/1/ns/mnt --net=/host/proc/1/ns/net
	   # found mount, returns 0
	   $ findmnt -o TARGET,FSTYPE --noheadings --first-only --source /dev/longhorn/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec
	   /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount ext4

	   # failed to find mount, returns 1
	   $ findmnt /dev/longhorn/unknown
	*/

	logrus.Debugf("Searching for mount point of device %v", devicePath)
	var ee *exec.ExitError
	out, err := ne.Execute("findmnt", []string{"-o", "target,fstype", "--first-only", "--noheadings", "--source", devicePath})
	if err != nil && errors.As(err, &ee) && ee.ExitCode() == 1 {
		// findmnt return error code 1 if no mount is found
		// so we need to unwrap the returned error to get the exit code
		logrus.WithError(ee).Debugf("Device %v is not mounted", devicePath)
		return "", nil
	}

	var mountPoint string
	if i := strings.LastIndex(strings.TrimSuffix(out, "\n"), " "); i != -1 {
		mountPoint = out[:i]
		logrus.Debugf("Found mount point %v for device %v", mountPoint, devicePath)
	}

	return mountPoint, err
}

func freezeFilesystem(ne *iutil.NamespaceExecutor, mountPoint string) error {
	/* Example Command output
	   # successful freeze, returns 0, no output
	   $ fsfreeze -f /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount

	   # already frozen, returns 1
	   $ fsfreeze -f /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount
	   fsfreeze: /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount: freeze failed: Device or resource busy
	*/
	var ee *exec.ExitError
	_, err := ne.ExecuteWithTimeout(freezeTimeout, "fsfreeze", []string{"-f", mountPoint})
	if err != nil && errors.As(err, &ee) && ee.ExitCode() == 1 {
		// most likely the filesystem is already frozen or the mount point doesn't exist
		logrus.WithError(err).Debugf("Freeze of filesystem mounted at %v failed", mountPoint)
		return nil
	}

	return err
}

func unfreezeFilesystem(ne *iutil.NamespaceExecutor, mountPoint string) error {
	/* Example Command output
	   # successful unfreeze, returns 0, no output
	   $ fsfreeze -u /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount

	   # not frozen, returns 1
	   $ fsfreeze -u /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount
		 fsfreeze: /var/lib/kubelet/pods/4d097f01-bb44-4d7d-8905-a9c59ff09468/volumes/kubernetes.io~csi/pvc-397cfa17-70d1-4780-bac7-bbff7e00d7ec/mount: unfreeze failed: Invalid argument

	   # unknown mount point, returns 1
	   $ fsfreeze -u /no-mount-point
	   fsfreeze: cannot open /no-mount-point: No such file or directory
	*/
	var ee *exec.ExitError
	_, err := ne.ExecuteWithTimeout(freezeTimeout, "fsfreeze", []string{"-u", mountPoint})
	if err != nil && errors.As(err, &ee) && ee.ExitCode() == 1 {
		// most likely the filesystem is not frozen or the mount point doesn't exist
		logrus.WithError(err).Debugf("Unfreeze of filesystem mounted at %v failed", mountPoint)
		return nil
	}

	return err
}
