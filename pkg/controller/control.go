package controller

import (
	"fmt"
	"net"
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

	isExpanding bool

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
}

const (
	syncTimeout = 60 * time.Minute
)

func NewController(name string, factory types.BackendFactory, frontend types.Frontend, isUpgrade bool) *Controller {
	c := &Controller{
		factory:       factory,
		Name:          name,
		frontend:      frontend,
		metrics:       &types.Metrics{},
		latestMetrics: &types.Metrics{},

		backupList:      map[string]string{},
		backupListMutex: &sync.RWMutex{},

		isUpgrade: isUpgrade,
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

func (c *Controller) AddReplica(address string) error {
	return c.addReplica(address, true)
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

func (c *Controller) addReplica(address string, snapshot bool) error {
	c.Lock()
	if ok, err := c.canAdd(address); !ok {
		c.Unlock()
		return err
	}
	c.Unlock()

	newBackend, err := c.factory.Create(address)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	return c.addReplicaNoLock(newBackend, address, snapshot)
}

func (c *Controller) Snapshot(name string, labels map[string]string) (string, error) {
	// We perform a system level sync without the lock. Cannot block read/write
	// Can be improved to only sync the filesystem on the block device later
	if ne, err := iutil.NewNamespaceExecutor(util.GetInitiatorNS()); err != nil {
		logrus.Errorf("WARNING: continue to snapshot for %v, but cannot sync due to cannot get the namespace executor: %v", name, err)
	} else {
		if _, err := ne.ExecuteWithTimeout(syncTimeout, "sync", []string{}); err != nil {
			// sync should never fail though, so it more like due to the nsenter
			logrus.Errorf("WARNING: continue to snapshot for %v, but sync failed: %v", name, err)
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

		if err := c.handleErrorNoLock(c.backend.Expand(size)); err != nil {
			logrus.Errorf("failed to expand the engine backend: %v", err)
			return
		}

		if c.frontend != nil {
			if err := c.frontend.Expand(size); err != nil {
				logrus.Errorf("failed to expand the engine frontend: %v", err)
				return
			}
		}

		expanded = true
		return
	}(size)

	return nil
}

func (c *Controller) startExpansion(size int64) error {
	c.Lock()
	defer c.Unlock()
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

func (c *Controller) addReplicaNoLock(newBackend types.Backend, address string, snapshot bool) error {
	if ok, err := c.canAdd(address); !ok {
		return err
	}

	if snapshot {
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
		Mode:    types.WO,
	})

	c.backend.AddBackend(address, newBackend)

	go c.monitoring(address, newBackend)

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
		if err := c.frontend.Startup(c.Name, c.size, c.sectorSize, c); err != nil {
			logrus.Errorf("Failed to startup frontend: %v", err)
			return errors.Wrap(err, "failed to start up frontend")
		}
	}
	return nil
}

func (c *Controller) StartFrontend(frontend string) error {
	c.Lock()
	defer c.Unlock()

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

func (c *Controller) Start(addresses ...string) error {
	var expectedRevision int64

	c.Lock()
	defer c.Unlock()

	if len(addresses) == 0 {
		return nil
	}

	if len(c.replicas) > 0 {
		return nil
	}

	c.reset()

	first := true
	for _, address := range addresses {
		newBackend, err := c.factory.Create(address)
		if err != nil {
			return err
		}

		newSize, err := newBackend.Size()
		if err != nil {
			return err
		}

		newSectorSize, err := newBackend.SectorSize()
		if err != nil {
			return err
		}

		if first {
			first = false
			c.size = newSize
			c.sectorSize = newSectorSize
		} else if c.size != newSize {
			return fmt.Errorf("Backend sizes do not match %d != %d", c.size, newSize)
		} else if c.sectorSize != newSectorSize {
			return fmt.Errorf("Backend sizes do not match %d != %d", c.sectorSize, newSectorSize)
		}

		if err := c.addReplicaNoLock(newBackend, address, false); err != nil {
			return err
		}
		// We will validate this later
		c.setReplicaModeNoLock(address, types.RW)
	}

	revisionCounters := make(map[string]int64)
	for _, r := range c.replicas {
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
	err := c.shutdownFrontend()
	if err != nil {
		logrus.Error("Error when shutting down frontend:", err)
	}
	err = c.shutdownBackend()
	if err != nil {
		logrus.Error("Error when shutting down backend:", err)
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
