package engine

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-instance-manager/process"
	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"

	. "gopkg.in/check.v1"
)

const (
	RetryCount    = 50
	RetryInterval = 100 * time.Millisecond
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	shutdownCh chan error
	em         *Manager
	pm         *process.Manager
	logDir     string
}

var _ = Suite(&TestSuite{})

type EngineWatcher struct {
	grpc.ServerStream
}

func (ew *EngineWatcher) Send(resp *rpc.EngineResponse) error {
	//Do nothing for now, just act as the receiving end
	return nil
}

func (s *TestSuite) SetUpSuite(c *C) {
	var err error

	logrus.SetLevel(logrus.DebugLevel)
	s.shutdownCh = make(chan error)

	s.logDir = os.TempDir()

	s.pm, err = process.NewManager("10000-30000", s.logDir, s.shutdownCh)
	c.Assert(err, IsNil)
	s.pm.Executor = &process.MockExecutor{}
	s.pm.HealthChecker = &process.MockHealthChecker{}
	processUpdateCh, err := s.pm.Subscribe()
	c.Assert(err, IsNil)

	s.em, err = NewEngineManager(s.pm, processUpdateCh, "0.0.0.0", s.shutdownCh)
	c.Assert(err, IsNil)

	s.em.dc = &MockDeviceCreator{}
	s.em.ec = &MockVolumeClient{
		em: s.em,
	}
}

func (s *TestSuite) TearDownSuite(c *C) {
	close(s.shutdownCh)
}

func (s *TestSuite) TestEngineManager(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	ew := &EngineWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "test_crud_engine-" + strconv.Itoa(i)
			volumeName := name + "-volume"
			binary := "any"
			// bypass the os.Stat() check
			upgradedBinary := "/bin/bash"
			size := int64(1024)
			replicas := []string{"replica1-" + name, "replica2-" + name}
			upgradedReplicas := []string{"upgraded_replica1-" + name, "upgraded_replica2-" + name}
			go s.em.EngineWatch(nil, ew)

			createReq := &rpc.EngineCreateRequest{
				Spec: &rpc.EngineSpec{
					Name:       name,
					VolumeName: volumeName,
					Binary:     binary,
					ListenIp:   "0.0.0.0",
					Size:       size,
					Frontend:   "tgt-blockdev",
					Backends:   []string{"tcp"},
					Replicas:   replicas,
				},
			}
			createResp, err := s.em.EngineCreate(nil, createReq)
			c.Assert(err, IsNil)
			c.Assert(createResp.Spec.Frontend, Equals, "tgt-blockdev")
			c.Assert(createResp.Spec.Size, Equals, size)
			c.Assert(createResp.Spec.Binary, Equals, binary)
			c.Assert(createResp.Spec.Replicas, DeepEquals, replicas)
			c.Assert(createResp.Status.ProcessStatus.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(createResp.Status.ProcessStatus.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(createResp.Status.ProcessStatus.State, Not(Equals), types.ProcessStateError)

			running := false
			for j := 0; j < RetryCount; j++ {
				getResp, err := s.em.EngineGet(nil, &rpc.EngineRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				c.Assert(getResp.Spec.Name, Equals, name)
				c.Assert(getResp.Spec.VolumeName, Equals, volumeName)
				c.Assert(getResp.Spec.Size, Equals, size)
				c.Assert(getResp.Spec.Frontend, Equals, "tgt-blockdev")
				c.Assert(getResp.Spec.Binary, Equals, binary)
				c.Assert(getResp.Spec.Replicas, DeepEquals, replicas)
				if getResp.Status.ProcessStatus.State == types.ProcessStateRunning {
					running = true
					break
				}
				time.Sleep(RetryInterval)
			}
			c.Assert(running, Equals, true)

			listResp, err := s.em.EngineList(nil, nil)
			c.Assert(err, IsNil)
			c.Assert(listResp.Engines[name], NotNil)
			c.Assert(listResp.Engines[name].Spec.Name, Equals, name)
			c.Assert(listResp.Engines[name].Spec.VolumeName, Equals, volumeName)
			c.Assert(listResp.Engines[name].Spec.Size, Equals, size)
			c.Assert(listResp.Engines[name].Spec.Binary, Equals, binary)
			c.Assert(listResp.Engines[name].Spec.Replicas, DeepEquals, replicas)
			c.Assert(listResp.Engines[name].Status.ProcessStatus.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(listResp.Engines[name].Status.ProcessStatus.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(listResp.Engines[name].Status.ProcessStatus.State, Not(Equals), types.ProcessStateError)

			_, err = s.em.FrontendStartCallback(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(err, IsNil)

			getResp, err := s.em.EngineGet(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Spec.VolumeName, Equals, volumeName)
			c.Assert(getResp.Spec.Size, Equals, size)
			c.Assert(getResp.Spec.Frontend, Equals, "tgt-blockdev")
			c.Assert(getResp.Spec.Binary, Equals, binary)
			c.Assert(getResp.Spec.Replicas, DeepEquals, replicas)
			c.Assert(getResp.Status.ProcessStatus.State, Equals, types.ProcessStateRunning)

			_, err = s.em.FrontendShutdownCallback(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(err, IsNil)

			getResp, err = s.em.EngineGet(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Spec.VolumeName, Equals, volumeName)
			c.Assert(getResp.Spec.Size, Equals, size)
			c.Assert(getResp.Spec.Frontend, Equals, "tgt-blockdev")
			c.Assert(getResp.Spec.Binary, Equals, binary)
			c.Assert(getResp.Spec.Replicas, DeepEquals, replicas)
			c.Assert(getResp.Status.ProcessStatus.State, Equals, types.ProcessStateRunning)

			_, err = s.em.FrontendShutdown(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(err, IsNil)

			getResp, err = s.em.EngineGet(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Spec.VolumeName, Equals, volumeName)
			c.Assert(getResp.Spec.Size, Equals, size)
			c.Assert(getResp.Spec.Frontend, Equals, "")
			c.Assert(getResp.Spec.Binary, Equals, binary)
			c.Assert(getResp.Spec.Replicas, DeepEquals, replicas)
			c.Assert(getResp.Status.ProcessStatus.State, Equals, types.ProcessStateRunning)

			_, err = s.em.FrontendStart(nil, &rpc.FrontendStartRequest{
				Name:     name,
				Frontend: "tgt-iscsi",
			})
			c.Assert(err, IsNil)

			getResp, err = s.em.EngineGet(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Spec.VolumeName, Equals, volumeName)
			c.Assert(getResp.Spec.Size, Equals, size)
			c.Assert(getResp.Spec.Frontend, Equals, "tgt-iscsi")
			c.Assert(getResp.Spec.Binary, Equals, binary)
			c.Assert(getResp.Spec.Replicas, DeepEquals, replicas)
			c.Assert(getResp.Status.ProcessStatus.State, Equals, types.ProcessStateRunning)

			upgradeResp, err := s.em.EngineUpgrade(nil, &rpc.EngineUpgradeRequest{
				Spec: &rpc.EngineSpec{
					Name:     name,
					Binary:   upgradedBinary,
					Size:     size,
					Replicas: upgradedReplicas,
				},
			})
			c.Assert(err, IsNil)
			c.Assert(upgradeResp.Spec.Name, Equals, name)
			c.Assert(upgradeResp.Spec.VolumeName, Equals, volumeName)
			c.Assert(upgradeResp.Spec.Size, Equals, size)
			c.Assert(upgradeResp.Spec.Frontend, Equals, "tgt-iscsi")
			c.Assert(upgradeResp.Spec.Binary, Equals, upgradedBinary)
			c.Assert(upgradeResp.Spec.Replicas, DeepEquals, upgradedReplicas)
			c.Assert(upgradeResp.Status.ProcessStatus.State, Equals, types.ProcessStateRunning)

			getResp, err = s.em.EngineGet(nil, &rpc.EngineRequest{
				Name: name,
			})
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Spec.VolumeName, Equals, volumeName)
			c.Assert(getResp.Spec.Size, Equals, size)
			c.Assert(getResp.Spec.Frontend, Equals, "tgt-iscsi")
			c.Assert(getResp.Spec.Binary, Equals, upgradedBinary)
			c.Assert(getResp.Spec.Replicas, DeepEquals, upgradedReplicas)
			c.Assert(getResp.Status.ProcessStatus.State, Equals, types.ProcessStateRunning)

			deleteReq := &rpc.EngineRequest{
				Name: name,
			}
			deleteResp, err := s.em.EngineDelete(nil, deleteReq)
			c.Assert(err, IsNil)
			c.Assert(deleteResp.Status.ProcessStatus.State, Not(Equals), types.ProcessStateStarting)
			c.Assert(deleteResp.Status.ProcessStatus.State, Not(Equals), types.ProcessStateRunning)
			c.Assert(deleteResp.Status.ProcessStatus.State, Not(Equals), types.ProcessStateError)
		}(i)
	}
	wg.Wait()
}
