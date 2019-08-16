package process

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

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
	pm         *Manager
	logDir     string
}

var _ = Suite(&TestSuite{})

func generateUUID() string {
	return uuid.NewV4().String()
}

type ProcessWatcher struct {
	grpc.ServerStream
}

func (pw *ProcessWatcher) Send(resp *rpc.ProcessResponse) error {
	//Do nothing for now, just act as the receiving end
	return nil
}

func (s *TestSuite) SetUpSuite(c *C) {
	var err error

	logrus.SetLevel(logrus.DebugLevel)
	s.shutdownCh = make(chan error)

	s.logDir = os.TempDir()
	s.pm, err = NewManager("10000-30000", s.logDir, s.shutdownCh)
	c.Assert(err, IsNil)
	s.pm.Executor = &TestExecutor{}
	s.pm.HealthChecker = &MockHealthChecker{}
}

func (s *TestSuite) TearDownSuite(c *C) {
	close(s.shutdownCh)
}

func (s *TestSuite) TestCRUD(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	pw := &ProcessWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "test_crud_process-" + strconv.Itoa(i)
			binary := "any"
			go s.pm.ProcessWatch(nil, pw)

			createReq := &rpc.ProcessCreateRequest{
				Spec: &rpc.ProcessSpec{
					Name:      name,
					Binary:    binary,
					Args:      []string{},
					PortCount: 1,
					PortArgs:  nil,
				},
			}
			createResp, err := s.pm.ProcessCreate(nil, createReq)
			c.Assert(err, IsNil)
			c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateError)

			getResp, err := s.pm.ProcessGet(nil, &rpc.ProcessGetRequest{
				Name: name,
			})
			c.Assert(err, IsNil)
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Status.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(getResp.Status.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(getResp.Status.State, Not(Equals), types.ProcessStateError)

			listResp, err := s.pm.ProcessList(nil, &rpc.ProcessListRequest{})
			c.Assert(err, IsNil)
			c.Assert(listResp.Processes[name], NotNil)
			c.Assert(listResp.Processes[name].Spec.Name, Equals, name)
			c.Assert(listResp.Processes[name].Status.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(listResp.Processes[name].Status.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(listResp.Processes[name].Status.State, Not(Equals), types.ProcessStateError)

			running := false
			for j := 0; j < RetryCount; j++ {
				getResp, err := s.pm.ProcessGet(nil, &rpc.ProcessGetRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				if getResp.Status.State == types.ProcessStateRunning {
					running = true
					break
				}
				time.Sleep(RetryInterval)
			}
			c.Assert(running, Equals, true)

			deleteReq := &rpc.ProcessDeleteRequest{
				Name: name,
			}
			deleteResp, err := s.pm.ProcessDelete(nil, deleteReq)
			c.Assert(err, IsNil)
			c.Assert(deleteResp.Deleted, Equals, true)
			c.Assert(deleteResp.Status.State, Not(Equals), types.ProcessStateStarting)
			c.Assert(deleteResp.Status.State, Not(Equals), types.ProcessStateRunning)
			c.Assert(deleteResp.Status.State, Not(Equals), types.ProcessStateError)
		}(i)
	}
	wg.Wait()
}
