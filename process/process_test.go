package process

import (
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/satori/go.uuid"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"

	. "gopkg.in/check.v1"
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

func (s *TestSuite) SetUpSuite(c *C) {
	var err error

	s.shutdownCh = make(chan error)

	s.logDir = os.TempDir()
	s.pm, err = NewManager("10000-30000", s.logDir, s.shutdownCh)
	c.Assert(err, IsNil)
	s.pm.Executor = &types.TestExecutor{}
}

func (s *TestSuite) TearDownSuite(c *C) {
	close(s.shutdownCh)
}

func (s *TestSuite) TestCRUD(c *C) {
	count := 1
	wg := &sync.WaitGroup{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.TODO()
			name := "test_crud_process-" + strconv.Itoa(i)
			binary := "any"
			uuid := generateUUID()
			createReq := &rpc.ProcessCreateRequest{
				Spec: &rpc.ProcessSpec{
					Uuid:      uuid,
					Name:      name,
					Binary:    binary,
					Args:      []string{},
					PortCount: 0,
					PortArgs:  nil,
				},
			}

			_, err := s.pm.ProcessCreate(ctx, createReq)
			c.Assert(err, IsNil)

			getResp, err := s.pm.ProcessGet(ctx, &rpc.ProcessGetRequest{
				Name: name,
			})
			c.Assert(err, IsNil)
			c.Assert(getResp.Spec.Uuid, Equals, uuid)
			c.Assert(getResp.Spec.Name, Equals, name)

			listResp, err := s.pm.ProcessList(ctx, &rpc.ProcessListRequest{})
			c.Assert(err, IsNil)
			c.Assert(listResp.Processes[name], NotNil)
			c.Assert(listResp.Processes[name].Spec.Uuid, Equals, uuid)
			c.Assert(listResp.Processes[name].Spec.Name, Equals, name)

			deleteReq := &rpc.ProcessDeleteRequest{
				Name: name,
			}

			deleteResp, err := s.pm.ProcessDelete(ctx, deleteReq)
			c.Assert(err, IsNil)
			c.Assert(deleteResp.Deleted, Equals, true)
		}(i)
	}
	wg.Wait()
}
