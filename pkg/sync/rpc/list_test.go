package rpc

import (
	"strconv"
	"testing"

	"golang.org/x/net/context"
	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-engine/pkg/replica"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestSnapshotHashListCRUD(c *C) {
	list := &SnapshotHashList{}

	snapshotName := "snapshot0"
	ctx, cancel := context.WithCancel(context.Background())
	task := replica.NewSnapshotHashJob(ctx, cancel, snapshotName, false)
	list.Add(snapshotName, task)

	_, err := list.Get(snapshotName)
	c.Assert(err, IsNil)

	_, err = list.Get("nonexistence")
	c.Assert(err, NotNil)

	err = list.Delete(snapshotName)
	c.Assert(err, IsNil)

	_, err = list.Get(snapshotName)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestSnapshotHashListRefreshTriggerByAdd(c *C) {
	list := &SnapshotHashList{}

	numSnapshots := MaxSnapshotHashJobSize + 2
	for i := 0; i < numSnapshots; i++ {
		snapshotName := "snapshot" + strconv.Itoa(i)

		ctx, cancel := context.WithCancel(context.Background())
		task := replica.NewSnapshotHashJob(ctx, cancel, snapshotName, false)
		task.State = replica.ProgressStateComplete

		list.Add(snapshotName, task)

		size := list.GetSize()
		if i < MaxSnapshotHashJobSize {
			c.Assert(size, Equals, i+1)
		} else {
			c.Assert(size, Equals, MaxSnapshotHashJobSize)
		}
	}
}

func (s *TestSuite) TestSnapshotHashListRefreshTriggerByGet(c *C) {
	list := &SnapshotHashList{}

	numSnapshots := MaxSnapshotHashJobSize + 1
	for i := 0; i < numSnapshots; i++ {
		snapshotName := "snapshot" + strconv.Itoa(i)
		ctx, cancel := context.WithCancel(context.Background())
		task := replica.NewSnapshotHashJob(ctx, cancel, snapshotName, false)
		list.Add(snapshotName, task)
	}

	for i := 0; i < numSnapshots; i++ {
		status, err := list.Get("snapshot" + strconv.Itoa(i))
		c.Assert(err, IsNil)

		status.State = replica.ProgressStateComplete

		// Try to trigger refresh
		_, err = list.Get("snapshot" + strconv.Itoa(i))
		c.Assert(err, IsNil)

		size := list.GetSize()
		if i < MaxSnapshotHashJobSize {
			c.Assert(size, Equals, MaxSnapshotHashJobSize+1)
		} else {
			c.Assert(size, Equals, MaxSnapshotHashJobSize)
		}
	}
}
