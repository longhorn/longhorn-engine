package replica

import (
	"crypto/md5"
	"fmt"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	"github.com/longhorn/longhorn-engine/pkg/util"
	. "gopkg.in/check.v1"
)

const (
	b  = 4096
	bs = 512
)

type TestBackingFile struct {
	*os.File
}

func (f *TestBackingFile) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), nil
}

func NewTestBackingFile(path string) (*TestBackingFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &TestBackingFile{
		f,
	}, nil
}

func (f *TestBackingFile) Size() (int64, error) {
	info, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestCreate(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()
}

func getNow() string {
	// Make sure timestamp is unique
	time.Sleep(1 * time.Second)
	return util.Now()
}

func (s *TestSuite) TestSnapshot(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	createdTime0 := getNow()

	labels := map[string]string{"name": "000", "key": "value"}
	err = r.Snapshot("000", true, createdTime0, labels)
	c.Assert(err, IsNil)

	createdTime1 := getNow()
	err = r.Snapshot("001", true, createdTime1, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].UserCreated, Equals, false)
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[3].Created, Equals, createdTime1)

	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].UserCreated, Equals, true)
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[2].Created, Equals, createdTime1)

	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].UserCreated, Equals, true)
	c.Assert(r.activeDiskData[1].Parent, Equals, "")
	c.Assert(r.activeDiskData[1].Created, Equals, createdTime0)
	c.Assert(r.activeDiskData[1].Labels, DeepEquals, labels)

	c.Assert(len(r.diskData), Equals, 3)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-000.img"].UserCreated, Equals, true)
	c.Assert(r.diskData["volume-snap-000.img"].Created, Equals, createdTime0)
	c.Assert(r.diskData["volume-snap-000.img"].Labels, DeepEquals, labels)
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-001.img"].UserCreated, Equals, true)
	c.Assert(r.diskData["volume-snap-001.img"].Created, Equals, createdTime1)
	c.Assert(r.diskData["volume-head-002.img"].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.diskData["volume-head-002.img"].UserCreated, Equals, false)
	c.Assert(r.diskData["volume-head-002.img"].Created, Equals, createdTime1)

	c.Assert(len(r.diskChildrenMap), Equals, 3)
	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-001.img"], Equals, true)
	c.Assert(len(r.diskChildrenMap["volume-snap-001.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-001.img"]["volume-head-002.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-head-002.img"], IsNil)

	disks := r.ListDisks()
	c.Assert(len(disks), Equals, 3)
	c.Assert(disks["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(disks["volume-snap-000.img"].UserCreated, Equals, true)
	c.Assert(disks["volume-snap-000.img"].Removed, Equals, false)
	c.Assert(len(disks["volume-snap-000.img"].Children), Equals, 1)
	c.Assert(disks["volume-snap-000.img"].Children["volume-snap-001.img"], Equals, true)
	c.Assert(disks["volume-snap-000.img"].Created, Equals, createdTime0)
	c.Assert(disks["volume-snap-000.img"].Size, Equals, "0")
	c.Assert(disks["volume-snap-000.img"].Labels, DeepEquals, labels)

	c.Assert(disks["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(disks["volume-snap-001.img"].UserCreated, Equals, true)
	c.Assert(disks["volume-snap-001.img"].Removed, Equals, false)
	c.Assert(len(disks["volume-snap-001.img"].Children), Equals, 1)
	c.Assert(disks["volume-snap-001.img"].Children["volume-head-002.img"], Equals, true)
	c.Assert(disks["volume-snap-001.img"].Created, Equals, createdTime1)
	c.Assert(disks["volume-snap-001.img"].Size, Equals, "0")

	c.Assert(disks["volume-head-002.img"].Parent, Equals, "volume-snap-001.img")
	c.Assert(disks["volume-head-002.img"].UserCreated, Equals, false)
	c.Assert(disks["volume-head-002.img"].Removed, Equals, false)
	c.Assert(len(disks["volume-head-002.img"].Children), Equals, 0)
	c.Assert(disks["volume-head-002.img"].Created, Equals, createdTime1)
	c.Assert(disks["volume-head-002.img"].Size, Equals, "0")
}

func (s *TestSuite) TestRevert(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	createdTime0 := getNow()
	err = r.Snapshot("000", true, createdTime0, nil)
	c.Assert(err, IsNil)

	createdTime1 := getNow()
	err = r.Snapshot("001", true, createdTime1, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	chain, err := r.Chain()
	c.Assert(err, IsNil)
	c.Assert(len(chain), Equals, 3)
	c.Assert(chain[0], Equals, "volume-head-002.img")
	c.Assert(chain[1], Equals, "volume-snap-001.img")
	c.Assert(chain[2], Equals, "volume-snap-000.img")

	revertTime1 := getNow()
	r, err = r.Revert("volume-snap-000.img", revertTime1)
	c.Assert(err, IsNil)

	chain, err = r.Chain()
	c.Assert(err, IsNil)
	c.Assert(len(chain), Equals, 2)
	c.Assert(chain[0], Equals, "volume-head-003.img")
	c.Assert(chain[1], Equals, "volume-snap-000.img")

	c.Assert(len(r.diskData), Equals, 3)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-head-003.img"].Parent, Equals, "volume-snap-000.img")

	c.Assert(r.diskData["volume-head-003.img"].Created, Equals, revertTime1)

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 2)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-001.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-head-003.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-001.img"], IsNil)
	c.Assert(r.diskChildrenMap["volume-head-002.img"], IsNil)
	c.Assert(r.diskChildrenMap["volume-head-003.img"], IsNil)

	createdTime3 := getNow()
	err = r.Snapshot("003", true, createdTime3, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.diskData), Equals, 4)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-003.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-head-004.img"].Parent, Equals, "volume-snap-003.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 2)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-001.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-003.img"], Equals, true)
	c.Assert(len(r.diskChildrenMap["volume-snap-003.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-003.img"]["volume-head-004.img"], Equals, true)

	revertTime2 := getNow()
	r, err = r.Revert("volume-snap-001.img", revertTime2)
	c.Assert(err, IsNil)

	chain, err = r.Chain()
	c.Assert(err, IsNil)
	c.Assert(len(chain), Equals, 3)
	c.Assert(chain[0], Equals, "volume-head-005.img")
	c.Assert(chain[1], Equals, "volume-snap-001.img")
	c.Assert(chain[2], Equals, "volume-snap-000.img")

	c.Assert(len(r.diskData), Equals, 4)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-000.img"].UserCreated, Equals, true)
	c.Assert(r.diskData["volume-snap-000.img"].Created, Equals, createdTime0)
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-001.img"].UserCreated, Equals, true)
	c.Assert(r.diskData["volume-snap-001.img"].Created, Equals, createdTime1)
	c.Assert(r.diskData["volume-snap-003.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-003.img"].UserCreated, Equals, true)
	c.Assert(r.diskData["volume-snap-003.img"].Created, Equals, createdTime3)
	c.Assert(r.diskData["volume-head-005.img"].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.diskData["volume-head-005.img"].UserCreated, Equals, false)
	c.Assert(r.diskData["volume-head-005.img"].Created, Equals, revertTime2)

	disks := r.ListDisks()
	c.Assert(len(disks), Equals, 4)
	verifyDisks := map[string]bool{
		"volume-snap-000.img": false,
		"volume-snap-001.img": false,
		"volume-snap-003.img": false,
		"volume-head-005.img": false,
	}
	for name := range disks {
		value, exists := verifyDisks[name]
		c.Assert(exists, Equals, true)
		c.Assert(value, Equals, false)
		verifyDisks[name] = true
	}

	c.Assert(len(r.diskChildrenMap["volume-snap-001.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-001.img"]["volume-head-005.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-003.img"], IsNil)
}

func (s *TestSuite) TestRemoveLeafNode(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("002", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 5)
	c.Assert(len(r.volume.files), Equals, 5)

	c.Assert(r.info.Head, Equals, "volume-head-003.img")
	c.Assert(r.activeDiskData[4].Name, Equals, "volume-head-003.img")
	c.Assert(r.activeDiskData[4].Parent, Equals, "volume-snap-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-snap-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	c.Assert(len(r.diskData), Equals, 4)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-002.img"].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.diskData["volume-head-003.img"].Parent, Equals, "volume-snap-002.img")

	r, err = r.Revert("volume-snap-000.img", getNow())
	c.Assert(err, IsNil)

	c.Assert(len(r.diskData), Equals, 4)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-002.img"].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.diskData["volume-head-004.img"].Parent, Equals, "volume-snap-000.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 2)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-head-004.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-001.img"], Equals, true)
	c.Assert(len(r.diskChildrenMap["volume-snap-001.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-001.img"]["volume-snap-002.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-002.img"], IsNil)

	err = r.RemoveDiffDisk("volume-snap-002.img", false)
	c.Assert(err, IsNil)

	c.Assert(len(r.diskData), Equals, 3)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-head-004.img"].Parent, Equals, "volume-snap-000.img")

	c.Assert(r.diskChildrenMap["volume-snap-001.img"], IsNil)

	err = r.RemoveDiffDisk("volume-snap-001.img", false)
	c.Assert(err, IsNil)

	c.Assert(len(r.diskData), Equals, 2)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-head-004.img"].Parent, Equals, "volume-snap-000.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-head-004.img"], Equals, true)
}

func (s *TestSuite) TestRemoveLast(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-snap-000.img", false)
	c.Assert(err, IsNil)
	c.Assert(len(r.activeDiskData), Equals, 3)
	c.Assert(len(r.volume.files), Equals, 3)
	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	c.Assert(len(r.diskData), Equals, 2)
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-head-002.img"].Parent, Equals, "volume-snap-001.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-001.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-001.img"]["volume-head-002.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-head-002.img"], IsNil)
}

func (s *TestSuite) TestRemoveMiddle(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-snap-001.img", false)
	c.Assert(err, IsNil)
	c.Assert(len(r.activeDiskData), Equals, 3)
	c.Assert(len(r.volume.files), Equals, 3)
	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	c.Assert(len(r.diskData), Equals, 2)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-head-002.img"].Parent, Equals, "volume-snap-000.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-head-002.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-head-002.img"], IsNil)
}

func (s *TestSuite) TestRemoveFirst(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-head-002.img", false)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestRemoveOutOfChain(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("002", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 5)
	c.Assert(len(r.volume.files), Equals, 5)

	c.Assert(r.info.Head, Equals, "volume-head-003.img")
	c.Assert(r.activeDiskData[4].Name, Equals, "volume-head-003.img")
	c.Assert(r.activeDiskData[4].Parent, Equals, "volume-snap-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-snap-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	r, err = r.Revert("volume-snap-000.img", getNow())
	c.Assert(err, IsNil)
	c.Assert(len(r.activeDiskData), Equals, 3)
	c.Assert(len(r.volume.files), Equals, 3)
	c.Assert(r.info.Head, Equals, "volume-head-004.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-head-004.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	c.Assert(len(r.diskData), Equals, 4)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-head-004.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-001.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-002.img"].Parent, Equals, "volume-snap-001.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 2)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-head-004.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-001.img"], Equals, true)
	c.Assert(len(r.diskChildrenMap["volume-snap-001.img"]), Equals, 1)
	c.Assert(r.diskChildrenMap["volume-snap-001.img"]["volume-snap-002.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-002.img"], IsNil)

	err = r.RemoveDiffDisk("volume-snap-001.img", false)
	c.Assert(err, IsNil)
	c.Assert(len(r.activeDiskData), Equals, 3)
	c.Assert(len(r.volume.files), Equals, 3)
	c.Assert(r.info.Head, Equals, "volume-head-004.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-head-004.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	c.Assert(len(r.diskData), Equals, 3)
	c.Assert(r.diskData["volume-snap-000.img"].Parent, Equals, "")
	c.Assert(r.diskData["volume-head-004.img"].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.diskData["volume-snap-002.img"].Parent, Equals, "volume-snap-000.img")

	c.Assert(len(r.diskChildrenMap["volume-snap-000.img"]), Equals, 2)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-head-004.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-000.img"]["volume-snap-002.img"], Equals, true)
	c.Assert(r.diskChildrenMap["volume-snap-002.img"], IsNil)
}

func (s *TestSuite) TestPrepareRemove(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	/*
		volume-snap-000.img
		volume-snap-001.img
		volume-head-002.img
	*/

	err = r.MarkDiskAsRemoved("001")
	c.Assert(err, IsNil)
	c.Assert(r.activeDiskData[2].Removed, Equals, true)

	actions, err := r.PrepareRemoveDisk("001")
	c.Assert(err, IsNil)
	c.Assert(actions, HasLen, 1)

	err = r.MarkDiskAsRemoved("volume-snap-000.img")
	c.Assert(err, IsNil)
	c.Assert(r.activeDiskData[1].Removed, Equals, true)

	actions, err = r.PrepareRemoveDisk("volume-snap-000.img")
	c.Assert(err, IsNil)
	c.Assert(actions, HasLen, 2)
	c.Assert(actions[0].Action, Equals, OpCoalesce)
	c.Assert(actions[0].Source, Equals, "volume-snap-000.img")
	c.Assert(actions[0].Target, Equals, "volume-snap-001.img")
	c.Assert(actions[1].Action, Equals, OpReplace)
	c.Assert(actions[1].Source, Equals, "volume-snap-000.img")
	c.Assert(actions[1].Target, Equals, "volume-snap-001.img")

	err = r.Snapshot("002", true, now, nil)
	c.Assert(err, IsNil)

	/*
		volume-snap-000.img (r)
		volume-snap-001.img (r)
		volume-snap-002.img
		volume-head-003.img
	*/

	c.Assert(len(r.activeDiskData), Equals, 5)
	c.Assert(len(r.volume.files), Equals, 5)

	/* https://github.com/longhorn/longhorn-engine/pkg/issues/184 */
	err = r.MarkDiskAsRemoved("002")
	c.Assert(err, IsNil)
	c.Assert(r.activeDiskData[3].Removed, Equals, true)

	actions, err = r.PrepareRemoveDisk("002")
	c.Assert(err, IsNil)
	c.Assert(actions, HasLen, 1)

	err = r.Snapshot("003", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("004", true, now, nil)
	c.Assert(err, IsNil)

	/*
		volume-snap-000.img (r)
		volume-snap-001.img (r)
		volume-snap-002.img (r)
		volume-snap-003.img
		volume-snap-004.img
		volume-head-005.img
	*/

	c.Assert(len(r.activeDiskData), Equals, 7)
	c.Assert(len(r.volume.files), Equals, 7)

	err = r.MarkDiskAsRemoved("003")
	c.Assert(err, IsNil)
	c.Assert(r.activeDiskData[4].Removed, Equals, true)

	actions, err = r.PrepareRemoveDisk("002")
	c.Assert(err, IsNil)
	c.Assert(actions, HasLen, 2)
	c.Assert(actions[0].Action, Equals, OpCoalesce)
	c.Assert(actions[0].Source, Equals, "volume-snap-002.img")
	c.Assert(actions[0].Target, Equals, "volume-snap-003.img")
	c.Assert(actions[1].Action, Equals, OpReplace)
	c.Assert(actions[1].Source, Equals, "volume-snap-002.img")
	c.Assert(actions[1].Target, Equals, "volume-snap-003.img")

	actions, err = r.PrepareRemoveDisk("003")
	c.Assert(err, IsNil)
	c.Assert(actions, HasLen, 2)
	c.Assert(actions[0].Action, Equals, OpCoalesce)
	c.Assert(actions[0].Source, Equals, "volume-snap-003.img")
	c.Assert(actions[0].Target, Equals, "volume-snap-004.img")
	c.Assert(actions[1].Action, Equals, OpReplace)
	c.Assert(actions[1].Source, Equals, "volume-snap-003.img")
	c.Assert(actions[1].Target, Equals, "volume-snap-004.img")
}

func byteEquals(c *C, expected, obtained []byte) {
	c.Assert(len(expected), Equals, len(obtained))

	for i := range expected {
		l := fmt.Sprintf("%d=%x", i, expected[i])
		r := fmt.Sprintf("%d=%x", i, obtained[i])
		c.Assert(r, Equals, l)
	}
}

func md5Equals(c *C, expected, obtained []byte) {
	c.Assert(len(expected), Equals, len(obtained))

	expectedMd5 := md5.Sum(expected)
	obtainedMd5 := md5.Sum(obtained)
	for i := range expectedMd5 {
		l := fmt.Sprintf("%d=%x", i, expectedMd5[i])
		r := fmt.Sprintf("%d=%x", i, obtainedMd5[i])
		c.Assert(r, Equals, l)
	}
}

func fill(buf []byte, val byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = val
	}
}

func (s *TestSuite) TestRead(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9*b, b, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	buf := make([]byte, 3*b)
	_, err = r.ReadAt(buf, 0)
	c.Assert(err, IsNil)
	byteEquals(c, buf, make([]byte, 3*b))
}

func (s *TestSuite) TestWrite(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9*b, b, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	buf := make([]byte, 9*b)
	fill(buf, 1)
	_, err = r.WriteAt(buf, 0)
	c.Assert(err, IsNil)

	readBuf := make([]byte, 9*b)
	_, err = r.ReadAt(readBuf, 0)
	c.Assert(err, IsNil)

	byteEquals(c, readBuf, buf)
}

func (s *TestSuite) TestSnapshotReadWrite(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Logf("Volume: %s", dir)
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(3*b, b, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	buf := make([]byte, 3*b)
	fill(buf, 3)
	count, err := r.WriteAt(buf, 0)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 3*b)
	err = r.Snapshot("000", true, getNow(), nil)
	c.Assert(err, IsNil)

	fill(buf[b:2*b], 2)
	count, err = r.WriteAt(buf[b:2*b], b)
	c.Assert(count, Equals, b)
	err = r.Snapshot("001", true, getNow(), nil)
	c.Assert(err, IsNil)

	fill(buf[:b], 1)
	count, err = r.WriteAt(buf[:b], 0)
	c.Assert(count, Equals, b)
	err = r.Snapshot("002", true, getNow(), nil)
	c.Assert(err, IsNil)

	readBuf := make([]byte, 3*b)
	_, err = r.ReadAt(readBuf, 0)
	c.Logf("%v", r.volume.location)
	c.Assert(err, IsNil)
	byteEquals(c, readBuf, buf)
	byteEquals(c, r.volume.location, []byte{3, 2, 1})

	r, err = r.Reload()
	c.Assert(err, IsNil)

	_, err = r.ReadAt(readBuf, 0)
	c.Assert(err, IsNil)
	byteEquals(c, readBuf, buf)
	byteEquals(c, r.volume.location, []byte{3, 2, 1})
}

func (s *TestSuite) TestBackingFile(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Logf("Volume: %s", dir)
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	buf := make([]byte, 3*b)
	fill(buf, 3)

	f, err := NewTestBackingFile(path.Join(dir, "backing"))
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write(buf)
	c.Assert(err, IsNil)

	backing := &backingfile.BackingFile{
		Path: "backing",
		Disk: f,
	}

	r, err := New(3*b, b, dir, backing, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	chain, err := r.Chain()
	c.Assert(err, IsNil)
	c.Assert(len(chain), Equals, 1)
	c.Assert(chain[0], Equals, "volume-head-000.img")

	newBuf := make([]byte, 1*b)
	_, err = r.WriteAt(newBuf, b)
	c.Assert(err, IsNil)

	newBuf2 := make([]byte, 3*b)
	fill(newBuf2, 3)
	fill(newBuf2[b:2*b], 0)

	_, err = r.ReadAt(buf, 0)
	c.Assert(err, IsNil)

	byteEquals(c, buf, newBuf2)
}

func (s *TestSuite) partialWriteRead(c *C, totalLength, writeLength, writeOffset int64) {
	fmt.Println("Starting partialWriteRead")
	dir, err := os.MkdirTemp("", "replica")
	c.Logf("Volume: %s", dir)
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	buf := make([]byte, totalLength)
	fill(buf, 3)

	r, err := New(totalLength, b, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	_, err = r.WriteAt(buf, 0)
	c.Assert(err, IsNil)

	err = r.Snapshot("000", true, getNow(), nil)
	c.Assert(err, IsNil)

	buf = make([]byte, writeLength)
	fill(buf, 1)

	_, err = r.WriteAt(buf, writeOffset)
	c.Assert(err, IsNil)

	buf = make([]byte, totalLength)
	_, err = r.ReadAt(buf, 0)
	c.Assert(err, IsNil)

	expected := make([]byte, totalLength)
	fill(expected, 3)
	fill(expected[writeOffset:writeOffset+writeLength], 1)

	byteEquals(c, expected, buf)
}

func (s *TestSuite) TestPartialWriteRead(c *C) {
	s.partialWriteRead(c, 3*b, 3*bs, 2*bs)
	s.partialWriteRead(c, 3*b, 3*bs, 21*bs)

	s.partialWriteRead(c, 3*b, 11*bs, 7*bs)
	s.partialWriteRead(c, 4*b, 19*bs, 7*bs)

	s.partialWriteRead(c, 3*b, 19*bs, 5*bs)
	s.partialWriteRead(c, 3*b, 19*bs, 0*bs)
}

func (s *TestSuite) testPartialRead(c *C, totalLength int64, readBuf []byte, offset int64) (int, error) {
	fmt.Println("Filling data for partialRead")
	dir, err := os.MkdirTemp("", "replica")
	fmt.Printf("Volume: %s\n", dir)
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	buf := make([]byte, totalLength)
	fill(buf, 3)

	r, err := New(totalLength, b, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	for i := int64(0); i < totalLength; i += b {
		buf := make([]byte, totalLength-i)
		fill(buf, byte(i/b+1))
		err := r.Snapshot(strconv.Itoa(int(i)), true, getNow(), nil)
		c.Assert(err, IsNil)
		_, err = r.WriteAt(buf, i)
		c.Assert(err, IsNil)
	}

	fmt.Println("Starting partialRead", r.volume.location)
	return r.ReadAt(readBuf, offset)
}

func (s *TestSuite) TestPartialRead(c *C) {
	buf := make([]byte, b)
	_, err := s.testPartialRead(c, 3*b, buf, b/2)
	c.Assert(err, IsNil)

	expected := make([]byte, b)
	fill(expected[:b/2], 1)
	fill(expected[b/2:], 2)

	byteEquals(c, expected, buf)
}

func (s *TestSuite) TestPartialReadZeroStartOffset(c *C) {
	buf := make([]byte, b+b/2)
	_, err := s.testPartialRead(c, 3*b, buf, 0)
	c.Assert(err, IsNil)

	expected := make([]byte, b+b/2)
	fill(expected[:b], 1)
	fill(expected[b:], 2)

	byteEquals(c, expected, buf)
}

func (s *TestSuite) TestPartialFullRead(c *C) {
	// Sanity test that filling data works right
	buf := make([]byte, 2*b)
	_, err := s.testPartialRead(c, 2*b, buf, 0)
	c.Assert(err, IsNil)

	expected := make([]byte, 2*b)
	fill(expected[:b], 1)
	fill(expected[b:], 2)

	byteEquals(c, expected, buf)
}

func (s *TestSuite) TestPartialReadZeroEndOffset(c *C) {
	buf := make([]byte, b+b/2)
	_, err := s.testPartialRead(c, 2*b, buf, b/2)
	c.Assert(err, IsNil)

	expected := make([]byte, b+b/2)
	fill(expected[:b/2], 1)
	fill(expected[b/2:], 2)

	byteEquals(c, expected, buf)
}

func (s *TestSuite) TestForceRemoveDiffDisk(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, false)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001a", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001a.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001a.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-head-002.img", true)
	c.Assert(err, NotNil)

	revertTime := getNow()
	r, err = r.Revert("volume-snap-000.img", revertTime)
	c.Assert(err, IsNil)

	err = r.Snapshot("001b", true, now, nil)
	c.Assert(err, IsNil)

	err = r.RemoveDiffDisk("volume-snap-000.img", false)
	c.Assert(err, NotNil)

	err = r.RemoveDiffDisk("volume-snap-000.img", true)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestUnmapMarkDiskRemoved(c *C) {
	dir, err := os.MkdirTemp("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil, false, true)
	c.Assert(err, IsNil)
	defer r.Close()

	now := getNow()
	err = r.Snapshot("000", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("001a", true, now, nil)
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001a.img")
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001a.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	revertTime := getNow()
	r, err = r.Revert("volume-snap-000.img", revertTime)
	c.Assert(err, IsNil)

	err = r.Snapshot("001b", true, now, nil)
	c.Assert(err, IsNil)

	err = r.Snapshot("002b", true, now, nil)
	c.Assert(err, IsNil)

	_, err = r.UnmapAt(0, 0)
	c.Assert(err, IsNil)

	c.Assert(r.activeDiskData[3].Name, Equals, "volume-snap-002b.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001b.img")
	c.Assert(r.activeDiskData[3].Removed, Equals, true)
	c.Assert(r.activeDiskData[2].Name, Equals, "volume-snap-001b.img")
	c.Assert(r.activeDiskData[2].Removed, Equals, true)
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Removed, Equals, false)
	c.Assert(r.activeDiskData[1].Parent, Equals, "")
}
