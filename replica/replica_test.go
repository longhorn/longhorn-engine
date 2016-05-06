package replica

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "gopkg.in/check.v1"
)

const (
	b = 4096
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestCreate(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()
}

func (s *TestSuite) TestSnapshot(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	err = r.Snapshot("000")
	c.Assert(err, IsNil)

	err = r.Snapshot("001")
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")
}

func (s *TestSuite) TestRemoveLast(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	err = r.Snapshot("000")
	c.Assert(err, IsNil)

	err = r.Snapshot("001")
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-snap-000.img")
	c.Assert(err, IsNil)
	c.Assert(len(r.activeDiskData), Equals, 3)
	c.Assert(len(r.volume.files), Equals, 3)
	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[1].name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")
}

func (s *TestSuite) TestRemoveMiddle(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	err = r.Snapshot("000")
	c.Assert(err, IsNil)

	err = r.Snapshot("001")
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-snap-001.img")
	c.Assert(err, IsNil)
	c.Assert(len(r.activeDiskData), Equals, 3)
	c.Assert(len(r.volume.files), Equals, 3)
	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")
}

func (s *TestSuite) TestRemoveFirst(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9, 3, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	err = r.Snapshot("000")
	c.Assert(err, IsNil)

	err = r.Snapshot("001")
	c.Assert(err, IsNil)

	c.Assert(len(r.activeDiskData), Equals, 4)
	c.Assert(len(r.volume.files), Equals, 4)

	c.Assert(r.info.Head, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].name, Equals, "volume-head-002.img")
	c.Assert(r.activeDiskData[3].Parent, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].name, Equals, "volume-snap-001.img")
	c.Assert(r.activeDiskData[2].Parent, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].name, Equals, "volume-snap-000.img")
	c.Assert(r.activeDiskData[1].Parent, Equals, "")

	err = r.RemoveDiffDisk("volume-head-002.img")
	c.Assert(err, Not(IsNil))
}

func byteEquals(c *C, left, right []byte) {
	c.Assert(len(left), Equals, len(right))

	for i := range left {
		l := fmt.Sprintf("%d=%x", i, left[i])
		r := fmt.Sprintf("%d=%x", i, right[i])
		c.Assert(l, Equals, r)
	}
}

func fill(buf []byte, val byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = val
	}
}

func (s *TestSuite) TestRead(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9*b, b, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	buf := make([]byte, 3*b)
	_, err = r.ReadAt(buf, 0)
	c.Assert(err, IsNil)
	byteEquals(c, buf, make([]byte, 3*b))
}

func (s *TestSuite) TestWrite(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(9*b, b, dir, nil)
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
	dir, err := ioutil.TempDir("", "replica")
	c.Logf("Volume: %s", dir)
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	r, err := New(3*b, b, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	buf := make([]byte, 3*b)
	fill(buf, 3)
	count, err := r.WriteAt(buf, 0)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 3*b)
	err = r.Snapshot("000")
	c.Assert(err, IsNil)

	fill(buf[b:2*b], 2)
	count, err = r.WriteAt(buf[b:2*b], b)
	c.Assert(count, Equals, b)
	err = r.Snapshot("001")
	c.Assert(err, IsNil)

	fill(buf[:b], 1)
	count, err = r.WriteAt(buf[:b], 0)
	c.Assert(count, Equals, b)
	err = r.Snapshot("002")
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
	dir, err := ioutil.TempDir("", "replica")
	c.Logf("Volume: %s", dir)
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	buf := make([]byte, 3*b)
	fill(buf, 3)

	f, err := os.Create(path.Join(dir, "backing"))
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write(buf)
	c.Assert(err, IsNil)

	backing := &BackingFile{
		Name: "backing",
		Disk: f,
	}

	r, err := New(3*b, b, dir, backing)
	c.Assert(err, IsNil)
	defer r.Close()

	chain, err := r.Chain()
	c.Assert(err, IsNil)
	c.Assert(len(chain), Equals, 2)
	c.Assert(chain[0], Equals, "volume-head-000.img")
	c.Assert(chain[1], Equals, "backing")

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
