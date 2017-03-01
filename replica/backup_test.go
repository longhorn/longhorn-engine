package replica

import (
	"io/ioutil"
	"os"
	"path"

	. "gopkg.in/check.v1"
)

const (
	mb = 1 << 20
)

func (s *TestSuite) TestBackup(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	err = os.Chdir(dir)
	c.Assert(err, IsNil)

	r, err := New(10*mb, bs, dir, nil)
	c.Assert(err, IsNil)
	defer r.Close()

	buf := make([]byte, 2*mb)
	fill(buf, 1)
	_, err = r.WriteAt(buf, mb)
	c.Assert(err, IsNil)

	chain, err := r.Chain()
	c.Assert(err, IsNil)

	rb := NewBackup(nil)
	volume := "test"
	err = rb.OpenSnapshot(chain[0], volume)
	c.Assert(err, IsNil)

	mappings, err := rb.CompareSnapshot(chain[0], "", volume)
	c.Assert(err, IsNil)
	c.Assert(len(mappings.Mappings), Equals, 2)
	c.Assert(mappings.BlockSize, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[0].Offset, Equals, int64(0))
	c.Assert(mappings.Mappings[0].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Offset, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Size, Equals, int64(2*mb))
}

func (s *TestSuite) TestBackupWithBackups(c *C) {
	s.testBackupWithBackups(c, nil)
}

func (s *TestSuite) TestBackupWithBackupsAndBacking(c *C) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	f, err := os.Create(path.Join(dir, "backing"))
	c.Assert(err, IsNil)
	defer f.Close()

	buf := make([]byte, 10*mb)
	fill(buf, 9)

	_, err = f.Write(buf)
	c.Assert(err, IsNil)

	backing := &BackingFile{
		Name: "backing",
		Disk: f,
	}

	s.testBackupWithBackups(c, backing)
}

func (s *TestSuite) testBackupWithBackups(c *C, backingFile *BackingFile) {
	dir, err := ioutil.TempDir("", "replica")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	err = os.Chdir(dir)
	c.Assert(err, IsNil)
	volume := "test"

	r, err := New(10*mb, bs, dir, backingFile)
	c.Assert(err, IsNil)
	defer r.Close()

	// Write layout as follows
	//               0 1 2 3 4 5 6 7 8 9 mb
	// chain[0] head 4 4 4 4 4 4 4 4 4 4
	// chain[1] 003          3 3     3 3
	// chain[2] 002  2 2     2 2
	// chain[3] 001    1 1
	// chain[4] back 9 9 9 9 9 9 9 9 9 9
	buf := make([]byte, 2*mb)
	fill(buf, 1)
	_, err = r.WriteAt(buf, mb)
	c.Assert(err, IsNil)
	err = r.Snapshot("001", true)

	c.Assert(err, IsNil)
	fill(buf, 2)
	_, err = r.WriteAt(buf, 0)
	c.Assert(err, IsNil)
	_, err = r.WriteAt(buf, 4*mb)
	c.Assert(err, IsNil)
	err = r.Snapshot("002", true)

	c.Assert(err, IsNil)
	fill(buf, 3)
	_, err = r.WriteAt(buf, 4*mb)
	c.Assert(err, IsNil)
	_, err = r.WriteAt(buf, 8*mb)
	c.Assert(err, IsNil)
	err = r.Snapshot("003", true)

	c.Assert(err, IsNil)
	buf = make([]byte, 10*mb)
	fill(buf, 4)
	_, err = r.WriteAt(buf, 0)
	c.Assert(err, IsNil)

	chain, err := r.Chain()

	rb := NewBackup(backingFile)

	// Test 003 -> ""
	err = rb.OpenSnapshot(chain[1], volume)
	c.Assert(err, IsNil)

	// Test read 003
	expected := make([]byte, 10*mb)
	readBuf := make([]byte, 10*mb)
	fill(expected[:2*mb], 2)
	fill(expected[2*mb:3*mb], 1)
	fill(expected[4*mb:6*mb], 3)
	fill(expected[8*mb:10*mb], 3)
	if backingFile != nil {
		fill(expected[3*mb:4*mb], 9)
		fill(expected[6*mb:8*mb], 9)
	}
	err = rb.ReadSnapshot(chain[1], volume, 0, readBuf)
	c.Assert(err, IsNil)
	md5Equals(c, readBuf, expected)

	mappings, err := rb.CompareSnapshot(chain[1], "", volume)
	c.Assert(err, IsNil)
	c.Assert(len(mappings.Mappings), Equals, 4)
	c.Assert(mappings.BlockSize, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[0].Offset, Equals, int64(0))
	c.Assert(mappings.Mappings[0].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Offset, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[2].Offset, Equals, int64(4*mb))
	c.Assert(mappings.Mappings[2].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[3].Offset, Equals, int64(8*mb))
	c.Assert(mappings.Mappings[3].Size, Equals, int64(2*mb))

	err = rb.CloseSnapshot(chain[1], volume)
	c.Assert(err, IsNil)

	// Test 003 -> 002
	err = rb.OpenSnapshot(chain[1], volume)
	mappings, err = rb.CompareSnapshot(chain[1], chain[2], volume)
	c.Assert(err, IsNil)
	c.Assert(len(mappings.Mappings), Equals, 2)
	c.Assert(mappings.BlockSize, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[0].Offset, Equals, int64(4*mb))
	c.Assert(mappings.Mappings[0].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Offset, Equals, int64(8*mb))
	c.Assert(mappings.Mappings[1].Size, Equals, int64(2*mb))
	err = rb.CloseSnapshot(chain[1], volume)

	// Test 002 -> 001
	err = rb.OpenSnapshot(chain[2], volume)
	c.Assert(err, IsNil)

	// Test read 002
	expected = make([]byte, 10*mb)
	readBuf = make([]byte, 10*mb)
	fill(expected[:2*mb], 2)
	fill(expected[2*mb:3*mb], 1)
	fill(expected[4*mb:6*mb], 2)
	if backingFile != nil {
		fill(expected[3*mb:4*mb], 9)
		fill(expected[6*mb:10*mb], 9)
	}
	err = rb.ReadSnapshot(chain[2], volume, 0, readBuf)
	c.Assert(err, IsNil)
	md5Equals(c, readBuf, expected)

	mappings, err = rb.CompareSnapshot(chain[2], chain[3], volume)
	c.Assert(err, IsNil)
	c.Assert(len(mappings.Mappings), Equals, 2)
	c.Assert(mappings.BlockSize, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[0].Offset, Equals, int64(0*mb))
	c.Assert(mappings.Mappings[0].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Offset, Equals, int64(4*mb))
	c.Assert(mappings.Mappings[1].Size, Equals, int64(2*mb))
	err = rb.CloseSnapshot(chain[2], volume)
	c.Assert(err, IsNil)

	// Test 002 -> ""
	err = rb.OpenSnapshot(chain[2], volume)
	c.Assert(err, IsNil)
	mappings, err = rb.CompareSnapshot(chain[2], "", volume)
	c.Assert(err, IsNil)
	c.Assert(len(mappings.Mappings), Equals, 3)
	c.Assert(mappings.BlockSize, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[0].Offset, Equals, int64(0*mb))
	c.Assert(mappings.Mappings[0].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Offset, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[2].Offset, Equals, int64(4*mb))
	c.Assert(mappings.Mappings[2].Size, Equals, int64(2*mb))
	err = rb.CloseSnapshot(chain[2], volume)
	c.Assert(err, IsNil)

	// Test 001 -> ""
	err = rb.OpenSnapshot(chain[3], volume)
	c.Assert(err, IsNil)

	// Test read 001
	expected = make([]byte, 10*mb)
	readBuf = make([]byte, 10*mb)
	fill(expected[mb:3*mb], 1)
	if backingFile != nil {
		fill(expected[:mb], 9)
		fill(expected[3*mb:10*mb], 9)
	}
	err = rb.ReadSnapshot(chain[3], volume, 0, readBuf)
	c.Assert(err, IsNil)
	md5Equals(c, readBuf, expected)

	mappings, err = rb.CompareSnapshot(chain[3], "", volume)
	c.Assert(err, IsNil)
	c.Assert(len(mappings.Mappings), Equals, 2)
	c.Assert(mappings.BlockSize, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[0].Offset, Equals, int64(0*mb))
	c.Assert(mappings.Mappings[0].Size, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Offset, Equals, int64(2*mb))
	c.Assert(mappings.Mappings[1].Size, Equals, int64(2*mb))
	err = rb.CloseSnapshot(chain[3], volume)
	c.Assert(err, IsNil)
}
