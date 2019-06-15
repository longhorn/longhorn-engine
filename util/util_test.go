package util

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestBitmap(c *C) {
	bm := NewBitmap(100, 200)

	start, end, err := bm.AllocateRange(100)
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(100))
	c.Assert(end, Equals, int32(199))

	start, end, err = bm.AllocateRange(1)
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(200))
	c.Assert(end, Equals, int32(200))

	_, _, err = bm.AllocateRange(1)
	c.Assert(err, NotNil)

	err = bm.ReleaseRange(100, 100)
	c.Assert(err, IsNil)

	start, end, err = bm.AllocateRange(1)
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(100))
	c.Assert(end, Equals, int32(100))

	err = bm.ReleaseRange(105, 120)
	c.Assert(err, IsNil)

	start, end, err = bm.AllocateRange(15)
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(105))
	c.Assert(end, Equals, int32(119))

	_, _, err = bm.AllocateRange(2)
	c.Assert(err, NotNil)

	start, end, err = bm.AllocateRange(1)
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(120))
	c.Assert(end, Equals, int32(120))
}
