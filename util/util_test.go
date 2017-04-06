package util

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestParseLabels(c *C) {
	input0 := []string{}
	input1 := []string{"test=123", "name=456"}
	input2 := []string{"1?x=23=234", "name=456"}
	input3 := []string{"test23=2?34", "name=456"}
	input4 := []string{"name=456"}

	lm, err := ParseLabels(input0)
	c.Assert(err, IsNil)
	c.Assert(len(lm), Equals, 0)

	lm, err = ParseLabels(input1)
	c.Assert(err, IsNil)
	c.Assert(len(lm), Equals, 2)
	c.Assert(lm["test"], Equals, "123")
	c.Assert(lm["name"], Equals, "456")

	lm, err = ParseLabels(input2)
	c.Assert(err, NotNil)
	c.Assert(len(lm), Equals, 0)

	lm, err = ParseLabels(input3)
	c.Assert(err, NotNil)
	c.Assert(len(lm), Equals, 0)

	lm, err = ParseLabels(input4)
	c.Assert(err, IsNil)
	c.Assert(len(lm), Equals, 1)
	c.Assert(lm["name"], Equals, "456")
}
