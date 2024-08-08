package util

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	c.Assert(err, IsNil)
	c.Assert(len(lm), Equals, 2)

	lm, err = ParseLabels(input4)
	c.Assert(err, IsNil)
	c.Assert(len(lm), Equals, 1)
	c.Assert(lm["name"], Equals, "456")
}

func createTempDir(c *C) string {
	dir, err := os.MkdirTemp("", "test")
	c.Assert(err, IsNil)
	return dir
}

func touchFile(c *C, path string) {
	f, err := os.Create(path)
	c.Assert(err, IsNil)
	f.Close()
}

func (s *TestSuite) TestResolveFilepathNoOp(c *C) {
	dirpath := createTempDir(c)
	f := filepath.Join(dirpath, "test")
	touchFile(c, f)

	f2, err := ResolveBackingFilepath(f)
	c.Assert(err, IsNil)
	c.Assert(f, Equals, f2)
}

func (s *TestSuite) TestResolveFilepathFromDirectory(c *C) {
	dirpath := createTempDir(c)
	f := filepath.Join(dirpath, "test")
	touchFile(c, f)

	f2, err := ResolveBackingFilepath(dirpath)
	c.Assert(err, IsNil)
	c.Assert(f, Equals, f2)
}

func (s *TestSuite) TestResolveFilepathTooManyFiles(c *C) {
	dirpath := createTempDir(c)
	f := filepath.Join(dirpath, "test")
	touchFile(c, f)
	f2 := filepath.Join(dirpath, "test2")
	touchFile(c, f2)

	_, err := ResolveBackingFilepath(dirpath)
	c.Assert(err, ErrorMatches, ".*found 2 files.*")
}

func (s *TestSuite) TestResolveFilepathSubdirectory(c *C) {
	dirpath := createTempDir(c)
	err := os.Mkdir(filepath.Join(dirpath, "test2"), 0777)
	c.Assert(err, IsNil)

	_, err = ResolveBackingFilepath(dirpath)
	c.Assert(err, ErrorMatches, ".*found a subdirectory")
}

func (s *TestSuite) TestSharedTimeouts(c *C) {
	shortTimeout := 8 * time.Second
	longTimeout := 16 * time.Second
	sharedTimeouts := NewSharedTimeouts(shortTimeout, longTimeout)

	// Increment the SharedTimeouts in multiple goroutines.
	wg := new(sync.WaitGroup)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			sharedTimeouts.Increment()
			wg.Done()
		}()
	}
	wg.Wait()
	c.Assert(sharedTimeouts.numConsumers, Equals, 3)

	// CheckAndDecrement with a duration smaller than shortTimeout.
	exceededTimeout := sharedTimeouts.CheckAndDecrement(4 * time.Second)
	c.Assert(exceededTimeout, Equals, time.Duration(0))
	c.Assert(sharedTimeouts.numConsumers, Equals, 3)

	// Decrement the SharedTimeouts.
	sharedTimeouts.Decrement()
	c.Assert(sharedTimeouts.numConsumers, Equals, 2)

	// Simultaneously CheckAndDecrement a duration larger than shortTimeout but smaller than longTimeout. One goroutine
	// "exceeds" the timeout. The other does not.
	numExceeded := &atomic.Int64{}
	timeoutExceeded := &atomic.Int64{}
	wg = new(sync.WaitGroup)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			exceededTimeout := sharedTimeouts.CheckAndDecrement(12 * time.Second)
			fmt.Println(exceededTimeout)
			if exceededTimeout > time.Duration(0) {
				fmt.Println(exceededTimeout)
				numExceeded.Add(1)
				timeoutExceeded.Store(int64(exceededTimeout))
			}
			wg.Done()
		}()
	}
	wg.Wait()
	c.Assert(int(numExceeded.Load()), Equals, 1)
	c.Assert(time.Duration(timeoutExceeded.Load()), Equals, shortTimeout)
	c.Assert(sharedTimeouts.numConsumers, Equals, 1)

	// CheckAndDecrement with a duration larger than longTimeout.
	exceededTimeout = sharedTimeouts.CheckAndDecrement(20 * time.Second)
	c.Assert(exceededTimeout, Equals, longTimeout)
	c.Assert(sharedTimeouts.numConsumers, Equals, 0)
}
