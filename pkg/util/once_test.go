package util

import (
	"fmt"
	. "gopkg.in/check.v1"
	"sync"
)

func (s *TestSuite) TestOnce(c *C) {
	var counter int
	var once Once
	f := func() error {
		if counter == 0 {
			// this is safe since we are inside of a lock while executing f
			// and f is the only thing accessing counter
			counter++
			return fmt.Errorf("first call of once failed")
		}
		return nil
	}

	err := once.Do(f)
	c.Assert(err, NotNil)

	err = once.Do(f)
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Assert(once.Do(f), IsNil)
			c.Assert(counter, Equals, 1)
		}()
	}
	wg.Wait()
}
