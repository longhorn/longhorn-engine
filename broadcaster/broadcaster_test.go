package broadcaster

import (
	"sync"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestBroadcaster(c *C) {
	b := Broadcaster{}

	w1 := b.NewWatcher("eventWithString", "eventWithMap")
	w2 := b.NewWatcher("eventWithMap")
	w3 := b.NewWatcher("eventWithString")

	data1 := "data1"
	e1 := &Event{
		Type: "eventWithString",
		Data: data1,
	}

	data2 := map[string]bool{"data2": true}
	e2 := &Event{
		Type: "eventWithMap",
		Data: data2,
	}

	wg := &sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()
		event := <-w1.Events()
		c.Assert(event.Data, DeepEquals, data1)
		event = <-w1.Events()
		c.Assert(event.Data, DeepEquals, data2)
	}()

	go func() {
		defer wg.Done()
		event := <-w2.Events()
		c.Assert(event.Data, DeepEquals, data2)
	}()

	go func() {
		defer wg.Done()
		event := <-w3.Events()
		c.Assert(event.Data, DeepEquals, data1)
	}()

	b.Notify(e1)
	b.Notify(e2)

	wg.Wait()

	w1.Close()

	wg.Add(3)
	go func() {
		defer wg.Done()
		_, ok := <-w1.Events()
		c.Assert(ok, Equals, false)
	}()

	go func() {
		defer wg.Done()
		event := <-w2.Events()
		c.Assert(event.Data, DeepEquals, data2)
	}()

	go func() {
		defer wg.Done()
		event := <-w3.Events()
		c.Assert(event.Data, DeepEquals, data1)
	}()

	b.Notify(e1)
	b.Notify(e2)

	wg.Wait()
}
