package broadcaster

import (
	"container/list"
	"sync"

	"github.com/sirupsen/logrus"
)

// Broadcaster is able to receive and pass runtime.Object to multiple Watchers,
// if they're watching for the given event
type Broadcaster struct {
	sync.Mutex
	watchers map[string]*list.List
}

type Event struct {
	Type string
	Data interface{}
}

type Watcher struct {
	eventChan chan *Event
	removed   bool
}

func (b *Broadcaster) NewWatcher(events ...string) *Watcher {
	b.Lock()
	defer b.Unlock()

	if b.watchers == nil {
		b.watchers = make(map[string]*list.List)
	}
	w := &Watcher{
		eventChan: make(chan *Event, 1000),
		removed:   false,
	}
	for _, e := range events {
		if b.watchers[e] == nil {
			b.watchers[e] = list.New()
		}
		l := b.watchers[e]
		l.PushBack(w)
		b.watchers[e] = l
	}
	return w
}

func (b *Broadcaster) Notify(e *Event) {
	b.Lock()
	defer b.Unlock()

	l, exists := b.watchers[e.Type]
	if !exists {
		// no one is watching for the event
		return
	}

	curr := l.Front()
	for curr != nil {
		next := curr.Next()

		w, ok := curr.Value.(*Watcher)
		if !ok {
			logrus.Errorf("BUG: stored non-watcher in the list")
			continue
		}
		if w.removed {
			l.Remove(curr)
		} else {
			tmpe := e
			select {
			case w.eventChan <- tmpe:
			default:
				logrus.Errorf("channel of watcher for %v is full, going to drop event, disconnect the watcher", e.Type)
				w.Close()
			}
		}
		curr = next
	}
}

func (w *Watcher) Events() chan *Event {
	return w.eventChan
}

func (w *Watcher) Close() {
	w.removed = true
	close(w.eventChan)
}

func NewSimpleEvent(eventType string) *Event {
	return &Event{
		Type: eventType,
		Data: struct{}{},
	}
}
