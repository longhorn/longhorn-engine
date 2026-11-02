package controller

import "sync"

// rangeLocker serializes operations on overlapping byte ranges [start, end).
type rangeLocker struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ranges map[*lockedRange]struct{}
}

type lockedRange struct {
	start, end int64
}

func newRangeLocker() *rangeLocker {
	r := &rangeLocker{ranges: map[*lockedRange]struct{}{}}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *rangeLocker) overlaps(start, end int64) bool {
	for l := range r.ranges {
		if start < l.end && l.start < end {
			return true
		}
	}
	return false
}

// Lock blocks until no held range overlaps [start, end), then holds it.
func (r *rangeLocker) Lock(start, end int64) *lockedRange {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.overlaps(start, end) {
		r.cond.Wait()
	}
	l := &lockedRange{start: start, end: end}
	r.ranges[l] = struct{}{}
	return l
}

func (r *rangeLocker) Unlock(l *lockedRange) {
	r.mu.Lock()
	delete(r.ranges, l)
	r.mu.Unlock()
	r.cond.Broadcast()
}
