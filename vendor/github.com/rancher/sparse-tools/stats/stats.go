package stats

import (
	"fmt"
	"time"

	"errors"

	"sync"

	"github.com/rancher/sparse-tools/log"
)

const (
	defaultBufferSize = 100 * 1000 // sample buffer size (cyclic)
)

//SampleOp operation
type SampleOp int

const (
	// OpNone unitialized operation
	OpNone SampleOp = iota
	// OpRead read from replica
	OpRead
	// OpWrite write to replica
	OpWrite
	// OpPing ping replica
	OpPing
)

type dataPoint struct {
	target    int // index in targetIDs (e.g replica index)
	op        SampleOp
	timestamp time.Time
	duration  time.Duration
	size      int // i/o operation size
	status    bool
}

// Minimize space by storing targetID indices in the dataPoint
var (
	targetIDs   = make([]string, 0, 128)
	targetMap   = make(map[string]int, 128)
	targetMutex sync.RWMutex
)

func targetIndex(targetID string) int {
	targetMutex.RLock()
	target, exists := targetMap[targetID]
	targetMutex.RUnlock()
	if exists {
		return target
	}

	// register the ID
	targetMutex.Lock()
	defer targetMutex.Unlock()

	target, exists = targetMap[targetID]
	if exists {
		return target
	}

	target = len(targetMap)
	targetMap[targetID] = target
	targetIDs = append(targetIDs, targetID)
	return target
}

func targetID(i int) string {
	targetMutex.RLock()
	defer targetMutex.RUnlock()
	return targetIDs[i]
}

// String conversions
func (op SampleOp) String() string {
	switch op {
	case OpRead:
		return "R"
	case OpWrite:
		return "W"
	case OpPing:
		return "P"
	}
	return "<unknown op>"
}

func (sample dataPoint) String() string {
	target := targetID(sample.target)
	if sample.duration != time.Duration(0) {
		if sample.status {
			return fmt.Sprintf("%s: ->%s %v[%3dkB] %8dus", sample.timestamp.Format(time.StampMicro), target, sample.op, sample.size/1024, sample.duration.Nanoseconds()/1000)
		}
		return fmt.Sprintf("%s: ->%s %v[%3dkB] %8dus failed", sample.timestamp.Format(time.StampMicro), target, sample.op, sample.size/1024, sample.duration.Nanoseconds()/1000)
	}
	return fmt.Sprintf("%s: ->%s %v[%3dkB] pending", sample.timestamp.Format(time.StampMicro), target, sample.op, sample.size/1024)
}

var (
	bufferSize = defaultBufferSize
	cdata      = make(chan dataPoint, defaultBufferSize)
)

func initStats(size int) {
	bufferSize = size
	cdata = make(chan dataPoint, bufferSize)
	log.Debug("Stats.init=", size)
}

func init() {
	initStats(bufferSize)
}

func storeSample(sample dataPoint) {
	//Maintain non-blocking state
	for {
		select {
		case cdata <- sample:
			return
		default:
			<-cdata
		}
	}
}

// Sample to the cyclic buffer
func Sample(timestamp time.Time, duration time.Duration, targetID string, op SampleOp, size int, status bool) {
	storeSample(dataPoint{targetIndex(targetID), op, timestamp, duration, size, status})
}

// Process unreported samples
func Process(processor func(dataPoint)) chan struct{} {
	return ProcessLimited(0 /*no limit*/, processor)
}

// ProcessLimited number of unreported samples is restricted by specified limit, the rest os droppped
func ProcessLimited(limit int, processor func(dataPoint)) chan struct{} {
	// Fetch unreported window
	done := make(chan struct{})
	dropCount := 0
	if limit > 0 {
		count := len(cdata)
		if count > limit {
			// drop old samples to satisfy the limit
			dropCount = count - limit
		}
	}

	go func(dropCount, limit int, pending []dataPoint, done chan struct{}) {
	samples:
		for count := 0; limit == 0 || count < limit; {
			select {
			case sample := <-cdata:
				log.Debug("Stats.Processing=", sample)
				if dropCount > 0 {
					// skip old samples
					dropCount--
					break
				}
				processor(sample)
				count++
			default:
				break samples
			}
		}
		for _, sample := range pending {
			log.Debug("Stats.Processing pending=", sample)
			processor(sample)
		}
		close(done)
	}(dropCount, limit, getPendingOps(), done)

	return done
}

func printSample(sample dataPoint) {
	fmt.Println(sample)
}

// Print samples
func Print() chan struct{} {
	return Process(printSample)
}

// PrintLimited samples
func PrintLimited(limit int) chan struct{} {
	return ProcessLimited(limit, printSample)
}

// Test helper to exercise small buffer sizes
func resetStats(size int) {
	log.Debug("Stats.reset")
	initStats(size)
}

//OpID pending operation id
type OpID int

var (
	pendingOps         = make([]dataPoint, 8, 128)
	pendingOpsFreeSlot = make([]int, 0, 128) // stack of recently freed IDs
	mutexPendingOps    sync.Mutex
)

//InsertPendingOp starts tracking of a pending operation
func InsertPendingOp(timestamp time.Time, targetID string, op SampleOp, size int) OpID {
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	var id int
	if len(pendingOpsFreeSlot) > 0 {
		//reuse recently freed id
		id = pendingOpsFreeSlot[len(pendingOpsFreeSlot)-1]
		pendingOpsFreeSlot = pendingOpsFreeSlot[:len(pendingOpsFreeSlot)-1]
	} else {
		id = pendingOpEmptySlot()
	}
	pendingOps[id] = dataPoint{targetIndex(targetID), op, timestamp, 0, size, false}
	log.Debug("InsertPendingOp id=", id)
	return OpID(id)
}

//RemovePendingOp removes tracking of a completed operation
func RemovePendingOp(id OpID, status bool) error {
	log.Debug("RemovePendingOp id=", id)
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	i := int(id)
	if i < 0 || i >= len(pendingOps) {
		errMsg := "RemovePendingOp: Invalid OpID"
		log.Error(errMsg, i)
		return errors.New(errMsg)
	}
	if pendingOps[i].op == OpNone {
		errMsg := "RemovePendingOp: OpID already removed"
		log.Error(errMsg, i)
		return errors.New(errMsg)
	}

	// Update the duration and store in the recent stats
	pendingOps[i].duration = time.Now().Sub(pendingOps[i].timestamp)
	pendingOps[i].status = status
	storeSample(pendingOps[i])

	//Remove from pending
	pendingOps[i].op = OpNone
	//Stack freed id for quick reuse
	pendingOpsFreeSlot = append(pendingOpsFreeSlot, i)
	return nil
}

func pendingOpEmptySlot() int {
	for i, op := range pendingOps {
		if op.op == OpNone {
			return i
		}
	}
	pendingOps = append(pendingOps, dataPoint{})
	return len(pendingOps) - 1
}

func getPendingOps() []dataPoint {
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	ops := make([]dataPoint, 0, len(pendingOps))
	for _, op := range pendingOps {
		if op.op != OpNone {
			ops = append(ops, op)
		}
	}
	return ops
}
