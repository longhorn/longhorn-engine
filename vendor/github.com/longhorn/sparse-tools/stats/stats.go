package stats

import (
	"fmt"
	"time"

	"sync"

	"github.com/sirupsen/logrus"
)

const (
	defaultBufferSize = 4 * 1000 // sample buffer size (cyclic)
)

// SampleOp operation
type SampleOp int

const (
	// OpNone uninitialized operation
	OpNone SampleOp = iota
	// OpRead read from replica
	OpRead
	// OpWrite write to replica
	OpWrite
	// OpUnmap unmap for replica
	OpUnmap
	// OpPing ping replica
	OpPing
)

type dataPoint struct {
	target    int // index in targetIDs (e.g replica index)
	seq       uint32
	op        SampleOp
	timestamp time.Time
	duration  time.Duration
	offset    int
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
	if i < 0 || i >= len(targetIDs) {
		return "<unspecified target>"
	}
	return targetIDs[i]
}

// String conversions
func (op SampleOp) String() string {
	switch op {
	case OpRead:
		return "R"
	case OpWrite:
		return "W"
	case OpUnmap:
		return "U"
	case OpPing:
		return "P"
	}
	return "<unknown op>"
}

func (sample dataPoint) String() string {
	target := targetID(sample.target)
	if sample.duration != time.Duration(0) {
		if sample.status {
			return fmt.Sprintf("%s: REQ %v ->%s %v[%3dkB@%vkB] %8dus", sample.timestamp.Format(time.StampMicro), sample.seq, target, sample.op, sample.size/1024, sample.offset/1024, sample.duration.Nanoseconds()/1000)
		}
		return fmt.Sprintf("%s: REQ %v ->%s %v[%3dkB@%vkB] %8dus failed", sample.timestamp.Format(time.StampMicro), sample.seq, target, sample.op, sample.size/1024, sample.offset/1024, sample.duration.Nanoseconds()/1000)
	}
	return fmt.Sprintf("%s: REQ %v ->%s %v[%3dkB@%vkB] pending", sample.timestamp.Format(time.StampMicro), sample.seq, target, sample.op, sample.size/1024, sample.offset/1024)
}

var (
	bufferSize = defaultBufferSize
	cdata      = make(chan dataPoint, defaultBufferSize)
)

func initStats(size int) {
	bufferSize = size
	cdata = make(chan dataPoint, bufferSize)
	logrus.Debugf("Stats.init bufferSize=%v", bufferSize)
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
func Sample(timestamp time.Time, duration time.Duration, targetID string, seq uint32, op SampleOp, offset, size int, status bool) {
	storeSample(dataPoint{targetIndex(targetID), seq, op, timestamp, duration, offset, size, status})
}

// Process unreported samples
func Process(processor func(dataPoint)) chan struct{} {
	return ProcessLimited(0 /*no limit*/, processor)
}

// ProcessLimited number of unreported samples is restricted by specified limit, the rest os dropped
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
				logrus.Debugf("Stats.Processing=%+v", sample)
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
			logrus.Debugf("Stats.Processing pending=%+v", sample)
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
	logrus.Debug("Stats.reset")
	initStats(size)
}

// OpID pending operation id
type OpID int

var (
	pendingOps         = make([]dataPoint, 8, 128)
	pendingOpsFreeSlot = make([]int, 0, 128) // stack of recently freed IDs
	mutexPendingOps    sync.Mutex
)

// InsertPendingOp starts tracking of a pending operation
func InsertPendingOp(timestamp time.Time, targetID string, seq uint32, op SampleOp, offset, size int) OpID {
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
	pendingOps[id] = dataPoint{targetIndex(targetID), seq, op, timestamp, 0, offset, size, false}
	logrus.Debugf("InsertPendingOp OpID=%v", id)
	return OpID(id)
}

// RemovePendingOp removes tracking of a completed operation
func RemovePendingOp(id OpID, status bool) error {
	logrus.Debugf("RemovePendingOp OpID=%v", id)
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	i := int(id)
	if i < 0 || i >= len(pendingOps) {
		err := fmt.Errorf("RemovePendingOp: Invalid OpID %v", i)
		logrus.Error(err)
		return err
	}
	if pendingOps[i].op == OpNone {
		err := fmt.Errorf("RemovePendingOp: OpID already removed OpID %v", i)
		logrus.Error(err)
		return err
	}

	// Update the duration and store in the recent stats
	pendingOps[i].duration = time.Since(pendingOps[i].timestamp)
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
