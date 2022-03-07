package sparse

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	batchBlockCount  = 32
	progressComplete = uint32(100)
)

type FileHandlingOperations interface {
	UpdateFileHandlingProgress(progress int, done bool, err error)
}

// updateProgress increases progress monotonically only.
// Once that's replaced we can use an atomic cmp&swp around progress that
// doesn't require locking.
// We also limit the update function calls to p=[0,100] since there
// is no point in calling updates for the same p value multiple times
func updateProgress(currentProgressAddr *uint32, newProgress uint32, done bool, err error, progressMutex *sync.Mutex, ops FileHandlingOperations) {
	forcedUpdate := done || err != nil
	if !forcedUpdate && newProgress <= atomic.LoadUint32(currentProgressAddr) {
		return
	}

	// this lock ensures that there is only a single in flight UpdateFileHandlingProgress operation
	// with multiple in flight operations, the lock on the receiver side would be non-deterministically
	// acquired which would potentially lead to non-monotonic progress updates.
	progressMutex.Lock()
	defer progressMutex.Unlock()
	if forcedUpdate || newProgress > atomic.LoadUint32(currentProgressAddr) {
		atomic.StoreUint32(currentProgressAddr, newProgress)
		ops.UpdateFileHandlingProgress(int(newProgress), done, err)
	}
}

// get the file system block size
func getFileSystemBlockSize(fileIo FileIoProcessor) (int, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(fileIo.Name(), &stat)
	return int(stat.Blksize), err
}

// mergeErrorChannels will merge all error channels into a single error out channel.
// the error out channel will be closed once the ctx is done or all error channels are closed
// if there is an error on one of the incoming channels the error will be relayed.
func mergeErrorChannels(ctx context.Context, channels ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	wg.Add(len(channels))

	out := make(chan error, len(channels))
	output := func(c <-chan error) {
		defer wg.Done()
		select {
		case err, ok := <-c:
			if ok {
				out <- err
			}
			return
		case <-ctx.Done():
			return
		}
	}

	for _, c := range channels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func processFileIntervals(ctx context.Context, in <-chan FileInterval, processInterval func(interval FileInterval) error) <-chan error {
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		for {
			select {
			case <-ctx.Done():
				return
			case interval, open := <-in:
				if !open {
					return
				}

				if err := processInterval(interval); err != nil {
					errc <- err
					return
				}
			}
		}
	}()
	return errc
}
