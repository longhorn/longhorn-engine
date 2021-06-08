package sparse

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	batchBlockCount  = 32
	progressComplete = uint32(100)
)

type FoldFileOperations interface {
	UpdateFoldFileProgress(progress int, done bool, err error)
}

// FoldFile folds child snapshot data into its parent
func FoldFile(childFileName, parentFileName string, ops FoldFileOperations) error {
	childFInfo, err := os.Stat(childFileName)
	if err != nil {
		return fmt.Errorf("os.Stat(childFileName) failed, error: %v", err)
	}
	parentFInfo, err := os.Stat(parentFileName)
	if err != nil {
		return fmt.Errorf("os.Stat(parentFileName) failed, error: %v", err)
	}

	// ensure no directory
	if childFInfo.IsDir() || parentFInfo.IsDir() {
		return fmt.Errorf("at least one file is directory, not a normal file")
	}

	// may caused by the expansion
	if childFInfo.Size() != parentFInfo.Size() {
		if childFInfo.Size() < parentFInfo.Size() {
			return fmt.Errorf("file sizes are not equal and the parent file is larger than the child file")
		}
		if err := os.Truncate(parentFileName, childFInfo.Size()); err != nil {
			return fmt.Errorf("failed to expand the parent file size before coalesce, error: %v", err)
		}
	}

	// open child and parent files
	childFileIo, err := NewDirectFileIoProcessor(childFileName, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open childFile, error: %v", err)
	}
	defer childFileIo.Close()

	parentFileIo, err := NewDirectFileIoProcessor(parentFileName, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open parentFile, error: %v", err)
	}
	defer parentFileIo.Close()

	return coalesce(parentFileIo, childFileIo, childFInfo.Size(), ops)
}

func coalesce(parentFileIo, childFileIo FileIoProcessor, fileSize int64, ops FoldFileOperations) (err error) {
	var progress uint32
	progressMutex := &sync.Mutex{}

	// progress updates can only be monotonically increasing, once that's replaced we can use an atomic cmp&swp around progress
	// that doesn't require locking and we also limit the update function calls to p=[0,100] since there
	// is no point in calling updates for the same p value multiple times
	updateProgress := func(newProgress uint32, done bool, err error) {
		forcedUpdate := done || err != nil
		if !forcedUpdate && newProgress <= atomic.LoadUint32(&progress) {
			return
		}

		// this lock ensures that there is only a single in flight UpdateFoldFileProgress operation
		// with multiple in flight operations, the lock on the receiver side would be non deterministically
		// acquired which would potentially lead to non monotonic progress updates.
		progressMutex.Lock()
		defer progressMutex.Unlock()
		if forcedUpdate || newProgress > atomic.LoadUint32(&progress) {
			ops.UpdateFoldFileProgress(int(newProgress), done, err)
			atomic.StoreUint32(&progress, newProgress)
		}
	}

	defer func() {
		if err != nil {
			log.Errorf(err.Error())
			updateProgress(atomic.LoadUint32(&progress), true, err)
		} else {
			updateProgress(progressComplete, true, nil)
		}
	}()

	blockSize, err := getFileSystemBlockSize(childFileIo)
	if err != nil {
		return fmt.Errorf("can't get FS block size, error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncStartTime := time.Now()
	out, errc, err := GetFileLayout(ctx, childFileIo)
	if err != nil {
		return fmt.Errorf("failed to retrieve file layout for file %v err: %v", childFileIo.Name(), err)
	}

	processSegment := func(segment FileInterval) error {
		batch := batchBlockCount * blockSize
		buffer := AllocateAligned(batch)

		if segment.Kind == SparseData {
			for offset := segment.Begin; offset < segment.End; {
				var n int

				size := batch
				if offset+int64(size) > segment.End {
					size = int(segment.End - offset)
				}
				// read a batch from child
				n, err := childFileIo.ReadAt(buffer[:size], offset)
				if err != nil {
					return fmt.Errorf("failed to read childFile filename: %v, size: %v, at: %v",
						childFileIo.Name(), size, offset)
				}
				// write a batch to parent
				n, err = parentFileIo.WriteAt(buffer[:size], offset)
				if err != nil {
					return fmt.Errorf("failed to write to parentFile filename: %v, size: %v, at: %v",
						parentFileIo.Name(), size, offset)
				}
				offset += int64(n)
			}

			newProgress := uint32(float64(segment.End) / float64(fileSize) * 100)
			updateProgress(newProgress, false, nil)
		}

		return nil
	}

	const WorkerCount = 4
	errorChannels := []<-chan error{errc}
	for i := 0; i < WorkerCount; i++ {
		errorChannels = append(errorChannels, processFileIntervals(ctx, out, processSegment))
	}

	// the below select will exit once all error channels are closed, or a single
	// channel has run into an error, which will lead to the ctx being cancelled
	mergedErrc := mergeErrorChannels(ctx, errorChannels...)
	select {
	case err = <-mergedErrc:
		break
	}

	log.Debugf("finished fold for parent: %v child: %v size: %v elapsed: %.2fs",
		parentFileIo.Name(), childFileIo.Name(), fileSize, time.Now().Sub(syncStartTime).Seconds())
	return err
}

// get the file system block size
func getFileSystemBlockSize(fileIo FileIoProcessor) (int, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(fileIo.Name(), &stat)
	return int(stat.Blksize), err
}
