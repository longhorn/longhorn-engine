package sparse

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	log "github.com/sirupsen/logrus"
)

const (
	defaultCoalesceWorkerCount = 4
)

// FoldFile folds child snapshot data into its parent
func FoldFile(childFileName, parentFileName string, ops FileHandlingOperations) error {
	childFInfo, err := os.Stat(childFileName)
	if err != nil {
		return errors.Wrapf(err, "failed to get file info of child file %v", childFileName)
	}
	parentFInfo, err := os.Stat(parentFileName)
	if err != nil {
		return errors.Wrapf(err, "failed to get file info of parent file %v", parentFileName)
	}

	// ensure no directory
	if childFInfo.IsDir() || parentFInfo.IsDir() {
		return fmt.Errorf("at least one file is directory, not a normal file")
	}

	// may be caused by the expansion
	if childFInfo.Size() != parentFInfo.Size() {
		if childFInfo.Size() < parentFInfo.Size() {
			return fmt.Errorf("file sizes are not equal and the parent file is larger than the child file")
		}
		if err := os.Truncate(parentFileName, childFInfo.Size()); err != nil {
			return errors.Wrap(err, "failed to expand the parent file size before coalesce")
		}
	}

	// open child and parent files
	childFileIo, err := NewDirectFileIoProcessor(childFileName, os.O_RDONLY, 0)
	if err != nil {
		return errors.Wrap(err, "failed to open childFile")
	}
	defer func() {
		_ = childFileIo.Close()
	}()

	parentFileIo, err := NewDirectFileIoProcessor(parentFileName, os.O_WRONLY, 0)
	if err != nil {
		return errors.Wrap(err, "failed to open parentFile")
	}
	defer func() {
		_ = parentFileIo.Close()
	}()

	return coalesce(parentFileIo, childFileIo, childFInfo.Size(), ops)
}

func coalesce(parentFileIo, childFileIo FileIoProcessor, fileSize int64, ops FileHandlingOperations) (err error) {
	progress := new(uint32)
	progressMutex := &sync.Mutex{}

	defer func() {
		if err != nil {
			log.Errorf("%v", err)
			updateProgress(progress, atomic.LoadUint32(progress), true, err, progressMutex, ops)
		} else {
			updateProgress(progress, progressComplete, true, nil, progressMutex, ops)
		}
	}()

	blockSize, err := getFileSystemBlockSize(childFileIo)
	if err != nil {
		return errors.Wrap(err, "can't get FS block size")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncStartTime := time.Now()
	out, errc, err := GetFileLayout(ctx, childFileIo)
	if err != nil {
		return errors.Wrapf(err, "failed to retrieve file layout for file %v", childFileIo.Name())
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
				_, err := childFileIo.ReadAt(buffer[:size], offset)
				if err != nil {
					return errors.Wrapf(err, "failed to read childFile filename: %v, size: %v, at: %v",
						childFileIo.Name(), size, offset)
				}
				// write a batch to parent
				n, err = parentFileIo.WriteAt(buffer[:size], offset)
				if err != nil {
					return errors.Wrapf(err, "failed to write to parentFile filename: %v, size: %v, at: %v",
						parentFileIo.Name(), size, offset)
				}
				offset += int64(n)
			}

			newProgress := uint32(float64(segment.End) / float64(fileSize) * 100)
			updateProgress(progress, newProgress, false, nil, progressMutex, ops)
		}

		return nil
	}

	errorChannels := []<-chan error{errc}
	for i := 0; i < defaultCoalesceWorkerCount; i++ {
		errorChannels = append(errorChannels, processFileIntervals(ctx, out, processSegment))
	}

	// the below select will exit once all error channels are closed, or a single
	// channel has run into an error, which will lead to the ctx being cancelled
	mergedErrc := mergeErrorChannels(ctx, errorChannels...)
	err = <-mergedErrc

	log.Debugf("Finished fold for parent %v, child %v, size %v, elapsed %.2fs",
		parentFileIo.Name(), childFileIo.Name(), fileSize,
		time.Since(syncStartTime).Seconds())
	return err
}
