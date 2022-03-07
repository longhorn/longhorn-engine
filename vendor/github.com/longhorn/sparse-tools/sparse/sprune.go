package sparse

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// PruneFile removes the overlapping chunks from the parent snapshot based on its child volume head
func PruneFile(parentFileName, childFileName string, ops FileHandlingOperations) error {
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

	// may be caused by the expansion
	if childFInfo.Size() != parentFInfo.Size() {
		if childFInfo.Size() < parentFInfo.Size() {
			return fmt.Errorf("file sizes are not equal and the parent file is larger than the child file")
		}
		if err := os.Truncate(parentFileName, childFInfo.Size()); err != nil {
			return fmt.Errorf("failed to expand the parent file size before pruning, error: %v", err)
		}
	}

	// open child and parent files
	childFileIo, err := NewDirectFileIoProcessor(childFileName, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open childFile, error: %v", err)
	}
	defer childFileIo.Close()

	parentFileIo, err := NewDirectFileIoProcessor(parentFileName, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open parentFile, error: %v", err)
	}
	defer parentFileIo.Close()

	return prune(parentFileIo, childFileIo, childFInfo.Size(), ops)
}

func prune(parentFileIo, childFileIo FileIoProcessor, fileSize int64, ops FileHandlingOperations) (err error) {
	progress := new(uint32)
	progressMutex := &sync.Mutex{}

	defer func() {
		if err != nil {
			log.Errorf(err.Error())
			updateProgress(progress, atomic.LoadUint32(progress), true, err, progressMutex, ops)
		} else {
			updateProgress(progress, progressComplete, true, nil, progressMutex, ops)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncStartTime := time.Now()
	out, errc, err := GetFileLayout(ctx, childFileIo)
	if err != nil {
		return fmt.Errorf("failed to retrieve file layout for file %v err: %v", childFileIo.Name(), err)
	}

	parentFiemap := NewFiemapFile(parentFileIo.GetFile())

	processSegment := func(segment FileInterval) error {
		if segment.Kind == SparseData {
			// punch a hole to the parent to reclaim the space
			if err := parentFiemap.PunchHole(segment.Begin, segment.Len()); err != nil {
				return fmt.Errorf("failed to punch a hole to childFile filename: %v, size: %v, at: %v, error: %v",
					childFileIo.Name(), segment.Begin, segment.Len(), err)
			}
		}
		newProgress := uint32(float64(segment.End) / float64(fileSize) * 100)
		updateProgress(progress, newProgress, false, nil, progressMutex, ops)

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

	log.Debugf("finished pruning for parent: %v child: %v size: %v elapsed: %.2fs",
		parentFileIo.Name(), childFileIo.Name(), fileSize, time.Now().Sub(syncStartTime).Seconds())
	return err
}
