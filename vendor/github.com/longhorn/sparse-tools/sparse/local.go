package sparse

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// SyncLocalFile syncs a local file from sourceFilePath to targetFilePath.
// It ensures the file size is a multiple of the Block constant before syncing.
func SyncLocalFile(sourceFilePath, targetFilePath string) error {
	log := logrus.WithFields(logrus.Fields{
		"sourceFilePath": sourceFilePath,
		"targetFilePath": targetFilePath,
	})

	fileInfo, err := os.Stat(sourceFilePath)
	if err != nil {
		log.WithError(err).Error("Failed to get file info of source file")
		return err
	}

	fileSize := fileInfo.Size()
	if fileSize%Blocks != 0 {
		return fmt.Errorf("invalid file size %v for local file sync", fileSize)
	}

	log = log.WithField("fileSize", fileSize)
	log.Info("Syncing file")

	return syncLocalContent(sourceFilePath, targetFilePath, fileSize)
}

func syncLocalContent(sourceFilePath, targetFilePath string, fileSize int64) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync content for source file %v", sourceFilePath)
	}()

	syncBatchSize := defaultSyncBatchSize

	local, err := newSyncLocal(sourceFilePath, targetFilePath, fileSize, syncBatchSize)
	if err != nil {
		return err
	}
	defer local.close()

	syncStartTime := time.Now()

	err = local.syncContent()
	if err != nil {
		return err
	}

	log := logrus.WithFields(logrus.Fields{
		"sourceFilePath": sourceFilePath,
		"targetFilePath": targetFilePath,
		"elapsed":        time.Since(syncStartTime).Seconds(),
	})
	log.Info("Finished syncing file")

	return nil
}

type syncLocal struct {
	sourceFilePath string
	targetFilePath string

	sourceFileIo *DirectFileIoProcessor
	targetFileIo *DirectFileIoProcessor

	syncBatchSize int64
}

func newSyncLocal(sourceFilePath, targetFilePath string, fileSize, syncBatchSize int64) (*syncLocal, error) {
	sourceFileIo, err := NewDirectFileIoProcessor(sourceFilePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	targetFileIo, err := NewDirectFileIoProcessor(targetFilePath, os.O_RDWR|os.O_CREATE, 0)
	if err != nil {
		sourceFileIo.Close()
		return nil, err
	}

	err = targetFileIo.Truncate(fileSize)
	if err != nil {
		sourceFileIo.Close()
		targetFileIo.Close()
		return nil, err
	}

	return &syncLocal{
		sourceFilePath: sourceFilePath,
		targetFilePath: targetFilePath,
		sourceFileIo:   sourceFileIo,
		targetFileIo:   targetFileIo,
		syncBatchSize:  syncBatchSize,
	}, nil
}

func (local *syncLocal) close() {
	local.sourceFileIo.Close()
	local.targetFileIo.Close()
}

func (local *syncLocal) syncContent() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileIntervalChannel, errChannel, err := local.sourceFileIo.GetDataLayout(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to get data layout for file %v", local.sourceFilePath)
	}

	errorChannels := []<-chan error{errChannel}
	errorChannels = append(errorChannels, processFileIntervals(ctx, fileIntervalChannel, local.processSegment))
	// the below select will exit once all error channels are closed, or a single
	// channel has run into an error, which will lead to the ctx being cancelled
	mergedErrc := mergeErrorChannels(ctx, errorChannels...)
	err = <-mergedErrc
	return err
}

func (local *syncLocal) processSegment(segment FileInterval) error {
	switch segment.Kind {
	case SparseHole:
		if err := local.syncHoleInterval(segment.Interval); err != nil {
			return errors.Wrapf(err, "failed to sync hole interval %+v", segment.Interval)
		}
	case SparseData:
		if err := local.syncDataInterval(segment.Interval); err != nil {
			return errors.Wrapf(err, "failed to sync data interval %+v", segment.Interval)
		}
	}
	return nil
}

func (local *syncLocal) syncHoleInterval(holeInterval Interval) error {
	fiemap := NewFiemapFile(local.targetFileIo.GetFile())
	err := fiemap.PunchHole(holeInterval.Begin, holeInterval.Len())
	if err != nil {
		return errors.Wrapf(err, "failed to punch hole interval %+v", holeInterval)
	}

	return nil
}

func (local *syncLocal) syncDataInterval(dataInterval Interval) error {
	// Process data in chunks
	for offset := dataInterval.Begin; offset < dataInterval.End; {
		size := getSize(offset, defaultSyncBatchSize, dataInterval.End)
		batchInterval := Interval{offset, offset + size}

		var err error
		var dataBuffer []byte

		dataBuffer, err = ReadDataInterval(local.sourceFileIo, batchInterval)
		if err != nil {
			return err
		}

		if dataBuffer != nil {
			logrus.Tracef("Sending dataBuffer size: %d", len(dataBuffer))
			if _, err := local.targetFileIo.WriteAt(dataBuffer, offset); err != nil {
				return errors.Wrapf(err, "failed to write data interval %+v", batchInterval)
			}
		}
		offset += batchInterval.Len()
	}
	return nil
}
