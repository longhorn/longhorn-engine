package sparse

import (
	"fmt"
	"os"
	"syscall"

	log "github.com/sirupsen/logrus"
)

const (
	batchBlockCount  = 32
	progressComplete = 100
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
	var progress int

	defer func() {
		if err != nil {
			log.Errorf(err.Error())
			ops.UpdateFoldFileProgress(progress, true, err)
		} else {
			ops.UpdateFoldFileProgress(progressComplete, true, nil)
		}
	}()

	blockSize, err := getFileSystemBlockSize(childFileIo)
	if err != nil {
		return fmt.Errorf("can't get FS block size, error: %v", err)
	}
	exts, err := GetFiemapExtents(childFileIo)
	if err != nil {
		return fmt.Errorf("failed to GetFiemapExtents of childFile filename: %s, err: %v", childFileIo.Name(), err)
	}

	for _, e := range exts {
		dataBegin := int64(e.Logical)
		dataEnd := int64(e.Logical + e.Length)

		// now we have a data start offset and length(hole - data)
		// let's read from child and write to parent file. We read/write up to
		// 32 blocks in a batch
		_, err = parentFileIo.Seek(dataBegin, os.SEEK_SET)
		if err != nil {
			return fmt.Errorf("Failed to os.Seek os.SEEK_SET parentFile filename: %v, at: %v", parentFileIo.Name(), dataBegin)
		}

		batch := batchBlockCount * blockSize
		buffer := AllocateAligned(batch)
		for offset := dataBegin; offset < dataEnd; {
			var n int

			size := batch
			if offset+int64(size) > dataEnd {
				size = int(dataEnd - offset)
			}
			// read a batch from child
			n, err = childFileIo.ReadAt(buffer[:size], offset)
			if err != nil {
				return fmt.Errorf("Failed to read childFile filename: %v, size: %v, at: %v", childFileIo.Name(), size, offset)
			}
			// write a batch to parent
			n, err = parentFileIo.WriteAt(buffer[:size], offset)
			if err != nil {
				return fmt.Errorf("Failed to write to parentFile filename: %v, size: %v, at: %v", parentFileIo.Name(), size, offset)
			}
			offset += int64(n)
			progress = int(float64(offset) / float64(fileSize) * 100)
			ops.UpdateFoldFileProgress(progress, false, nil)
		}
	}

	return nil
}

// get the file system block size
func getFileSystemBlockSize(fileIo FileIoProcessor) (int, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(fileIo.Name(), &stat)
	return int(stat.Blksize), err
}
