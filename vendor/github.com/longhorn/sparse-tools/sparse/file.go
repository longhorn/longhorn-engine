package sparse

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

type FileIoProcessor interface {
	// File I/O methods for direct or bufferend I/O
	ReadAt(data []byte, offset int64) (int, error)
	WriteAt(data []byte, offset int64) (int, error)
	UnmapAt(length uint32, offset int64) (int, error)
	GetFile() *os.File
	GetFieMap() *FiemapFile
	GetDataLayout(ctx context.Context) (<-chan FileInterval, <-chan error, error)
	Close() error
	Sync() error
	Truncate(size int64) error
	Seek(offset int64, whence int) (ret int64, err error)
	Name() string
	Stat() (os.FileInfo, error)
}

type BufferedFileIoProcessor struct {
	*FiemapFile
}

func NewBufferedFileIoProcessor(name string, flag int, perm os.FileMode, isCreate ...bool) (*BufferedFileIoProcessor, error) {
	file, err := os.OpenFile(name, flag, perm)

	// if file does not exist, we need to create it if asked to
	if err != nil && len(isCreate) > 0 && isCreate[0] {
		file, err = os.Create(name)
	}
	if err != nil {
		return nil, err
	}

	return &BufferedFileIoProcessor{NewFiemapFile(file)}, nil
}

func NewBufferedFileIoProcessorByFP(fp *os.File) *BufferedFileIoProcessor {
	return &BufferedFileIoProcessor{NewFiemapFile(fp)}
}

func (file *BufferedFileIoProcessor) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), file.PunchHole(offset, int64(length))
}

func (file *BufferedFileIoProcessor) GetFile() *os.File {
	return file.File
}

func (file *BufferedFileIoProcessor) GetFieMap() *FiemapFile {
	return file.FiemapFile
}

func (file *BufferedFileIoProcessor) GetDataLayout(ctx context.Context) (<-chan FileInterval, <-chan error, error) {
	return GetFileLayout(ctx, file)
}

func (file *BufferedFileIoProcessor) Size() (int64, error) {
	info, err := file.File.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (file *BufferedFileIoProcessor) Close() error {
	file.File.Sync()
	return file.File.Close()
}

type DirectFileIoProcessor struct {
	*FiemapFile
}

const (
	// what to align the block buffer to
	alignment = 4096

	// BlockSize sic
	BlockSize = alignment
)

func NewDirectFileIoProcessor(name string, flag int, perm os.FileMode, isCreate ...bool) (*DirectFileIoProcessor, error) {
	file, err := os.OpenFile(name, syscall.O_DIRECT|flag, perm)

	// if failed open existing and isCreate flag is true, we need to create it if asked to
	if err != nil && len(isCreate) > 0 && isCreate[0] {
		file, err = os.OpenFile(name, os.O_CREATE|syscall.O_DIRECT|flag, perm)
	}
	if err != nil {
		return nil, err
	}

	return &DirectFileIoProcessor{NewFiemapFile(file)}, nil
}

func NewDirectFileIoProcessorByFP(fp *os.File) *DirectFileIoProcessor {
	return &DirectFileIoProcessor{NewFiemapFile(fp)}
}

// ReadAt read into unaligned data buffer via direct I/O
// Use AllocateAligned to avoid extra data buffer copy
func (file *DirectFileIoProcessor) ReadAt(data []byte, offset int64) (int, error) {
	if alignmentShift(data) == 0 {
		return file.File.ReadAt(data, offset)
	}
	buf := AllocateAligned(len(data))
	n, err := file.File.ReadAt(buf, offset)
	copy(data, buf)
	return n, err
}

// WriteAt write from unaligned data buffer via direct I/O
// Use AllocateAligned to avoid extra data buffer copy
func (file *DirectFileIoProcessor) WriteAt(data []byte, offset int64) (int, error) {
	if alignmentShift(data) == 0 {
		return file.File.WriteAt(data, offset)
	}
	// Write unaligned
	buf := AllocateAligned(len(data))
	copy(buf, data)
	n, err := file.File.WriteAt(buf, offset)
	return n, err
}

// UnmapAt punches a hole
func (file *DirectFileIoProcessor) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), file.PunchHole(offset, int64(length))
}

func (file *DirectFileIoProcessor) GetFile() *os.File {
	return file.File
}

func (file *DirectFileIoProcessor) GetFieMap() *FiemapFile {
	return file.FiemapFile
}

func (file *DirectFileIoProcessor) GetDataLayout(ctx context.Context) (<-chan FileInterval, <-chan error, error) {
	return GetFileLayout(ctx, file)
}

func (file *DirectFileIoProcessor) Size() (int64, error) {
	info, err := file.File.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// AllocateAligned returns []byte of size aligned to alignment
func AllocateAligned(size int) []byte {
	block := make([]byte, size+alignment)
	shift := alignmentShift(block)
	offset := 0
	if shift != 0 {
		offset = alignment - shift
	}
	block = block[offset : size+offset]
	shift = alignmentShift(block)
	if shift != 0 {
		panic("Alignment failure")
	}
	return block
}

// alignmentShift returns alignment of the block in memory
func alignmentShift(block []byte) int {
	if len(block) == 0 {
		return 0
	}
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(alignment-1))
}

func ReadDataInterval(rw ReaderWriterAt, dataInterval Interval) ([]byte, error) {
	data := AllocateAligned(int(dataInterval.Len()))
	n, err := rw.ReadAt(data, dataInterval.Begin)
	if err != nil {
		if err == io.EOF {
			log.Debugf("have read at the end of file, total read: %d", n)
		} else {
			errStr := fmt.Sprintf("File to read interval:%s, error: %s", dataInterval, err)
			log.Error(errStr)
			return nil, fmt.Errorf(errStr)
		}
	}
	return data[:n], nil
}

func WriteDataInterval(file FileIoProcessor, dataInterval Interval, data []byte) error {
	_, err := file.WriteAt(data, dataInterval.Begin)
	if err != nil {
		errStr := fmt.Sprintf("Failed to write file interval:%s, error: %s", dataInterval, err)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	return nil
}

func HashFileInterval(file FileIoProcessor, dataInterval Interval) ([]byte, error) {
	data, err := ReadDataInterval(file, dataInterval)
	if err != nil {
		return nil, err
	}
	return HashData(data)
}

func HashData(data []byte) ([]byte, error) {
	sum := sha512.Sum512(data)
	return sum[:], nil
}

func GetFileLayout(ctx context.Context, file FileIoProcessor) (<-chan FileInterval, <-chan error, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}

	// for EXT4 on kernels < 5.8 the extent retrieval time sucks
	// for 1024 extents, it can take 3 minutes to return from the kernel
	// so this process cannot be killed for that amount of time
	// we use a 10x MaxInflightIntervals buffer to allow the extent retrieval to get 5x ahead of processing
	// this makes any variance in the extent retrieval time irrelevant and decreases potential wait time
	const MaxExtentsBuffer = 1024
	const MaxInflightIntervals = MaxExtentsBuffer * 10
	out := make(chan FileInterval, MaxInflightIntervals)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)

		startTime := time.Now()
		totalExtents := 0
		defer func() {
			log.Debugf("retrieved extents for file %v, fileSize: %v elapsed: %.2fs, extents: %v",
				fileInfo.Name(), fileInfo.Size(),
				time.Now().Sub(startTime).Seconds(), totalExtents)
		}()

		var lastIntervalEnd int64
		isFinalInterval := fileInfo.Size() == 0
		for !isFinalInterval {
			exts, err := GetFiemapRegionExts(file, Interval{lastIntervalEnd, fileInfo.Size()}, MaxExtentsBuffer)
			if err != nil {
				errc <- err
				return
			}

			// we pre allocate the temporary interval buffer, so we only need a single allocation
			// the final extent can have 3 intervals, a prior hole, data, final hole
			// other extents will have at most 2 intervals, a prior hole, data
			intervals := make([]FileInterval, 0, 1+len(exts)*2)
			isFinalInterval = len(exts) == 0
			totalExtents += len(exts)

			// special case the whole file is a hole
			if totalExtents == 0 && fileInfo.Size() != 0 {
				hole := FileInterval{SparseHole, Interval{0, fileInfo.Size()}}
				intervals = append(intervals, hole)
			}

			// map to intervals
			for _, e := range exts {
				data := FileInterval{SparseData, Interval{int64(e.Logical), int64(e.Logical + e.Length)}}

				if lastIntervalEnd < data.Begin {
					hole := FileInterval{SparseHole, Interval{lastIntervalEnd, data.Begin}}
					intervals = append(intervals, hole)
				}

				lastIntervalEnd = data.End
				intervals = append(intervals, data)
				isFinalInterval = e.Flags&FIEMAP_EXTENT_LAST != 0

				if isFinalInterval {
					// add a hole between last data segment and end of file
					if lastIntervalEnd < fileInfo.Size() {
						hole := FileInterval{SparseHole, Interval{lastIntervalEnd, fileInfo.Size()}}
						intervals = append(intervals, hole)
					}
				}
			}

			// transmit intervals
			for _, interval := range intervals {
				select {
				case out <- interval:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, errc, err
}

func GetFiemapExtents(file FileIoProcessor) ([]Extent, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	var extents []Extent
	var lastIntervalEnd int64
	const MaxExtentsBuffer = 1024

	isFinalExtent := fileInfo.Size() == 0
	for !isFinalExtent && lastIntervalEnd < fileInfo.Size() {
		exts, err := GetFiemapRegionExts(file, Interval{lastIntervalEnd, fileInfo.Size()}, MaxExtentsBuffer)
		if err != nil {
			return nil, err
		}

		if len(exts) == 0 {
			return extents, nil
		}

		lastIntervalEnd = int64(exts[len(exts)-1].Logical + exts[len(exts)-1].Length + 1)
		isFinalExtent = exts[len(exts)-1].Flags&FIEMAP_EXTENT_LAST != 0
		extents = append(extents, exts...)
	}

	return extents, nil
}

func GetFiemapRegionExts(file FileIoProcessor, interval Interval, extCount int) ([]Extent, error) {
	if interval.End == 0 || extCount == 0 {
		return nil, nil
	}

	retrievalStart := time.Now()
	_, exts, errno := file.GetFieMap().FiemapRegion(uint32(extCount), uint64(interval.Begin), uint64(interval.End-interval.Begin))
	if errno != 0 {
		log.Error("failed to call fiemap.Fiemap(extCount)")
		return exts, fmt.Errorf(errno.Error())
	}

	log.Tracef("retrieved %v/%v extents from file %v interval: %v elapsed: %.2fs",
		len(exts), extCount, file.Name(), interval,
		time.Now().Sub(retrievalStart).Seconds())

	// The exts returned by File System should be ordered
	var lastExtStart uint64
	for i, ext := range exts {

		// if lastExtStart is initialized and this ext start is less than last ext start
		if i != 0 && ext.Logical < lastExtStart {
			return exts, fmt.Errorf("the extents returned by fiemap are not ordered")
		}
		lastExtStart = ext.Logical
	}

	return exts, nil
}
