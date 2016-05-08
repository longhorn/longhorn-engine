package sparse

import (
	"os"
	"syscall"

	"fmt"

	"github.com/frostschutz/go-fibmap"
	"github.com/rancher/sparse-tools/log"
)

// Interval [Begin, End) is non-inclusive at the End
type Interval struct {
	Begin, End int64
}

// Len returns length of Interval
func (interval Interval) Len() int64 {
	return interval.End - interval.Begin
}

// String conversion
func (interval Interval) String() string {
	return fmt.Sprintf("[%8d:%8d](%3d)", interval.Begin/Blocks, interval.End/Blocks, interval.Len()/Blocks)
}

// FileIntervalKind distinguishes between data and hole
type FileIntervalKind int

// Sparse file Interval types
const (
	SparseData FileIntervalKind = 1 + iota
	SparseHole
	SparseIgnore // ignore file interval (equal src vs dst part)
)

// FileInterval describes either sparse data Interval or a hole
type FileInterval struct {
	Kind FileIntervalKind
	Interval
}

func (i FileInterval) String() string {
	kind := "?"
	switch i.Kind {
	case SparseData:
		kind = "D"
	case SparseHole:
		kind = " "
	case SparseIgnore:
		kind = "i"
	}
	return fmt.Sprintf("%s%v", kind, i.Interval)
}

// Storage block size in bytes
const (
	Blocks int64 = 4 << 10 // 4k
)

// os.Seek sparse whence values.
const (
	// Adjust the file offset to the next location in the file
	// greater than or equal to offset containing data.  If offset
	// points to data, then the file offset is set to offset.
	seekData int = 3

	// Adjust the file offset to the next hole in the file greater
	// than or equal to offset.  If offset points into the middle of
	// a hole, then the file offset is set to offset.  If there is no
	// hole past offset, then the file offset is adjusted to the End
	// of the file (i.e., there is an implicit hole at the End of any
	// file).
	seekHole int = 4
)

// syscall.Fallocate mode bits
const (
	// default is extend size
	fallocFlKeepSize uint32 = 1

	// de-allocates range
	fallocFlPunchHole uint32 = 2
)

// RetrieveLayoutStream streams sparse file data/hole layout
// Based on fiemap
// To abort: abortStream <- error
// Check status: err := <- errStream
// Usage: go RetrieveLayoutStream(...)
func RetrieveLayoutStream(abortStream <-chan error, file *os.File, r Interval, layoutStream chan<- FileInterval, errStream chan<- error) {
	const extents = 1024
	const chunkSizeMax = 1 /*GB*/ << 30
	chunkSize := r.Len()
	if chunkSize > chunkSizeMax {
		chunkSize = chunkSizeMax
	}

	chunk := Interval{r.Begin, r.Begin + chunkSize}
	// Process file extents for each chunk
	intervalLast := Interval{chunk.Begin, chunk.Begin}
	for chunk.Begin < r.End {
		if chunk.End > r.End {
			chunk.End = r.End
		}

		for more := true; more && chunk.Len() > 0; {
			ext, errno := fibmap.Fiemap(file.Fd(), uint64(chunk.Begin), uint64(chunk.Len()), 1024)
			if errno != 0 {
				close(layoutStream)
				errStream <- &os.PathError{Op: "Fiemap", Path: file.Name(), Err: errno}
				return
			}
			if len(ext) == 0 {
				break
			}

			// Process each extents
			for _, e := range ext {
				interval := Interval{int64(e.Logical), int64(e.Logical + e.Length)}
				log.Debug("Extent:", interval, e.Flags)
				if e.Flags&fibmap.FIEMAP_EXTENT_LAST != 0 {
					more = false
				}
				if intervalLast.End < interval.Begin {
					if intervalLast.Len() > 0 {
						// Pop last Data
						layoutStream <- FileInterval{SparseData, intervalLast}
					}
					// report hole
					intervalLast = Interval{intervalLast.End, interval.Begin}
					layoutStream <- FileInterval{SparseHole, intervalLast}

					// Start data
					intervalLast = interval
				} else {
					// coalesce
					intervalLast.End = interval.End
				}
				chunk.Begin = interval.End
			}
		}
		chunk = Interval{chunk.End, chunk.End + chunkSizeMax}
	}

	if intervalLast.Len() > 0 {
		// Pop last Data
		layoutStream <- FileInterval{SparseData, intervalLast}
	}
	if intervalLast.End < r.End {
		// report hole
		layoutStream <- FileInterval{SparseHole, Interval{intervalLast.End, r.End}}
	}

	close(layoutStream)
	errStream <- nil
	return
}

// RetrieveLayoutStream0 streams sparse file data/hole layout
// Deprecated; Based on file.seek; use RetrieveLayoutStream instead
// To abort: abortStream <- error
// Check status: err := <- errStream
// Usage: go RetrieveLayoutStream(...)
func RetrieveLayoutStream0(abortStream <-chan error, file *os.File, r Interval, layoutStream chan<- FileInterval, errStream chan<- error) {
	curr := r.Begin

	// Data or hole?
	offsetData, errData := file.Seek(curr, seekData)
	offsetHole, errHole := file.Seek(curr, seekHole)
	var interval FileInterval
	if errData != nil {
		// Hole only
		interval = FileInterval{SparseHole, Interval{curr, r.End}}
		if interval.Len() > 0 {
			layoutStream <- interval
		}
		close(layoutStream)
		errStream <- nil
		return
	} else if errHole != nil {
		// Data only
		interval = FileInterval{SparseData, Interval{curr, r.End}}
		if interval.Len() > 0 {
			layoutStream <- interval
		}
		close(layoutStream)
		errStream <- nil
		return
	}

	if offsetData < offsetHole {
		interval = FileInterval{SparseData, Interval{curr, offsetHole}}
		curr = offsetHole
	} else {
		interval = FileInterval{SparseHole, Interval{curr, offsetData}}
		curr = offsetData
	}
	if interval.Len() > 0 {
		layoutStream <- interval
	}

	for curr < r.End {
		// Check abort condition
		select {
		case err := <-abortStream:
			close(layoutStream)
			errStream <- err
			return
		default:
		}

		var whence int
		if SparseData == interval.Kind {
			whence = seekData
		} else {
			whence = seekHole
		}

		// Note: file.Seek masks syscall.ENXIO hence syscall is used instead
		next, errno := syscall.Seek(int(file.Fd()), curr, whence)
		if errno != nil {
			switch errno {
			case syscall.ENXIO:
				// no more intervals
				next = r.End // close the last interval
			default:
				// mimic standard "os"" package error handler
				close(layoutStream)
				errStream <- &os.PathError{Op: "seek", Path: file.Name(), Err: errno}
				return
			}
		}
		if SparseData == interval.Kind {
			// End of data, handle the last hole if any
			interval = FileInterval{SparseHole, Interval{curr, next}}
		} else {
			// End of hole, handle the last data if any
			interval = FileInterval{SparseData, Interval{curr, next}}
		}
		curr = next
		if interval.Len() > 0 {
			layoutStream <- interval
		}
	}
	close(layoutStream)
	errStream <- nil
	return
}

// RetrieveLayout retrieves sparse file hole and data layout
func RetrieveLayout(file *os.File, r Interval) ([]FileInterval, error) {
	layout := make([]FileInterval, 0, 1024)
	abortStream := make(chan error)
	layoutStream := make(chan FileInterval, 128)
	errStream := make(chan error)

	go RetrieveLayoutStream(abortStream, file, r, layoutStream, errStream)
	for interval := range layoutStream {
		layout = append(layout, interval)
	}
	return layout, <-errStream
}

// PunchHole in a sparse file, preserve file size
func PunchHole(file *os.File, hole Interval) error {
	fd := int(file.Fd())
	mode := fallocFlPunchHole | fallocFlKeepSize
	return syscall.Fallocate(fd, mode, hole.Begin, hole.Len())
}
