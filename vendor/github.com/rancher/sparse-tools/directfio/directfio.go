package directfio

import (
	"unsafe"
	"os"
)

const (
	// what to align the block buffer to
	alignment = 4096

	// BlockSize sic
	BlockSize = alignment
)

// ReadAt read into unaligned data buffer via direct I/O
func ReadAt(file *os.File, data []byte, offset int64) (int, error) {
    if alignmentShift(data) == 0 {
        return file.ReadAt(data, offset)
    }
    buf := AllocateAligned(len(data))
    n, err := file.ReadAt(buf, offset)
    copy(data, buf)
    return n, err
}

// WriteAt write from unaligned data buffer via direct I/O
func WriteAt(file *os.File, data []byte, offset int64) (int, error) {
    if alignmentShift(data) == 0 {
        return file.WriteAt(data, offset)
    }
    // Write unaligned
    buf := AllocateAligned(len(data))
    copy(buf, data)
    n, err := file.WriteAt(buf, offset)
    return n, err
}

// AllocateAligned returns []byte of size aligned to alignment
func AllocateAligned(size int) []byte {
	block := make([]byte, size+alignment)
	shift := alignmentShift(block)
	offset := 0
	if shift != 0 {
		offset = alignment - shift
	}
	block = block[offset:size]
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

