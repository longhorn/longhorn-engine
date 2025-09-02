package replica

import (
	"errors"
	"fmt"
	"io"

	"github.com/longhorn/longhorn-engine/pkg/types"
	. "gopkg.in/check.v1"
)

var (
	ErrClosed          = errors.New("disk is closed")
	ErrWriteBeyondSize = errors.New("write beyond disk size")
)

// Mock implementation of types.DiffDisk for testing
type mockDiffDisk struct {
	data     []byte
	size     int64
	isClosed bool
}

func newMockDiffDisk(size int64) types.DiffDisk {
	return &mockDiffDisk{
		data: make([]byte, size),
		size: size,
	}
}

func (m *mockDiffDisk) ReadAt(buf []byte, offset int64) (int, error) {
	if m.isClosed {
		return 0, ErrClosed
	}
	if offset >= m.size {
		return 0, io.EOF
	}
	end := offset + int64(len(buf))
	if end > m.size {
		end = m.size
	}
	n := copy(buf, m.data[offset:end])
	return n, nil
}

func (m *mockDiffDisk) WriteAt(buf []byte, offset int64) (int, error) {
	if m.isClosed {
		return 0, ErrClosed
	}
	if offset+int64(len(buf)) > m.size {
		return 0, ErrWriteBeyondSize
	}
	n := copy(m.data[offset:], buf)
	return n, nil
}

func (m *mockDiffDisk) UnmapAt(length uint32, offset int64) (int, error) {
	if m.isClosed {
		return 0, ErrClosed
	}
	end := offset + int64(length)
	if end > m.size {
		end = m.size
	}
	for i := offset; i < end; i++ {
		m.data[i] = 0
	}
	return int(end - offset), nil
}

func (m *mockDiffDisk) Size() (int64, error) {
	if m.isClosed {
		return 0, ErrClosed
	}
	return m.size, nil
}

func (m *mockDiffDisk) Close() error {
	m.isClosed = true
	return nil
}

func (m *mockDiffDisk) Sync() error {
	return nil
}

func (m *mockDiffDisk) Fd() uintptr {
	return uintptr(0)
}

func createTestDiffDisk(sectorSize int64, numFiles int, fileSize int64) *diffDisk {
	files := make([]types.DiffDisk, numFiles)
	files[0] = nil // Index 0 is always nil

	for i := 1; i < numFiles; i++ {
		files[i] = newMockDiffDisk(fileSize)
	}

	locationSize := int(fileSize / sectorSize)
	if fileSize%sectorSize != 0 {
		locationSize++
	}

	return &diffDisk{
		location:   make([]byte, locationSize),
		files:      files,
		sectorSize: sectorSize,
		size:       fileSize,
	}
}

func initializeSector(d *diffDisk, sectorIndex int, fileIndex int, fillValue byte) {
	baseBuf := make([]byte, d.sectorSize)
	fill(baseBuf, fillValue)

	offset := int64(sectorIndex) * d.sectorSize
	if _, err := d.files[fileIndex].WriteAt(baseBuf, offset); err != nil {
		panic(fmt.Sprintf("failed to write sector %d: %v", sectorIndex, err))
	}
	d.location[sectorIndex] = byte(fileIndex)
}

func initializeSectors(d *diffDisk, startSector, count, fileIndex int, fillValue byte) {
	for i := 0; i < count; i++ {
		initializeSector(d, startSector+i, fileIndex, fillValue)
	}
}

func (s *TestSuite) TestDiffDiskWriteAt(c *C) {
	tests := []struct {
		name          string
		sectorSize    int64
		fileSize      int64
		numFiles      int
		buf           []byte
		offset        int64
		expectedBytes int
		expectedError bool
		setupFunc     func(*diffDisk, []byte)
	}{
		{
			name:          "empty buffer should return 0",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           []byte{},
			offset:        0,
			expectedBytes: 0,
			expectedError: false,
		},
		{
			name:          "aligned write - single sector",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           make([]byte, 512),
			offset:        0,
			expectedBytes: 512,
			expectedError: false,
			setupFunc:     func(d *diffDisk, buf []byte) { fill(buf, 0xAA) },
		},
		{
			name:          "aligned write - multiple sectors",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           make([]byte, 1024), // 2 sectors
			offset:        512,                // Start at sector 1
			expectedBytes: 1024,
			expectedError: false,
			setupFunc:     func(d *diffDisk, buf []byte) { fill(buf, 0xBB) },
		},
		{
			name:          "unaligned write - single sector",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           []byte("hello world"),
			offset:        100, // Unaligned offset
			expectedBytes: 11,
			expectedError: false,
			setupFunc:     func(d *diffDisk, buf []byte) { initializeSector(d, 0, 2, 0xAA) },
		},
		{
			name:          "unaligned write - multiple sectors",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           make([]byte, 600), // Spans 2 sectors
			offset:        400,               // Start in middle of first sector
			expectedBytes: 600,
			expectedError: false,
			setupFunc: func(d *diffDisk, buf []byte) {
				fill(buf, 0xCC)
				initializeSectors(d, 0, 2, 2, 0xAA)
			},
		},
		{
			name:          "write at sector boundary",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           make([]byte, 512),
			offset:        512, // Exactly at sector boundary
			expectedBytes: 512,
			expectedError: false,
			setupFunc: func(d *diffDisk, buf []byte) {
				fill(buf, 0xDD)
				initializeSector(d, 1, 2, 0xAA)
			},
		},
		{
			name:          "large write - multiple aligned sectors",
			sectorSize:    512,
			fileSize:      8192,
			numFiles:      3,
			buf:           make([]byte, 2048), // 4 sectors
			offset:        1024,               // Start at sector 2
			expectedBytes: 2048,
			expectedError: false,
			setupFunc:     func(d *diffDisk, buf []byte) { fill(buf, 0xFF) },
		},
		{
			name:          "complex spanning scenario",
			sectorSize:    512,
			fileSize:      4096,
			numFiles:      3,
			buf:           make([]byte, 800), // Spans multiple sectors with unaligned start and end
			offset:        300,
			expectedBytes: 800,
			expectedError: false,
			setupFunc: func(d *diffDisk, buf []byte) {
				fill(buf, 0xFF)
				initializeSectors(d, 0, 3, 2, 0xAA)
			},
		},
	}

	for _, tt := range tests {
		d := createTestDiffDisk(tt.sectorSize, tt.numFiles, tt.fileSize)

		// Setup test data if needed
		if tt.setupFunc != nil {
			tt.setupFunc(d, tt.buf)
		}

		// Execute WriteAt
		n, err := d.WriteAt(tt.buf, tt.offset)

		// Verify results
		c.Assert(n, Equals, tt.expectedBytes, Commentf("Test: %s - Written bytes mismatch", tt.name))

		if tt.expectedError {
			c.Assert(err, NotNil, Commentf("Test: %s - Expected error but got nil", tt.name))
		} else {
			c.Assert(err, IsNil, Commentf("Test: %s - Unexpected error: %v", tt.name, err))
		}
	}
}

func (s *TestSuite) TestComputeNominalWrittenBytes(c *C) {
	tests := []struct {
		name           string
		bufSize        int64
		wb             int64
		offset         int64
		readOffset     int64
		expectedResult int64
	}{
		{
			name:           "wb covers entire buffer",
			bufSize:        100,
			wb:             150,
			offset:         125,
			readOffset:     100,
			expectedResult: 100,
			// When wb-offset (125) > bufSize (100), should return bufSize"
		},
		{
			name:           "wb partially covers buffer",
			bufSize:        100,
			wb:             75,
			offset:         125,
			readOffset:     100,
			expectedResult: 50,
			// When wb-offset (50) < bufSize (100), should return wb-offset
		},
		{
			name:           "wb equals offset",
			bufSize:        100,
			wb:             50,
			offset:         150,
			readOffset:     100,
			expectedResult: 0,
			// When wb equals offset, should return 0
		},
		{
			name:           "wb less than offset",
			bufSize:        100,
			wb:             30,
			offset:         150,
			readOffset:     100,
			expectedResult: 0,
			// When wb < offset (negative result), should return 0
		},
		{
			name:           "zero buffer size",
			bufSize:        0,
			wb:             50,
			offset:         125,
			readOffset:     100,
			expectedResult: 0,
			// When bufSize is 0, should return 0
		},
		{
			name:           "zero wb",
			bufSize:        100,
			wb:             0,
			offset:         125,
			readOffset:     100,
			expectedResult: 0,
			// When wb is 0, result is negative, should return 0
		},
		{
			name:           "offset equals readOffset",
			bufSize:        100,
			wb:             75,
			offset:         100,
			readOffset:     100,
			expectedResult: 75,
			// When offset equals readOffset, should return min(wb, bufSize)
		},
		{
			name:           "zero offset",
			bufSize:        100,
			wb:             75,
			offset:         0,
			readOffset:     0,
			expectedResult: 75,
			// When offset is 0, should return min(wb, bufSize)
		},
		{
			name:           "all zero",
			bufSize:        0,
			wb:             0,
			offset:         0,
			readOffset:     0,
			expectedResult: 0,
			// When all parameters are 0, should return 0
		},
		{
			name:           "wb exactly covers buffer",
			bufSize:        100,
			wb:             125,
			offset:         25,
			readOffset:     25,
			expectedResult: 100,
			// When wb-offset exactly equals bufSize, should return bufSize
		},
		{
			name:           "large numbers test",
			bufSize:        1048576, // 1MB
			wb:             2097152, // 2MB
			offset:         524288,  // 512KB
			readOffset:     524288,  // 512KB
			expectedResult: 1048576, // 1MB
			// Test with large numbers (MB range)
		},
	}

	for _, tt := range tests {
		c.Logf("Running test: %s", tt.name)

		result := computeNominalWrittenBytes(tt.bufSize, tt.wb, tt.offset, tt.readOffset)

		c.Assert(result, Equals, tt.expectedResult, Commentf("Test: %s, Expected: %d, Got: %d", tt.name, tt.expectedResult, result))
		// Additional validation - result should never be negative
		c.Assert(result >= 0, Equals, true, Commentf("Test: %s - Result should never be negative, got: %d", tt.name, result))
		// Additional validation - result should never exceed bufSize
		c.Assert(result <= tt.bufSize, Equals, true, Commentf("Test: %s - Result should never exceed bufSize (%d), got: %d", tt.name, tt.bufSize, result))
	}
}
