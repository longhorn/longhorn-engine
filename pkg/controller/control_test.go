package controller

import (
	"io"
	"reflect"
	"testing"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-engine/pkg/types"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

type testset struct {
	volumeSize        int64
	volumeCurrentSize int64
	backendSizes      map[int64]struct{}
	expectedSize      int64
}

func (s *TestSuite) TestDetermineCorrectVolumeSize(c *C) {
	testsets := []testset{
		{
			volumeSize:        64,
			volumeCurrentSize: 0,
			backendSizes: map[int64]struct{}{
				64: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 0,
			backendSizes: map[int64]struct{}{
				0: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 64,
			backendSizes: map[int64]struct{}{
				64: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 64,
			backendSizes: map[int64]struct{}{
				32: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 32,
			backendSizes: map[int64]struct{}{
				64: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 32,
			backendSizes: map[int64]struct{}{
				32: {},
			},
			expectedSize: 32,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 32,
			backendSizes: map[int64]struct{}{
				32: {},
				64: {},
			},
			expectedSize: 32,
		},
	}

	for _, t := range testsets {
		size := determineCorrectVolumeSize(t.volumeSize, t.volumeCurrentSize, t.backendSizes)
		c.Assert(size, Equals, t.expectedSize)
	}
}

type fakeReader struct {
	source []byte
}

func (r *fakeReader) ReadAt(buf []byte, off int64) (int, error) {
	copy(buf, r.source[off:off+int64(len(buf))])
	return len(buf), nil
}

type fakeWriter struct {
	source []byte
}

func (w *fakeWriter) WriteAt(buf []byte, off int64) (int, error) {
	copy(w.source[off:off+int64(len(buf))], buf)
	return len(buf), nil
}

func newMockReplicator(readSource, writeSource []byte) *replicator {
	return &replicator{
		backendsAvailable: true,
		backends:          map[string]backendWrapper{},
		writerIndex:       map[int]string{0: "fakeWriter"},
		readerIndex:       map[int]string{0: "fakeReader"},
		readers:           []io.ReaderAt{&fakeReader{source: readSource}},
		writer:            &fakeWriter{source: writeSource},
		next:              0,
	}
}

func (s *TestSuite) TestWriteInWOMode(c *C) {
	type testCase struct {
		buf          []byte
		off          int64
		expectedData []byte
	}

	var dataLength = diskutil.VolumeSectorSize * 4
	var readSourceInitVal byte = 1
	var writeSourceInitVal byte = 0
	var newVal byte = 2

	testsets := []testCase{}

	// Test case #0
	buf := makeByteSliceWithInitialData(512, newVal)
	var off int64 = 0
	expectedData := makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 512:
			expectedData[i] = newVal
		case i < 4096:
			expectedData[i] = readSourceInitVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #1
	buf = makeByteSliceWithInitialData(512, newVal)
	off = 512
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 512:
			expectedData[i] = readSourceInitVal
		case i < 512+512:
			expectedData[i] = newVal
		case i < 4096:
			expectedData[i] = readSourceInitVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #2
	buf = makeByteSliceWithInitialData(512, newVal)
	off = 4096 - 512
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 4096-512:
			expectedData[i] = readSourceInitVal
		case i < 4096:
			expectedData[i] = newVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #3
	buf = makeByteSliceWithInitialData(4096+1024, newVal)
	off = 4096 - 512
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 4096-512:
			expectedData[i] = readSourceInitVal
		case i < 4096-512+4096+1024:
			expectedData[i] = newVal
		case i < 4096*3:
			expectedData[i] = readSourceInitVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #4
	buf = makeByteSliceWithInitialData(4096, newVal)
	off = 4096
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 4096:
			continue
		case i < 4096+4096:
			expectedData[i] = newVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #5
	buf = makeByteSliceWithInitialData(4096*2, newVal)
	off = 4096
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 4096:
			continue
		case i < 4096*3:
			expectedData[i] = newVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #6
	buf = makeByteSliceWithInitialData(4096+512, newVal)
	off = int64(dataLength - 4096 - 512)
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < 4096*2:
			continue
		case i < 4096*4-4096-512:
			expectedData[i] = readSourceInitVal
		case i < 4096*4:
			expectedData[i] = newVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #7
	buf = makeByteSliceWithInitialData(512, newVal)
	off = int64(dataLength - 512)
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	for i := 0; i < len(expectedData); i++ {
		switch {
		case i < dataLength-4096:
			continue
		case i < dataLength-512:
			expectedData[i] = readSourceInitVal
		case i < dataLength:
			expectedData[i] = newVal
		}
	}
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #8
	buf = makeByteSliceWithInitialData(0, newVal)
	off = 4096 * 3
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	// Test case #9
	buf = makeByteSliceWithInitialData(0, newVal)
	off = 4096 + 512
	expectedData = makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	testsets = append(testsets, testCase{buf: buf, off: off, expectedData: expectedData})

	readSource := makeByteSliceWithInitialData(dataLength, readSourceInitVal)
	writeSource := makeByteSliceWithInitialData(dataLength, writeSourceInitVal)
	controller := Controller{
		VolumeName: "test-controller",
		replicas:   []types.Replica{types.Replica{Address: "0.0.0.0", Mode: types.WO}},
		backend:    newMockReplicator(readSource, writeSource),
	}

	for _, t := range testsets {
		// Uncomment this for debugging purpose
		// fmt.Printf("test case number: %v \n", i)

		// reset data
		resetSlice(writeSource, writeSourceInitVal)
		// run test
		n, err := controller.writeInWOMode(t.buf, t.off)
		// check data
		c.Assert(n, Equals, len(t.buf))
		c.Assert(err, Equals, nil)
		c.Assert(reflect.DeepEqual(writeSource, t.expectedData), Equals, true)
	}
}

func makeByteSliceWithInitialData(length int, val byte) []byte {
	buf := make([]byte, length)
	resetSlice(buf, val)
	return buf
}

func resetSlice(data []byte, val byte) {
	for i := range data {
		data[i] = val
	}
}

func (s *TestSuite) TestHandleDiskNoSpaceErrorForReplicas(c *C) {
	tests := []struct {
		name                 string
		replicas             []types.Replica
		replicaNoSpaceErrMap map[string]string
		expectedError        error
		expectedReplicaModes map[string]types.Mode
	}{
		{
			name:                 "empty error map should return nil",
			replicas:             []types.Replica{},
			replicaNoSpaceErrMap: map[string]string{},
			expectedError:        nil,
			expectedReplicaModes: map[string]types.Mode{},
		},
		{
			name: "single writable replica with no space error should return ErrNoSpaceLeftOnDevice",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]string{
				"1.1.1.1": "no space left on device",
			},
			expectedError: types.ErrNoSpaceLeftOnDevice,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
			},
		},
		{
			name: "all writable replicas with no space error should return ErrNoSpaceLeftOnDevice",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]string{
				"1.1.1.1": "no space left on device",
				"1.1.1.2": "no space left on device",
			},
			expectedError: types.ErrNoSpaceLeftOnDevice,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.ERR,
				"1.1.1.3": types.ERR,
			},
		},
		{
			name: "partial replicas with no space error should mark affected replicas as ERR",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.WO},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]string{
				"1.1.1.1": "no space left on device",
			},
			expectedError: nil,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.ERR, // Should be changed to ERR
				"1.1.1.2": types.RW,
				"1.1.1.3": types.RW,
			},
		},
		{
			name: "replicas with WO mode should count as writable",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]string{
				"1.1.1.2": "no space left on device",
			},
			expectedError: nil,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.ERR,
				"1.1.1.3": types.ERR,
			},
		},
	}

	readSource := makeByteSliceWithInitialData(1, 1)
	writeSource := makeByteSliceWithInitialData(1, 1)
	ctrl := &Controller{
		replicas:   []types.Replica{},
		VolumeName: "test-no-space-left-on-device",
		backend:    newMockReplicator(readSource, writeSource),
	}
	for _, tt := range tests {
		ctrl.replicas = tt.replicas

		// Call the method under test
		err := ctrl.handleDiskNoSpaceErrorForReplicas(tt.replicaNoSpaceErrMap)

		// Check the error result
		if tt.expectedError != nil {
			c.Assert(err, NotNil)
			c.Assert(err, Equals, tt.expectedError)
		} else {
			c.Assert(err, IsNil)
		}

		// Check that replica modes are set correctly
		for address, expectedMode := range tt.expectedReplicaModes {
			found := false
			for _, replica := range ctrl.replicas {
				if replica.Address == address {
					c.Assert(replica.Mode, Equals, expectedMode)
					found = true
					break
				}
			}
			c.Assert(found, Equals, true)
		}
	}
}
