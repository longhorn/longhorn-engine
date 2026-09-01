package controller

import (
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"

	"github.com/longhorn/longhorn-engine/pkg/types"
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
		replicaNoSpaceErrMap map[string]int
		expectedAllFull      bool
		expectedReplicaModes map[string]types.Mode
	}{
		{
			name:                 "empty error map should return nil",
			replicas:             []types.Replica{},
			replicaNoSpaceErrMap: map[string]int{},
			expectedAllFull:      false,
			expectedReplicaModes: map[string]types.Mode{},
		},
		{
			name: "single writable replica with no space error should return ErrNoSpaceLeftOnDevice",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
			},
			expectedAllFull: true,
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
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
				"1.1.1.2": 1,
			},
			expectedAllFull: true,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.ERR,
				"1.1.1.3": types.ERR,
			},
		},
		{
			name: "partial replicas with no space error should mark affected replicas as ERR",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
				"1.1.1.2": 1,
			},
			expectedAllFull: true,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should be changed to ERR
				"1.1.1.2": types.ERR,
			},
		},
		{
			name: "replicas with WO mode should count as writable",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.2": 1,
			},
			expectedAllFull: false,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.ERR,
				"1.1.1.3": types.ERR,
			},
		},
		{
			name: "replicas in max length of replicas that have the same written bytes should not be marked as ERR",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.RW},
				{Address: "1.1.1.4", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
				"1.1.1.2": 1,
				"1.1.1.3": 2,
				"1.1.1.4": 3,
			},
			expectedAllFull: true,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.RW, // Should not be changed to ERR
				"1.1.1.3": types.ERR,
				"1.1.1.4": types.ERR,
			},
		},
		{
			name: "replicas with max wb of max length of replicas that have the same written bytes should not be marked as ERR",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.RW},
				{Address: "1.1.1.4", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 2,
				"1.1.1.2": 2,
				"1.1.1.3": 1,
				"1.1.1.4": 1,
			},
			expectedAllFull: true,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.RW, // Should not be changed to ERR
				"1.1.1.3": types.ERR,
				"1.1.1.4": types.ERR,
			},
		},
		{
			name: "replicas with different written bytes should be marked as ERR",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.2": 2,
				"1.1.1.3": 1,
			},
			expectedAllFull: false,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.ERR,
				"1.1.1.3": types.ERR,
			},
		},
		{
			name: "replicas with different modes and different written bytes should be handled correctly",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.WO},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 3,
				"1.1.1.2": 2,
				"1.1.1.3": 1,
			},
			expectedAllFull: true,
			expectedReplicaModes: map[string]types.Mode{
				"1.1.1.1": types.RW, // Should not be changed to ERR
				"1.1.1.2": types.ERR,
				"1.1.1.3": types.ERR,
			},
		},
		{
			name: "all replicas are RW with different written bytes should be handled correctly",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.RW},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 3,
				"1.1.1.2": 2,
				"1.1.1.3": 1,
			},
			expectedAllFull: true,
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
		areAllReplicasNoSpace := ctrl.handleDiskNoSpaceErrorForReplicas(tt.replicaNoSpaceErrMap)
		c.Assert(areAllReplicasNoSpace, Equals, tt.expectedAllFull, Commentf("Test case: %s - Expected all replicas on no space: %v but got %v", tt.name, tt.expectedAllFull, areAllReplicasNoSpace))

		// Check that replica modes are set correctly
		for address, expectedMode := range tt.expectedReplicaModes {
			found := false
			for _, replica := range ctrl.replicas {
				if replica.Address == address {
					c.Assert(replica.Mode, Equals, expectedMode, Commentf("Test case: %s - Expected mode %v for replica %s but got %v", tt.name, expectedMode, address, replica.Mode))
					found = true
					break
				}
			}
			c.Assert(found, Equals, true, Commentf("Test case: %s - Expected replica %s to be found", tt.name, address))
		}
	}
}

func (s *TestSuite) TestCategorizeOutOfSpaceReplicas(c *C) {
	tests := []struct {
		name                   string
		replicas               []types.Replica
		replicaNoSpaceErrMap   map[string]int
		expectedRWReplicaCount int
		expectedNoSpaceMap     map[string]int
		expectedROReplicaList  []string
	}{
		{
			name:                   "empty replicas and error map",
			replicas:               []types.Replica{},
			replicaNoSpaceErrMap:   map[string]int{},
			expectedRWReplicaCount: 0,
			expectedNoSpaceMap:     map[string]int{},
			expectedROReplicaList:  []string{},
		},
		{
			name: "only RW replicas without errors",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap:   map[string]int{},
			expectedRWReplicaCount: 2,
			expectedNoSpaceMap:     map[string]int{},
			expectedROReplicaList:  []string{},
		},
		{
			name: "RW and WO replicas with some no space errors",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
			},
			expectedRWReplicaCount: 1,
			expectedNoSpaceMap: map[string]int{
				"1.1.1.1": 1,
			},
			expectedROReplicaList: []string{},
		},
		{
			name: "all writable replicas with no space errors",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
				"1.1.1.2": 1,
			},
			expectedRWReplicaCount: 1,
			expectedNoSpaceMap: map[string]int{
				"1.1.1.1": 1,
			},
			expectedROReplicaList: []string{"1.1.1.2"},
		},
		{
			name: "mixed modes with errors including non-existent replicas",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.WO},
				{Address: "1.1.1.3", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1":      1,
				"1.1.1.2":      1,
				"1.1.1.3":      1,
				"non.existent": 1, // Non-existent replica
			},
			expectedRWReplicaCount: 1,
			expectedNoSpaceMap: map[string]int{
				"1.1.1.1": 1,
			},
			expectedROReplicaList: []string{"1.1.1.2"},
		},
		{
			name: "only ERR replicas with no space errors",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.ERR},
				{Address: "1.1.1.2", Mode: types.ERR},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 1,
				"1.1.1.2": 1,
			},
			expectedRWReplicaCount: 0,
			expectedNoSpaceMap:     map[string]int{},
			expectedROReplicaList:  []string{},
		},
		{
			name: "various written bytes values for same replicas",
			replicas: []types.Replica{
				{Address: "1.1.1.1", Mode: types.RW},
				{Address: "1.1.1.2", Mode: types.RW},
				{Address: "1.1.1.3", Mode: types.WO},
			},
			replicaNoSpaceErrMap: map[string]int{
				"1.1.1.1": 5,
				"1.1.1.2": 3,
				"1.1.1.3": 5,
			},
			expectedRWReplicaCount: 2,
			expectedNoSpaceMap: map[string]int{
				"1.1.1.1": 5,
				"1.1.1.2": 3,
			},
			expectedROReplicaList: []string{"1.1.1.3"},
		},
	}

	readSource := makeByteSliceWithInitialData(1, 1)
	writeSource := makeByteSliceWithInitialData(1, 1)
	ctrl := &Controller{
		replicas:   []types.Replica{},
		VolumeName: "test-get-rw-replica-count",
		backend:    newMockReplicator(readSource, writeSource),
	}

	for _, tt := range tests {
		ctrl.replicas = tt.replicas

		// Call the method under test
		rwReplicaCount, noSpaceMap, roReplicaList := ctrl.categorizeOutOfSpaceReplicas(tt.replicaNoSpaceErrMap)

		// Check the results
		c.Assert(rwReplicaCount, Equals, tt.expectedRWReplicaCount, Commentf("Test case: %s - RW replica count mismatch", tt.name))
		c.Assert(len(noSpaceMap), Equals, len(tt.expectedNoSpaceMap), Commentf("Test case: %s - No space map length mismatch", tt.name))
		c.Assert(len(roReplicaList), Equals, len(tt.expectedROReplicaList), Commentf("Test case: %s - RO replica list length mismatch", tt.name))

		// Check the no space map contents
		for expectedAddr, expectedWB := range tt.expectedNoSpaceMap {
			actualWB, exists := noSpaceMap[expectedAddr]
			c.Assert(exists, Equals, true, Commentf("Test case: %s - Address %s not found in no space map", tt.name, expectedAddr))
			c.Assert(actualWB, Equals, expectedWB, Commentf("Test case: %s - Written bytes mismatch for address %s", tt.name, expectedAddr))
		}

		// Ensure no extra entries in the actual map
		for actualAddr := range noSpaceMap {
			_, exists := tt.expectedNoSpaceMap[actualAddr]
			c.Assert(exists, Equals, true, Commentf("Test case: %s - Unexpected address %s in no space map", tt.name, actualAddr))
		}

		// Check the RO replica list contents
		for i, expectedAddr := range tt.expectedROReplicaList {
			c.Assert(roReplicaList[i], Equals, expectedAddr, Commentf("Test case: %s - RO replica list mismatch at index %d", tt.name, i))
		}

	}
}

func (s *TestSuite) TestListReplicasToErrOnEnospc(c *C) {
	tests := []struct {
		name                      string
		replicaToErrOnNoSpaceMap  map[string]int
		expectedReplicasToErrList []string
	}{
		{
			name:                      "empty map should return empty list",
			replicaToErrOnNoSpaceMap:  map[string]int{},
			expectedReplicasToErrList: []string{},
		},
		{
			name: "single replica should return empty list (keep the only replica)",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 100,
			},
			expectedReplicasToErrList: []string{},
		},
		{
			name: "replicas with same written bytes should keep all",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 100,
				"1.1.1.2": 100,
				"1.1.1.3": 100,
			},
			expectedReplicasToErrList: []string{},
		},
		{
			name: "replicas with different written bytes should keep highest",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 50,
				"1.1.1.2": 100,
				"1.1.1.3": 75,
			},
			expectedReplicasToErrList: []string{
				"1.1.1.1",
				"1.1.1.3",
			},
		},
		{
			name: "multiple replicas with highest written bytes should keep all highest",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 100,
				"1.1.1.2": 100,
				"1.1.1.3": 75,
				"1.1.1.4": 50,
			},
			expectedReplicasToErrList: []string{
				"1.1.1.3",
				"1.1.1.4",
			},
		},
		{
			name: "multiple groups with same count - keep group with highest written bytes",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 100, // Group 1: 2 replicas with 100 bytes
				"1.1.1.2": 100,
				"1.1.1.3": 75, // Group 2: 2 replicas with 75 bytes
				"1.1.1.4": 75,
			},
			expectedReplicasToErrList: []string{
				"1.1.1.3",
				"1.1.1.4",
			},
		},
		{
			name: "complex scenario with multiple groups",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 200, // Group 1: 1 replica with 200 bytes
				"1.1.1.2": 150, // Group 2: 3 replicas with 150 bytes (largest group)
				"1.1.1.3": 150,
				"1.1.1.4": 150,
				"1.1.1.5": 100, // Group 3: 2 replicas with 100 bytes
				"1.1.1.6": 100,
			},
			expectedReplicasToErrList: []string{
				"1.1.1.1",
				"1.1.1.5",
				"1.1.1.6",
			},
		},
		{
			name: "zero written bytes should be handled correctly",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": 0, // Group 1: 2 replicas with 0 bytes
				"1.1.1.2": 0,
				"1.1.1.3": 50, // Group 2: 1 replica with 50 bytes
			},
			expectedReplicasToErrList: []string{
				"1.1.1.3",
			},
		},
		{
			name: "negative written bytes edge case",
			replicaToErrOnNoSpaceMap: map[string]int{
				"1.1.1.1": -10, // Group 1: 1 replica with -10 bytes
				"1.1.1.2": 0,   // Group 2: 2 replicas with 0 bytes (largest group)
				"1.1.1.3": 0,
				"1.1.1.4": 50, // Group 3: 1 replica with 50 bytes
			},
			expectedReplicasToErrList: []string{
				"1.1.1.1",
				"1.1.1.4",
			},
		},
	}

	for _, tt := range tests {
		// Call the function under test
		result := listReplicasToErrOnEnospc(tt.replicaToErrOnNoSpaceMap)

		// Check the length
		c.Assert(len(result), Equals, len(tt.expectedReplicasToErrList),
			Commentf("Test case: %s - Length mismatch", tt.name))

		// Convert result to map for easier comparison
		resultMap := make(map[string]bool)
		for _, addr := range result {
			resultMap[addr] = true
		}

		// Check each expected address is in the result
		for _, expectedAddr := range tt.expectedReplicasToErrList {
			c.Assert(resultMap[expectedAddr], Equals, true, Commentf("Test case: %s - Expected address %s not found in result", tt.name, expectedAddr))
		}

		// Check no unexpected addresses in the result
		for _, actualAddr := range result {
			found := false
			for _, expectedAddr := range tt.expectedReplicasToErrList {
				if actualAddr == expectedAddr {
					found = true
					break
				}
			}
			c.Assert(found, Equals, true, Commentf("Test case: %s - Unexpected address %s in result", tt.name, actualAddr))
		}
	}
}

func awaitLifecycleResult(c *C, result <-chan error) error {
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		c.Fatal("lifecycle operation did not complete")
		return nil
	}
}

func (s *TestSuite) TestExpansionRejectedDuringFrontendStartup(c *C) {
	// Model a block frontend waiting for device readiness outside the controller lock.
	controller := &Controller{size: 4096, frontend: &removalTestFrontend{state: types.StateDown}, frontendNeedsCleanup: true}
	err := controller.Expand(8192)
	c.Assert(err, ErrorMatches, "cannot expand during the frontend startup")
	c.Assert(controller.IsExpanding(), Equals, false)
	c.Assert(controller.Size(), Equals, int64(4096))
	// Like an in-progress expansion, the transient rejection is not recorded;
	// longhorn-manager retries the expansion while the size still mismatches.
	lastErr, failedAt := controller.GetExpansionErrorInfo()
	c.Assert(lastErr, Equals, "")
	c.Assert(failedAt, Equals, "")
}

type cleanupTestFrontend struct {
	types.Frontend
	shutdownErr error
	shutdowns   int
}

func (f *cleanupTestFrontend) FrontendName() string { return types.EngineFrontendBlockDev }
func (f *cleanupTestFrontend) State() types.State   { return types.StateDown }
func (f *cleanupTestFrontend) Shutdown() error {
	f.shutdowns++
	return f.shutdownErr
}

func (s *TestSuite) TestFrontendRetryPreservesFailedCleanup(c *C) {
	for _, name := range []string{types.EngineFrontendBlockDev, types.EngineFrontendISCSI} {
		c.Logf("Test case: %s", name)
		cleanupErr := errors.New("cleanup failed")
		f := &cleanupTestFrontend{shutdownErr: cleanupErr}
		controller := &Controller{frontend: f, frontendNeedsCleanup: true}
		err := controller.StartFrontend(name)
		c.Assert(errors.Is(err, cleanupErr), Equals, true)
		c.Assert(controller.frontend, Equals, f)
		c.Assert(controller.frontendNeedsCleanup, Equals, true)
		c.Assert(f.shutdowns, Equals, 1)
		f.shutdownErr = nil
		c.Assert(controller.StartFrontend(name), IsNil)
		c.Assert(controller.frontend, Not(Equals), f)
		c.Assert(controller.frontendNeedsCleanup, Equals, false)
		c.Assert(f.shutdowns, Equals, 2)
	}
}

type removalTestFrontend struct {
	types.Frontend
	state types.State
}

func (f *removalTestFrontend) State() types.State { return f.state }

type removalTestBackend struct{ types.Backend }

func (b *removalTestBackend) StopMonitoring() {}
func (b *removalTestBackend) Close() error    { return nil }

func (s *TestSuite) TestRemoveLastReplicaRejectedWhileFrontendStartedOrPending(c *C) {
	for _, tc := range []struct {
		name         string
		state        types.State
		needsCleanup bool
		rejected     bool
	}{
		{"frontend up", types.StateUp, true, true},
		{"frontend startup pending", types.StateDown, true, true},
		{"frontend down", types.StateDown, false, false},
	} {
		c.Logf("Test case: %s", tc.name)
		backend := &replicator{}
		backend.AddBackend("test", &removalTestBackend{}, types.RW)
		controller := &Controller{
			frontend:             &removalTestFrontend{state: tc.state},
			frontendNeedsCleanup: tc.needsCleanup,
			backend:              backend,
			replicas:             []types.Replica{{Address: "test", Mode: types.RW}},
		}

		err := controller.RemoveReplica("test")
		if tc.rejected {
			c.Assert(err, ErrorMatches, "cannot remove last replica if volume is up")
			c.Assert(len(controller.ListReplicas()), Equals, 1)
		} else {
			c.Assert(err, IsNil)
			c.Assert(len(controller.ListReplicas()), Equals, 0)
		}
	}
}

type startupTestBackend struct {
	removalTestBackend
	closed bool
}

func (b *startupTestBackend) Size() (int64, error)                        { return 4096, nil }
func (b *startupTestBackend) SectorSize() (int64, error)                  { return 512, nil }
func (b *startupTestBackend) GetState() (string, error)                   { return "open", nil }
func (b *startupTestBackend) ResetRebuild() error                         { return nil }
func (b *startupTestBackend) GetMonitorChannel() types.MonitorChannel     { return nil }
func (b *startupTestBackend) GetUnmapMarkSnapChainRemoved() (bool, error) { return false, nil }
func (b *startupTestBackend) IsRevisionCounterDisabled() (bool, error)    { return true, nil }
func (b *startupTestBackend) ReadAt(p []byte, _ int64) (int, error)       { return len(p), nil }
func (b *startupTestBackend) Close() error {
	b.closed = true
	return nil
}

type startupTestFactory struct {
	backends []*startupTestBackend
}

func (f *startupTestFactory) Create(string, string, types.DataServerProtocol, types.SharedTimeouts, bool, int64) (types.Backend, error) {
	backend := &startupTestBackend{}
	f.backends = append(f.backends, backend)
	return backend, nil
}

func (s *TestSuite) TestAddReplicaRejectedDuringFrontendStartup(c *C) {
	factory := &startupTestFactory{}
	// Model a block frontend waiting for device readiness outside the controller lock.
	controller := &Controller{
		factory:              factory,
		frontend:             &removalTestFrontend{state: types.StateDown},
		frontendNeedsCleanup: true,
		replicas:             []types.Replica{{Address: "test", Mode: types.RW}},
	}
	err := controller.AddReplica("new", false, true, types.WO)
	c.Assert(err, ErrorMatches, "cannot add replica during the frontend startup")
	c.Assert(len(factory.backends), Equals, 0)
	c.Assert(len(controller.ListReplicas()), Equals, 1)
}

type startupTestFrontend struct {
	types.Frontend
	failure        string
	err            error
	state          types.State
	shutdowns      int
	readinessCalls int
	controller     *Controller
}

func (f *startupTestFrontend) FrontendName() string { return types.EngineFrontendBlockDev }
func (f *startupTestFrontend) State() types.State   { return f.state }
func (f *startupTestFrontend) Init(string, int64, int64) error {
	if f.failure == "init" {
		return f.err
	}
	return nil
}
func (f *startupTestFrontend) Startup(types.ReaderWriterUnmapperAt) error {
	if f.failure == "startup" {
		return f.err
	}
	return nil
}
func (f *startupTestFrontend) Upgrade(string, int64, int64, types.ReaderWriterUnmapperAt) error {
	if f.failure == "upgrade" {
		return f.err
	}
	return nil
}
func (f *startupTestFrontend) WaitForDeviceReady() error {
	f.readinessCalls++
	// Login/scan READs must be served while startup waits for device readiness.
	if _, err := f.controller.ReadAt(make([]byte, 512), 0); err != nil {
		return err
	}
	if f.failure == "readiness" {
		return f.err
	}
	f.state = types.StateUp
	return nil
}
func (f *startupTestFrontend) Shutdown() error {
	f.shutdowns++
	f.state = types.StateDown
	return nil
}

func (s *TestSuite) TestStartFrontendFailureKeepsStartupState(c *C) {
	for _, failure := range []string{"", "init", "startup", "upgrade", "readiness"} {
		c.Logf("Test case: %s", failure)
		factory := &startupTestFactory{}
		f := &startupTestFrontend{failure: failure, err: errors.New("frontend failed"), state: types.StateDown}
		controller := &Controller{
			factory: factory, frontend: f, isUpgrade: failure == "upgrade",
			revisionCounterDisabled: true, metrics: &types.Metrics{},
		}
		f.controller = controller
		result := make(chan error, 1)
		go func() { result <- controller.Start(4096, 4096, "test") }()
		err := awaitLifecycleResult(c, result)
		if failure == "" {
			c.Assert(err, IsNil)
			c.Assert(f.State(), Equals, types.StateUp)
		} else {
			c.Assert(errors.Is(err, f.err), Equals, true)
			c.Assert(f.State(), Equals, types.StateDown)
		}
		// Like master, Start() does not tear down the frontend or backend on
		// failure, e.g., the in-use device of a live upgrade must be preserved.
		c.Assert(f.shutdowns, Equals, 0)
		c.Assert(factory.backends[0].closed, Equals, false)
		c.Assert(len(controller.ListReplicas()), Equals, 1)
		c.Assert(controller.frontendNeedsCleanup, Equals, true)
		if failure == "" || failure == "readiness" {
			c.Assert(f.readinessCalls, Equals, 1)
		} else {
			c.Assert(f.readinessCalls, Equals, 0)
		}
	}
}
