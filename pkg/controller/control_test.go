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
