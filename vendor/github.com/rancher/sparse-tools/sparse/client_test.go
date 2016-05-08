package sparse

import (
	"testing"

	"github.com/rancher/sparse-tools/log"
)

const localhost = "127.0.0.1"
const timeout = 5 //seconds
var remoteAddr = TCPEndPoint{localhost, 5000}

func TestSyncAnyFile(t *testing.T) {
	src := "src.bar"
	dst := "dst.bar"
	run := false

	if run {
		// Sync
		go TestServer(remoteAddr, timeout)
		_, err := SyncFile(src, remoteAddr, dst, timeout)

		// Verify
		if err != nil {
			t.Fatal("sync error")
		}
		if !filesAreEqual(src, dst) {
			t.Fatal("file content diverged")
		}
	}
}

func TestSyncFile1(t *testing.T) {
	// D H D => D D H
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile2(t *testing.T) {
	// H D H  => D H H
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile3(t *testing.T) {
	// D H D => D D
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile4(t *testing.T) {
	// H D H  => D H
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile5(t *testing.T) {
	// H D H  => H D
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile6(t *testing.T) {
	// H D H  => D
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile7(t *testing.T) {
	// H D H  => H
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile8(t *testing.T) {
	// D H D =>
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile9(t *testing.T) {
	// H D H  =>
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff1(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff2(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff3(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff4(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff5(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff6(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff7(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff8(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff9(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff10(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff11(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff12(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff13(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff14(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff15(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff16(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff17(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 28 * Blocks}},
		{SparseHole, Interval{28 * Blocks, 32 * Blocks}},
		{SparseData, Interval{32 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff18(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 28 * Blocks}},
		{SparseHole, Interval{28 * Blocks, 36 * Blocks}},
		{SparseData, Interval{36 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff19(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 31 * Blocks}},
		{SparseHole, Interval{31 * Blocks, 33 * Blocks}},
		{SparseData, Interval{33 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff20(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 32 * Blocks}},
		{SparseHole, Interval{32 * Blocks, 36 * Blocks}},
		{SparseData, Interval{36 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseHole, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff21(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 28 * Blocks}},
		{SparseData, Interval{28 * Blocks, 32 * Blocks}},
		{SparseHole, Interval{32 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff22(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 28 * Blocks}},
		{SparseData, Interval{28 * Blocks, 36 * Blocks}},
		{SparseHole, Interval{36 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff23(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 31 * Blocks}},
		{SparseData, Interval{31 * Blocks, 33 * Blocks}},
		{SparseHole, Interval{33 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff24(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 32 * Blocks}},
		{SparseData, Interval{32 * Blocks, 36 * Blocks}},
		{SparseHole, Interval{36 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseHole, Interval{34 * Blocks, 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncHash1(t *testing.T) {
	var hash1, hash2 []byte
	{
		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		}
		layoutRemote := layoutLocal
		hash1 = testSyncFile(t, layoutLocal, layoutRemote)
	}
	{

		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 3 * Blocks}},
		}
		layoutRemote := layoutLocal
		hash2 = testSyncFile(t, layoutLocal, layoutRemote)
	}
	if !isHashDifferent(hash1, hash2) {
		t.Fatal("Files with same data content but different layouts should have unique hashes")
	}
}

func TestSyncHash2(t *testing.T) {
	var hash1, hash2 []byte
	{
		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		}
		layoutRemote := []FileInterval{}
		hash1 = testSyncFile(t, layoutLocal, layoutRemote)
	}
	{

		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 3 * Blocks}},
		}
		layoutRemote := []FileInterval{}
		hash2 = testSyncFile(t, layoutLocal, layoutRemote)
	}
	if !isHashDifferent(hash1, hash2) {
		t.Fatal("Files with same data content but different layouts should have unique hashes")
	}
}

func testSyncFile(t *testing.T, layoutLocal, layoutRemote []FileInterval) (hashLocal []byte) {
	localPath := tempFilePath("ssync-src-")
	remotePath := tempFilePath("ssync-dst-")
	// Only log errors
	log.LevelPush(log.LevelError)
	defer log.LevelPop()

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	// Create test files
	createTestSparseFile(localPath, layoutLocal)
	if len(layoutRemote) > 0 {
		// only create destination test file if layout is speciifed
		createTestSparseFile(remotePath, layoutRemote)
	}

	// Sync
	go TestServer(remoteAddr, timeout)
	hashLocal, err := SyncFile(localPath, remoteAddr, remotePath, timeout)

	// Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(localPath, remotePath) {
		t.Fatal("file content diverged")
	}
	return
}

// created in current dir for benchmark tests
var localBigPath = "ssync-src-file.bar"
var remoteBigPath = "ssync-dst-file.bar"

func Test_1G_cleanup(*testing.T) {
	// remove temporaries if the benchmarks below are not run
	filesCleanup(localBigPath, remoteBigPath)
}

func Benchmark_1G_InitFiles(b *testing.B) {
	// Setup files
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, (256 << 10) * Blocks}},
	}
	layoutRemote := []FileInterval{}

	filesCleanup(localBigPath, remoteBigPath)
	createTestSparseFile(localBigPath, layoutLocal)
	createTestSparseFile(remoteBigPath, layoutRemote)
}

func Benchmark_1G_SendFiles_Whole(b *testing.B) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	go TestServer(remoteAddr, timeout)
	_, err := SyncFile(localBigPath, remoteAddr, remoteBigPath, timeout)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_SendFiles_Diff(b *testing.B) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	go TestServer(remoteAddr, timeout)
	_, err := SyncFile(localBigPath, remoteAddr, remoteBigPath, timeout)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_CheckFiles(b *testing.B) {
	if !filesAreEqual(localBigPath, remoteBigPath) {
		b.Error("file content diverged")
		return
	}
	filesCleanup(localBigPath, remoteBigPath)
}
