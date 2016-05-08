package sparse

import (
	"testing"

	"github.com/rancher/sparse-tools/log"
)

func TestFoldLayout1(t *testing.T) {
	// D H D => D D H
	layoutFrom := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}

	layoutResult := foldLayout(layoutFrom, layoutTo)
	status := checkLayout(layoutResult, layoutModel)
	if !status {
		t.Fatal("Folded layout diverged")
	}
}

func TestFoldLayout2(t *testing.T) {
	// H D H  => D H H
	layoutFrom := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}

	layoutResult := foldLayout(layoutFrom, layoutTo)
	status := checkLayout(layoutResult, layoutModel)
	if !status {
		t.Fatal("Folded layout diverged")
	}
}

func TestFoldFile1(t *testing.T) {
	// D H D => D D H
	layoutFrom := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile2(t *testing.T) {
	// H D H  => D H H
	layoutFrom := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func foldLayout(from, to []FileInterval) []FileInterval {
	if len(from) != len(to) {
		log.Fatal("foldLayout: non equal length not implemented")
	}
	result := make([]FileInterval, len(to))
	copy(result, to)
	for i, interval := range from {
		if to[i].Interval != interval.Interval {
			log.Fatal("foldLayout: subinterval feature not implemented")
		}
		if interval.Kind == SparseData {
			result[i] = interval
		}
	}
	return result
}

func checkLayout(from, to []FileInterval) bool {
	if len(from) != len(to) {
		log.Error("checkLayout: non equal length:", len(from), len(to))
		return false
	}
	for i, interval := range from {
		if to[i] != interval {
			log.Error("checkayout: intervals not equal:", interval, to[i])
			return false
		}
	}
	return true
}

func testFoldFile(t *testing.T, layoutFrom, layoutTo []FileInterval) (hashLocal []byte) {
	localPath := tempFilePath("sfold-src-")
	remotePath := tempFilePath("sfold-dst-")

	// Only log errors
	log.LevelPush(log.LevelError)
	defer log.LevelPop()

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	// Create test files
	createTestSparseFile(localPath, layoutFrom)
	createTestSparseFile(remotePath, layoutTo)
	layoutResult := foldLayout(layoutFrom, layoutTo)

	// Fold
	err := FoldFile(localPath, remotePath)

	// Verify
	if err != nil {
		t.Fatal("Fold error:", err)
	}

	err = checkTestSparseFile(remotePath, layoutResult)
	if err != nil {
		t.Fatal("Folded content diverged:", err)
	}
	return
}
