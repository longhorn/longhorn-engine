package rpc

import (
	. "gopkg.in/check.v1"
)

// mockSyncOps records cumulative progress updates to simulate RebuildStatus.
type mockSyncOps struct {
	processedSize int64
	totalSize     int64
}

func (m *mockSyncOps) UpdateSyncFileProgress(size int64) {
	m.processedSize += size
}

func (m *mockSyncOps) progress() int {
	if m.totalSize == 0 {
		return 0
	}
	return int((float32(m.processedSize) / float32(m.totalSize)) * 100)
}

// TestProgressExceeds100WithoutTracker demonstrates the bug: when a file sync
// is retried without resetting progress, the reported progress exceeds 100%.
func (s *TestSuite) TestProgressExceeds100WithoutTracker(c *C) {
	mock := &mockSyncOps{totalSize: 1000}

	// Simulate syncing a single 1000-byte file directly (old code path).
	// First attempt syncs 600 bytes, then fails.
	mock.UpdateSyncFileProgress(600)
	c.Assert(mock.progress(), Equals, 60)

	// Retry: the file is synced from scratch, sending another 1000 bytes.
	// Without any reset, processedSize becomes 1600 -> 160%.
	mock.UpdateSyncFileProgress(1000)
	c.Assert(mock.progress() > 100, Equals, true) // BUG: 160%
}

// TestProgressStaysCorrectWithTracker demonstrates the fix: perFileProgressOps
// detaches on retry and a new tracker is created so progress never exceeds 100%.
func (s *TestSuite) TestProgressStaysCorrectWithTracker(c *C) {
	mock := &mockSyncOps{totalSize: 1000}
	tracker := &perFileProgressOps{SyncFileOperations: mock}

	// First attempt: sync 600 bytes, then fail.
	tracker.UpdateSyncFileProgress(600)
	c.Assert(mock.progress(), Equals, 60)

	// Retry: detach subtracts the 600 bytes and disconnects the old tracker.
	tracker.detach()
	c.Assert(mock.progress(), Equals, 0)
	c.Assert(mock.processedSize, Equals, int64(0))

	// Create a new tracker for the retry (as the production code does).
	tracker = &perFileProgressOps{SyncFileOperations: mock}

	// Second attempt: full 1000-byte sync succeeds.
	tracker.UpdateSyncFileProgress(1000)
	c.Assert(mock.progress(), Equals, 100)
}

// TestProgressMultipleFilesWithRetry simulates a realistic scenario with
// multiple files where one file needs a retry.
func (s *TestSuite) TestProgressMultipleFilesWithRetry(c *C) {
	// Total: fileA=400 + fileB=600 = 1000
	mock := &mockSyncOps{processedSize: 1, totalSize: 1001} // mirrors PrepareRebuild init

	trackerA := &perFileProgressOps{SyncFileOperations: mock}
	trackerB := &perFileProgressOps{SyncFileOperations: mock}

	// fileA syncs completely (400 bytes).
	trackerA.UpdateSyncFileProgress(400)
	c.Assert(mock.progress(), Equals, 40)

	// fileB partially syncs 300 bytes, then fails.
	trackerB.UpdateSyncFileProgress(300)
	c.Assert(mock.progress(), Equals, 70)

	// fileB retry: detach its contribution and create a new tracker.
	trackerB.detach()
	c.Assert(mock.progress(), Equals, 40)

	trackerB = &perFileProgressOps{SyncFileOperations: mock}

	// fileB syncs completely on second attempt (600 bytes).
	trackerB.UpdateSyncFileProgress(600)
	c.Assert(mock.progress(), Equals, 100)
}

// TestDetachIgnoresLateCallbacks verifies that after detach(), further
// UpdateSyncFileProgress calls on the old tracker are ignored.
func (s *TestSuite) TestDetachIgnoresLateCallbacks(c *C) {
	mock := &mockSyncOps{totalSize: 1000}
	tracker := &perFileProgressOps{SyncFileOperations: mock}

	tracker.UpdateSyncFileProgress(500)
	c.Assert(mock.processedSize, Equals, int64(500))

	tracker.detach()
	c.Assert(mock.processedSize, Equals, int64(0))

	// Late callback from previous ssync attempt — should be ignored.
	tracker.UpdateSyncFileProgress(200)
	c.Assert(mock.processedSize, Equals, int64(0))
}

// TestDetachIdempotent verifies that calling detach() twice does not double-subtract.
func (s *TestSuite) TestDetachIdempotent(c *C) {
	mock := &mockSyncOps{totalSize: 1000}
	tracker := &perFileProgressOps{SyncFileOperations: mock}

	tracker.UpdateSyncFileProgress(500)
	c.Assert(mock.processedSize, Equals, int64(500))

	tracker.detach()
	c.Assert(mock.processedSize, Equals, int64(0))

	// Second detach should be a no-op.
	tracker.detach()
	c.Assert(mock.processedSize, Equals, int64(0))
}
