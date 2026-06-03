package replica

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/longhorn/go-common-libs/wal"

	"github.com/longhorn/longhorn-engine/pkg/types"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

// readMeta loads <dir>/<file> as JSON.
func readJSONFile(t *testing.T, path string, v interface{}) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(v); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func mustJSONT(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestApplyCreateHeadIdempotent verifies CREATE_HEAD can be re-applied
// over an existing file without corruption.
func TestApplyCreateHeadIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	args := mustJSONT(t, CreateHeadArgs{
		HeadName: "volume-head-001.img",
		Size:     4096,
		Meta:     disk{Name: "volume-head-001.img", Parent: "volume-snap-foo.img"},
	})
	for i := 0; i < 3; i++ {
		if err := applyCreateHead(s, args); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		st, err := os.Stat(filepath.Join(dir, "volume-head-001.img"))
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() != 4096 {
			t.Fatalf("size: got %d", st.Size())
		}
	}
}

// TestApplyLinkAsSnapshotIdempotent verifies LINK_AS_SNAPSHOT is a no-op
// when dst is already hardlinked to src.
func TestApplyLinkAsSnapshotIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	src := "volume-head-000.img"
	dst := "volume-snap-foo.img"

	srcPath := filepath.Join(dir, src)
	if err := os.WriteFile(srcPath, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srcPath+diskutil.DiskMetadataSuffix, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}

	args := mustJSONT(t, LinkAsSnapshotArgs{SourceImage: src, DestSnap: dst})
	for i := 0; i < 3; i++ {
		if err := applyLinkAsSnapshot(s, args); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		same, err := sameInode(srcPath, filepath.Join(dir, dst))
		if err != nil || !same {
			t.Fatalf("iter %d: expected same inode (err=%v same=%v)", i, err, same)
		}
	}
}

// TestApplyDeleteOldHeadIdempotent verifies DELETE_OLD_HEAD is a no-op when
// nothing is left to delete.
func TestApplyDeleteOldHeadIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	args := mustJSONT(t, DeleteOldHeadArgs{HeadName: "volume-head-000.img"})
	if err := applyDeleteOldHead(s, args); err != nil {
		t.Fatal(err)
	}
	if err := applyDeleteOldHead(s, args); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryReplaysSnapshotCreateAfterPartialApply simulates a process
// kill during snapshot create, after the journal contains all intents but
// before any Apply has run. Recovery must roll the transaction forward to
// completion.
func TestRecoveryReplaysSnapshotCreateAfterPartialApply(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}

	// Initial state: a freshly-created replica with one head.
	const oldHead = "volume-head-000.img"
	const newHead = "volume-head-001.img"
	const newSnap = "volume-snap-mysnap.img"
	const size int64 = 4096

	initialInfo := Info{
		Size:       size,
		Head:       oldHead,
		SectorSize: 512,
	}
	if err := s.writeJSONAtomic(volumeMetaData, &initialInfo); err != nil {
		t.Fatal(err)
	}
	// Create the old head image + meta.
	if err := applyCreateHead(s, mustJSONT(t, CreateHeadArgs{
		HeadName: oldHead, Size: size,
		Meta: disk{Name: oldHead},
	})); err != nil {
		t.Fatal(err)
	}

	// Open journal and write a complete plan for SNAP_CREATE without
	// applying any step or committing. Then drop the fd + flock
	// abruptly to simulate SIGKILL.
	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	newInfo := initialInfo
	newInfo.Head = newHead
	newInfo.Parent = newSnap
	newInfo.Dirty = true
	tx, err := j.Begin(wal.OpSnapCreate, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(1, wal.ActionCreateHead, mustJSONT(t, CreateHeadArgs{
		HeadName: newHead, Size: size,
		Meta: disk{Name: newHead, Parent: newSnap},
	})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(2, wal.ActionLinkAsSnapshot, mustJSONT(t, LinkAsSnapshotArgs{
		SourceImage: oldHead, DestSnap: newSnap,
	})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(3, wal.ActionUpdateVolumeMeta, mustJSONT(t, UpdateVolumeMetaArgs{
		Info: newInfo,
	})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(4, wal.ActionUpdateSnapMeta, mustJSONT(t, UpdateSnapMetaArgs{
		SnapName: newSnap,
		Meta:     disk{Name: newSnap, UserCreated: true, Created: "2026-06-03T00:00:00Z"},
	})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(5, wal.ActionDeleteOldHead, mustJSONT(t, DeleteOldHeadArgs{
		HeadName: oldHead,
	})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Prepare(); err != nil {
		t.Fatal(err)
	}
	// Simulated SIGKILL: drop the fd + flock without commit. The Replica
	// would do this if the process was killed.
	closeJournalAbruptly(t, j)

	// Pre-recovery sanity: volume.meta is unchanged.
	var midInfo Info
	readJSONFile(t, filepath.Join(dir, volumeMetaData), &midInfo)
	if midInfo.Head != oldHead {
		t.Fatalf("pre-recovery volume.meta should still point to %q, got %q", oldHead, midInfo.Head)
	}

	// Recover.
	j2, err := recoverJournal(dir)
	if err != nil {
		t.Fatalf("recoverJournal: %v", err)
	}
	defer j2.Close()

	// Verify post-recovery state: new head + snap exist, volume.meta
	// points to new head, snap meta has UserCreated, old head is gone.
	var finalInfo Info
	readJSONFile(t, filepath.Join(dir, volumeMetaData), &finalInfo)
	if finalInfo.Head != newHead || finalInfo.Parent != newSnap || !finalInfo.Dirty {
		t.Fatalf("post-recovery volume.meta: %+v", finalInfo)
	}
	if _, err := os.Stat(filepath.Join(dir, newHead)); err != nil {
		t.Fatalf("new head missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, newSnap)); err != nil {
		t.Fatalf("new snap missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, oldHead)); err == nil {
		t.Fatalf("old head should have been deleted by recovery")
	}
	var snapMeta disk
	readJSONFile(t, filepath.Join(dir, newSnap+diskutil.DiskMetadataSuffix), &snapMeta)
	if !snapMeta.UserCreated {
		t.Fatalf("snap meta UserCreated not set: %+v", snapMeta)
	}

	// Journal should be at a clean, empty state after checkpoint.
	sz, err := j2.Size()
	if err != nil {
		t.Fatal(err)
	}
	if sz != 0 {
		t.Fatalf("expected journal truncated after recovery, size=%d", sz)
	}
}

// TestRecoveryAbortsEmptyTransaction covers the case where the process
// crashed after TXN_BEGIN but before any INTENT was written. Recovery
// must safely discard the transaction.
func TestRecoveryAbortsEmptyTransaction(t *testing.T) {
	dir := t.TempDir()

	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Begin(wal.OpSnapCreate, nil); err != nil {
		t.Fatal(err)
	}
	closeJournalAbruptly(t, j)

	j2, err := recoverJournal(dir)
	if err != nil {
		t.Fatalf("recoverJournal: %v", err)
	}
	defer j2.Close()

	sz, _ := j2.Size()
	if sz != 0 {
		t.Fatalf("expected empty journal after recovery, size=%d", sz)
	}
}

// TestSnapshotCreateThroughReplicaIsCrashSafe drives a real *Replica via
// Snapshot() and then re-opens the directory, asserting the chain is in
// the expected committed state. This is the happy path under journaling.
func TestSnapshotCreateThroughReplicaIsCrashSafe(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("s1", true, "now", nil); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — recovery should be a no-op and chain should be intact.
	r2, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()

	if r2.info.Head != "volume-head-001.img" {
		t.Fatalf("expected head volume-head-001.img, got %q", r2.info.Head)
	}
	if !strings.HasPrefix(r2.info.Parent, "volume-snap-s1") {
		t.Fatalf("expected parent volume-snap-s1*, got %q", r2.info.Parent)
	}
	if _, ok := r2.diskData[r2.info.Parent]; !ok {
		t.Fatalf("snap not in diskData: %v", r2.diskData)
	}
}

// closeJournalAbruptly drops the fd and flock without writing
// CHECKPOINT, simulating an abrupt process death.
func closeJournalAbruptly(t *testing.T, j *wal.Journal) {
	t.Helper()
	if err := journalForceClose(j); err != nil {
		t.Fatalf("forceClose: %v", err)
	}
}

// journalForceClose is a small reflection-free helper that uses the
// package-internal accessor. We can't reach into the journal struct
// directly from this package, so this is implemented in the journal
// package itself as wal.ForceCloseForTest.
func journalForceClose(j *wal.Journal) error {
	return wal.ForceCloseForTest(j)
}// TestApplyRmDiskIdempotent verifies RM_DISK is a no-op when files are
// already absent and removes them cleanly when present.
func TestApplyRmDiskIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	name := "volume-snap-foo.img"

	// Run on missing files: must succeed.
	if err := applyRmDisk(s, mustJSONT(t, RmDiskArgs{Name: name})); err != nil {
		t.Fatalf("rm on missing: %v", err)
	}

	// Create files then run; must remove all three.
	for _, suf := range []string{"", diskutil.DiskMetadataSuffix, diskutil.DiskChecksumSuffix} {
		if err := os.WriteFile(filepath.Join(dir, name+suf), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := applyRmDisk(s, mustJSONT(t, RmDiskArgs{Name: name})); err != nil {
		t.Fatalf("rm on present: %v", err)
	}
	for _, suf := range []string{"", diskutil.DiskMetadataSuffix, diskutil.DiskChecksumSuffix} {
		if _, err := os.Stat(filepath.Join(dir, name+suf)); err == nil {
			t.Fatalf("file %v should be gone", name+suf)
		}
	}

	// Run again on already-absent files: must succeed.
	if err := applyRmDisk(s, mustJSONT(t, RmDiskArgs{Name: name})); err != nil {
		t.Fatalf("rm second pass: %v", err)
	}
}

// TestRecoveryReplaysRemoveDiffDisk simulates a crash mid-RemoveDiffDisk:
// all intents written, no Apply executed, then SIGKILL. Recovery must
// finish the operation.
func TestRecoveryReplaysRemoveDiffDisk(t *testing.T) {
	dir := t.TempDir()

	// Build a chain on disk: snap-000 <- snap-001 <- head-002.
	r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("000", true, "now", nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("001", true, "now", nil); err != nil {
		t.Fatal(err)
	}
	const removeName = "volume-snap-001.img"
	const childName = "volume-head-002.img"

	// Compute the new child meta as RemoveDiffDisk would.
	gp := r.diskData[removeName].Parent
	newChildMeta := *r.diskData[childName]
	newChildMeta.Parent = gp

	// Reach into the open journal and write the plan, then drop fd+flock
	// without commit, then close the replica abruptly.
	tx, err := r.wal.Begin(wal.OpSnapRemove, mustJSONT(t, SnapRemoveParams{
		Name: removeName, Child: childName,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(1, wal.ActionUpdateSnapMeta, mustJSONT(t,
		UpdateSnapMetaArgs{SnapName: childName, Meta: newChildMeta})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(2, wal.ActionRmDisk, mustJSONT(t,
		RmDiskArgs{Name: removeName})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Prepare(); err != nil {
		t.Fatal(err)
	}
	// Simulate SIGKILL: drop the journal abruptly. Replica struct stays
	// in memory but its files on disk are exactly as a kill would leave
	// them.
	if err := wal.ForceCloseForTest(r.wal); err != nil {
		t.Fatal(err)
	}
	r.wal = nil

	// Sanity: removeName still exists on disk.
	if _, err := os.Stat(filepath.Join(dir, removeName)); err != nil {
		t.Fatalf("pre-recovery: %v should still exist (%v)", removeName, err)
	}

	// Reopen — recovery should replay the plan.
	r2, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()

	if _, err := os.Stat(filepath.Join(dir, removeName)); err == nil {
		t.Fatalf("post-recovery: %v should be deleted", removeName)
	}
	if r2.diskData[childName].Parent != gp {
		t.Fatalf("child parent: want %q got %q", gp, r2.diskData[childName].Parent)
	}
	if _, ok := r2.diskData[removeName]; ok {
		t.Fatalf("%v should be gone from diskData", removeName)
	}
}

// TestRecoveryReplaysRevert simulates a crash mid-Revert: all intents
// written, no Apply, then SIGKILL. Recovery must rotate to the new head.
func TestRecoveryReplaysRevert(t *testing.T) {
	dir := t.TempDir()

	r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("000", true, "now", nil); err != nil {
		t.Fatal(err)
	}
	const parent = "volume-snap-000.img"
	const oldHead = "volume-head-001.img"
	const newHead = "volume-head-002.img"

	if r.info.Head != oldHead {
		t.Fatalf("expected head %q, got %q", oldHead, r.info.Head)
	}

	newHeadDisk := disk{Parent: parent, Name: newHead, Created: "now"}
	newInfo := r.info
	newInfo.Head = newHead
	newInfo.Parent = parent
	newInfo.Dirty = true

	tx, err := r.wal.Begin(wal.OpSnapRevert, mustJSONT(t, SnapRevertParams{
		Parent: parent, OldHead: oldHead, NewHead: newHead,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(1, wal.ActionCreateHead, mustJSONT(t,
		CreateHeadArgs{HeadName: newHead, Size: 4096, Meta: newHeadDisk})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(2, wal.ActionUpdateVolumeMeta, mustJSONT(t,
		UpdateVolumeMetaArgs{Info: newInfo})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Intent(3, wal.ActionDeleteOldHead, mustJSONT(t,
		DeleteOldHeadArgs{HeadName: oldHead})); err != nil {
		t.Fatal(err)
	}
	if err := tx.Prepare(); err != nil {
		t.Fatal(err)
	}
	if err := wal.ForceCloseForTest(r.wal); err != nil {
		t.Fatal(err)
	}
	r.wal = nil

	if _, err := os.Stat(filepath.Join(dir, oldHead)); err != nil {
		t.Fatalf("pre-recovery: %v should exist", oldHead)
	}

	r2, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()

	if r2.info.Head != newHead {
		t.Fatalf("expected head %q after recovery, got %q", newHead, r2.info.Head)
	}
	if !strings.HasPrefix(r2.info.Parent, parent) {
		t.Fatalf("expected parent %q, got %q", parent, r2.info.Parent)
	}
	if _, err := os.Stat(filepath.Join(dir, newHead)); err != nil {
		t.Fatalf("new head missing post-recovery: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, oldHead)); err == nil {
		t.Fatalf("old head should be gone post-recovery")
	}
}

// TestRevertThroughReplicaIsCrashSafe drives a real Revert, closes,
// and reopens; the chain must reflect the revert.
func TestRevertThroughReplicaIsCrashSafe(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("base", true, "now", nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("after", true, "now", nil); err != nil {
		t.Fatal(err)
	}

	target := "volume-snap-base.img"
	r2, err := r.Revert(target, "now2")
	if err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if err := r2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r3, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r3.Close()

	if r3.info.Parent != target {
		t.Fatalf("expected parent %q, got %q", target, r3.info.Parent)
	}
}

// TestRemoveThroughReplicaIsCrashSafe drives a real RemoveDiffDisk and
// reopens; the snapshot must be gone and child rewired.
func TestRemoveThroughReplicaIsCrashSafe(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("a", true, "now", nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Snapshot("b", true, "now", nil); err != nil {
		t.Fatal(err)
	}

	if err := r.RemoveDiffDisk("volume-snap-a.img", false); err != nil {
		t.Fatalf("RemoveDiffDisk: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r2, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()

	if _, ok := r2.diskData["volume-snap-a.img"]; ok {
		t.Fatalf("snap-a should be gone")
	}
	bMeta, ok := r2.diskData["volume-snap-b.img"]
	if !ok {
		t.Fatalf("snap-b missing")
	}
	if bMeta.Parent != "" {
		t.Fatalf("snap-b parent should be empty, got %q", bMeta.Parent)
	}
}
