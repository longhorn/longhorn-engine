package replica

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/longhorn/longhorn-engine/pkg/types"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

// newTestReplica opens a fresh journaled replica in a temp dir.
func newTestReplica(t *testing.T, dir string) *Replica {
	t.Helper()
	r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// TestApplyLinkAsSnapshotRemovesStaleChecksumWhenAlreadyLinked exercises the
// already-linked fast path of applyLinkAsSnapshot: even when both hardlinks
// are already correct, a stale checksum left by a prior partial attempt must
// be removed and the directory fsynced before returning.
func TestApplyLinkAsSnapshotRemovesStaleChecksumWhenAlreadyLinked(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	src := "volume-head-000.img"
	dst := "volume-snap-foo.img"

	srcImg := filepath.Join(dir, src)
	if err := os.WriteFile(srcImg, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srcImg+diskutil.DiskMetadataSuffix, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}

	// Pre-establish the correct hardlinks so the fast path is taken.
	if err := os.Link(srcImg, filepath.Join(dir, dst)); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(srcImg+diskutil.DiskMetadataSuffix, filepath.Join(dir, dst+diskutil.DiskMetadataSuffix)); err != nil {
		t.Fatal(err)
	}

	// A stale checksum from a prior attempt must not survive the relink.
	staleChecksum := filepath.Join(dir, dst+diskutil.DiskChecksumSuffix)
	if err := os.WriteFile(staleChecksum, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}

	args := mustJSONT(t, LinkAsSnapshotArgs{SourceImage: src, DestSnap: dst})
	if err := applyLinkAsSnapshot(s, args); err != nil {
		t.Fatalf("applyLinkAsSnapshot: %v", err)
	}

	if _, err := os.Stat(staleChecksum); !os.IsNotExist(err) {
		t.Fatalf("stale checksum should have been removed, stat err=%v", err)
	}
	same, err := sameInode(srcImg, filepath.Join(dir, dst))
	if err != nil || !same {
		t.Fatalf("dst should stay hardlinked to src (err=%v same=%v)", err, same)
	}
}

// TestApplyUpdateVolumeMetaIdempotent verifies UPDATE_VOLUME_META can be
// re-applied and always leaves the same durable volume.meta.
func TestApplyUpdateVolumeMetaIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	info := Info{Size: 4096, Head: "volume-head-001.img", Parent: "volume-snap-x.img", SectorSize: 512, Dirty: true}
	args := mustJSONT(t, UpdateVolumeMetaArgs{Info: info})
	for i := 0; i < 3; i++ {
		if err := applyUpdateVolumeMeta(s, args); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		var got Info
		readJSONFile(t, filepath.Join(dir, volumeMetaData), &got)
		if got != info {
			t.Fatalf("iter %d: got %+v want %+v", i, got, info)
		}
	}
}

// TestApplyUpdateSnapMetaIdempotent verifies UPDATE_SNAP_META can be
// re-applied and always leaves the same durable snapshot .meta.
func TestApplyUpdateSnapMetaIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := stepDir{dir: dir}
	snap := "volume-snap-x.img"
	meta := disk{Name: snap, Parent: "volume-snap-y.img", UserCreated: true, Created: "now"}
	args := mustJSONT(t, UpdateSnapMetaArgs{SnapName: snap, Meta: meta})
	for i := 0; i < 3; i++ {
		if err := applyUpdateSnapMeta(s, args); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		var got disk
		readJSONFile(t, filepath.Join(dir, snap+diskutil.DiskMetadataSuffix), &got)
		if got.Name != meta.Name || got.Parent != meta.Parent ||
			got.UserCreated != meta.UserCreated || got.Created != meta.Created {
			t.Fatalf("iter %d: got %+v want %+v", i, got, meta)
		}
	}
}

// TestPoisonedReplicaRejectsMutators verifies that once a replica is
// poisoned (a prepared journal transaction could not be completed in this
// process), every chain mutator is rejected with errReplicaPoisoned. The
// replica runs on a real WAL journal created by New.
func TestPoisonedReplicaRejectsMutators(t *testing.T) {
	dir := t.TempDir()
	r := newTestReplica(t, dir)
	defer func() { _ = r.Close() }()

	if err := r.Snapshot("a", true, "now", nil); err != nil {
		t.Fatalf("Snapshot a: %v", err)
	}
	if err := r.Snapshot("b", true, "now", nil); err != nil {
		t.Fatalf("Snapshot b: %v", err)
	}

	r.poison()
	if !r.poisoned {
		t.Fatal("poison() should set poisoned")
	}
	if r.wal != nil {
		t.Fatal("poison() should release the journal")
	}

	checks := []struct {
		name string
		call func() error
	}{
		{"Snapshot", func() error { return r.Snapshot("c", true, "now", nil) }},
		{"Revert", func() error { _, err := r.Revert("volume-snap-a.img", "now"); return err }},
		{"RemoveDiffDisk", func() error { return r.RemoveDiffDisk("volume-snap-a.img", false) }},
		{"MarkDiskAsRemoved", func() error { return r.MarkDiskAsRemoved("volume-snap-a.img") }},
		{"ReplaceDisk", func() error { return r.ReplaceDisk("volume-snap-a.img", "volume-snap-b.img") }},
		{"SetRebuilding", func() error { return r.SetRebuilding(true) }},
		{"Expand", func() error { return r.Expand(r.info.Size * 2) }},
	}
	for _, c := range checks {
		if err := c.call(); !errors.Is(err, errReplicaPoisoned) {
			t.Errorf("%s: expected errReplicaPoisoned, got %v", c.name, err)
		}
	}
}

// TestPoisonedReplicaSkipsVolumeMetaWriteOnClose verifies that Close on a
// poisoned replica does not overwrite the on-disk volume.meta with the
// stale in-memory Info. Otherwise a durable, journal-advanced volume.meta
// would be clobbered and recovery would skip the completed update.
func TestPoisonedReplicaSkipsVolumeMetaWriteOnClose(t *testing.T) {
	dir := t.TempDir()
	r := newTestReplica(t, dir)

	// Simulate a durable transaction that already advanced volume.meta on
	// disk to a head the in-memory Info does not know about.
	advanced := r.info
	advanced.Head = "volume-head-777.img"
	advanced.Dirty = true
	s := stepDir{dir: dir}
	if err := s.writeJSONAtomic(volumeMetaData, &advanced); err != nil {
		t.Fatal(err)
	}

	r.poison()

	if err := r.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var got Info
	readJSONFile(t, filepath.Join(dir, volumeMetaData), &got)
	if got.Head != "volume-head-777.img" {
		t.Fatalf("poisoned Close clobbered volume.meta: got Head=%q want volume-head-777.img", got.Head)
	}
}

// TestApplyFailurePoisonsReplicaAndRecovers is an end-to-end test driven
// through a real *Replica and its real WAL journal. It induces a genuine
// post-PREPARE apply failure during Revert by making volume.meta's tmp
// write target un-creatable, then verifies:
//   - Revert fails and the replica becomes poisoned,
//   - a subsequent mutator is rejected,
//   - reconstruction replays the pending transaction to completion.
func TestApplyFailurePoisonsReplicaAndRecovers(t *testing.T) {
	dir := t.TempDir()
	r := newTestReplica(t, dir)

	if err := r.Snapshot("base", true, "now", nil); err != nil {
		t.Fatalf("Snapshot base: %v", err)
	}
	const target = "volume-snap-base.img"
	const oldHead = "volume-head-001.img"
	const newHead = "volume-head-002.img"
	if r.info.Head != oldHead {
		t.Fatalf("expected head %q, got %q", oldHead, r.info.Head)
	}

	// Block the UPDATE_VOLUME_META step (step 2 of revert): writeJSONAtomic
	// creates "volume.meta.tmp" first, so a directory at that path makes
	// os.Create fail after CREATE_HEAD (step 1) has already succeeded.
	blocker := filepath.Join(dir, volumeMetaData+tmpFileSuffix)
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Revert(target, "now2"); err == nil {
		t.Fatal("expected Revert to fail on blocked volume.meta write")
	}
	if !r.poisoned {
		t.Fatal("failed Revert should poison the replica")
	}
	if err := r.Snapshot("c", true, "now", nil); !errors.Is(err, errReplicaPoisoned) {
		t.Fatalf("poisoned replica should reject Snapshot, got %v", err)
	}

	// The partial state must be durable: new head exists, volume.meta still
	// points at the old head (step 2 never completed).
	if _, err := os.Stat(filepath.Join(dir, newHead)); err != nil {
		t.Fatalf("CREATE_HEAD (step 1) should have produced %q: %v", newHead, err)
	}
	var midInfo Info
	readJSONFile(t, filepath.Join(dir, volumeMetaData), &midInfo)
	if midInfo.Head != oldHead {
		t.Fatalf("pre-recovery volume.meta should still point at %q, got %q", oldHead, midInfo.Head)
	}

	// Clear the blocker and reconstruct: recovery must roll the prepared
	// revert transaction forward to completion.
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	r2 := newTestReplica(t, dir)
	defer func() { _ = r2.Close() }()

	if r2.info.Head != newHead {
		t.Fatalf("post-recovery head: want %q got %q", newHead, r2.info.Head)
	}
	if r2.info.Parent != target {
		t.Fatalf("post-recovery parent: want %q got %q", target, r2.info.Parent)
	}
	if _, err := os.Stat(filepath.Join(dir, oldHead)); err == nil {
		t.Fatalf("DELETE_OLD_HEAD should have removed %q", oldHead)
	}
}

// TestPostCommitOpenHeadFailurePoisonsReplicaAndRecovers verifies that when
// the new head image cannot be opened for I/O after the snapshot-create
// transaction has already committed, the replica is poisoned (the on-disk
// chain advanced past the stale in-memory view), rejects further mutators,
// and reconstruction reads the committed state.
func TestPostCommitOpenHeadFailurePoisonsReplicaAndRecovers(t *testing.T) {
	dir := t.TempDir()
	r := newTestReplica(t, dir)

	const oldHead = "volume-head-000.img"
	const newHead = "volume-head-001.img"
	const newSnap = "volume-snap-s1.img"
	if r.info.Head != oldHead {
		t.Fatalf("expected initial head %q, got %q", oldHead, r.info.Head)
	}

	// Force the post-commit open of the new head to fail.
	orig := openNewHeadForIO
	openNewHeadForIO = func(_ *Replica, _ string) (types.DiffDisk, error) {
		return nil, errors.New("injected open failure")
	}
	restored := false
	restore := func() {
		if !restored {
			openNewHeadForIO = orig
			restored = true
		}
	}
	defer restore()

	if err := r.Snapshot("s1", true, "now", nil); err == nil {
		t.Fatal("expected Snapshot to fail on blocked new-head open")
	}
	if !r.poisoned {
		t.Fatal("post-commit open failure should poison the replica")
	}
	if err := r.Snapshot("s2", true, "now", nil); !errors.Is(err, errReplicaPoisoned) {
		t.Fatalf("poisoned replica should reject Snapshot, got %v", err)
	}

	// The transaction committed: the new head and snapshot exist, volume.meta
	// points at the new head, and the old head is gone.
	if _, err := os.Stat(filepath.Join(dir, newHead)); err != nil {
		t.Fatalf("committed txn should have produced %q: %v", newHead, err)
	}
	var midInfo Info
	readJSONFile(t, filepath.Join(dir, volumeMetaData), &midInfo)
	if midInfo.Head != newHead {
		t.Fatalf("committed volume.meta should point at %q, got %q", newHead, midInfo.Head)
	}
	if _, err := os.Stat(filepath.Join(dir, oldHead)); err == nil {
		t.Fatalf("committed txn should have removed old head %q", oldHead)
	}

	// Reconstruct: the replica must read the committed chain.
	restore()
	r2 := newTestReplica(t, dir)
	defer func() { _ = r2.Close() }()

	if r2.info.Head != newHead {
		t.Fatalf("post-recovery head: want %q got %q", newHead, r2.info.Head)
	}
	if r2.info.Parent != newSnap {
		t.Fatalf("post-recovery parent: want %q got %q", newSnap, r2.info.Parent)
	}
}

// TestPostCommitReloadFailurePoisonsReplicaAndRecovers verifies that when the
// post-commit Reload of a revert transaction fails, the replica is poisoned
// (the on-disk chain advanced to the new head while the in-memory state still
// reflects the old head and the WAL was released), rejects further mutators,
// and reconstruction reads the committed reverted chain.
func TestPostCommitReloadFailurePoisonsReplicaAndRecovers(t *testing.T) {
	dir := t.TempDir()
	r := newTestReplica(t, dir)

	if err := r.Snapshot("base", true, "now", nil); err != nil {
		t.Fatalf("Snapshot base: %v", err)
	}
	const target = "volume-snap-base.img"
	const oldHead = "volume-head-001.img"
	const newHead = "volume-head-002.img"
	if r.info.Head != oldHead {
		t.Fatalf("expected head %q, got %q", oldHead, r.info.Head)
	}

	// Force the post-commit reload to fail.
	orig := reloadAfterRevert
	reloadAfterRevert = func(_ *Replica) (*Replica, error) {
		return nil, errors.New("injected reload failure")
	}
	restored := false
	restore := func() {
		if !restored {
			reloadAfterRevert = orig
			restored = true
		}
	}
	defer restore()

	if _, err := r.Revert(target, "now2"); err == nil {
		t.Fatal("expected Revert to fail on blocked reload")
	}
	if !r.poisoned {
		t.Fatal("post-commit reload failure should poison the replica")
	}
	if err := r.Snapshot("c", true, "now", nil); !errors.Is(err, errReplicaPoisoned) {
		t.Fatalf("poisoned replica should reject Snapshot, got %v", err)
	}

	// The revert committed: the new head exists, volume.meta points at it,
	// and the old head is gone.
	if _, err := os.Stat(filepath.Join(dir, newHead)); err != nil {
		t.Fatalf("committed revert should have produced %q: %v", newHead, err)
	}
	var midInfo Info
	readJSONFile(t, filepath.Join(dir, volumeMetaData), &midInfo)
	if midInfo.Head != newHead {
		t.Fatalf("committed volume.meta should point at %q, got %q", newHead, midInfo.Head)
	}
	if _, err := os.Stat(filepath.Join(dir, oldHead)); err == nil {
		t.Fatalf("committed revert should have removed old head %q", oldHead)
	}

	// Reconstruct: the replica must read the committed reverted chain.
	restore()
	r2 := newTestReplica(t, dir)
	defer func() { _ = r2.Close() }()

	if r2.info.Head != newHead {
		t.Fatalf("post-recovery head: want %q got %q", newHead, r2.info.Head)
	}
	if r2.info.Parent != target {
		t.Fatalf("post-recovery parent: want %q got %q", target, r2.info.Parent)
	}
}
