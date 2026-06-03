package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/longhorn/go-common-libs/wal"

	"github.com/longhorn/longhorn-engine/pkg/types"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

// fingerprint captures the durable on-disk state of a replica directory
// in a way two states can be compared for "logically equal".
//
// Image files are summarised by size+sha256 (we only look at the first
// 64 KiB to keep tests fast; all images in these tests are zero-filled
// <= 4 KiB).
// .meta and volume.meta are decoded as JSON and re-encoded canonically
// so trivial whitespace differences don't matter.
type fingerprint struct {
	Files map[string]string // name -> "size:hash" or "json:<canonical>"
	Info  Info
}

func fingerprintDir(t *testing.T, dir string) fingerprint {
	t.Helper()
	fp := fingerprint{Files: map[string]string{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Ignore the journal itself; its content varies by recovery
		// path even when the on-disk replica is identical. Also ignore
		// the lock file (presence depends on whether a Journal was
		// open at fingerprint time) and any quarantined broken files.
		if name == wal.FileName || name == wal.LockFileName ||
			strings.HasPrefix(name, wal.FileName+".broken-") ||
			strings.HasSuffix(name, tmpFileSuffix) {
			continue
		}
		path := filepath.Join(dir, name)
		if name == volumeMetaData {
			var info Info
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.NewDecoder(f).Decode(&info); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			_ = f.Close()
			fp.Info = info
			continue
		}
		if strings.HasSuffix(name, diskutil.DiskMetadataSuffix) {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var d disk
			if err := json.Unmarshal(b, &d); err != nil {
				t.Fatalf("decode %s: %v", name, err)
			}
			canon, _ := json.Marshal(d)
			fp.Files[name] = "json:" + string(canon)
			continue
		}
		// Image / checksum / other -- size + content hash.
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.CopyN(h, f, 1<<16); err != nil && err != io.EOF {
			_ = f.Close()
			t.Fatal(err)
		}
		_ = f.Close()
		fp.Files[name] = fmt.Sprintf("size:%d:hash:%s", st.Size(), hex.EncodeToString(h.Sum(nil)))
	}
	return fp
}

func fpEqual(a, b fingerprint) bool {
	if a.Info != b.Info {
		return false
	}
	if len(a.Files) != len(b.Files) {
		return false
	}
	for k, v := range a.Files {
		if b.Files[k] != v {
			return false
		}
	}
	return true
}

func fpDiff(a, b fingerprint) string {
	keys := map[string]bool{}
	for k := range a.Files {
		keys[k] = true
	}
	for k := range b.Files {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var out strings.Builder
	if a.Info != b.Info {
		fmt.Fprintf(&out, "Info: %+v != %+v\n", a.Info, b.Info)
	}
	for _, k := range sorted {
		va, vb := a.Files[k], b.Files[k]
		if va != vb {
			fmt.Fprintf(&out, "  %s:\n    a=%s\n    b=%s\n", k, va, vb)
		}
	}
	return out.String()
}

// stepPlan is a single journaled step.
type stepPlan struct {
	id     uint32
	action wal.Action
	args   []byte
}

// driveCrash opens the journal in dir, writes Begin + the first
// `intents` plan entries. If `prepared` is true, also writes the
// PREPARE record after intents (otherwise the txn looks torn). Then
// applies + STEP_DONEs the first `applied` of them (must be <=
// intents). If commit is true, writes TXN_COMMIT and Closes cleanly;
// otherwise force-closes without commit (SIGKILL).
func driveCrash(t *testing.T, dir string, op wal.Op, plan []stepPlan, intents, applied int, prepared, commit bool) {
	t.Helper()
	if applied > intents {
		t.Fatalf("invalid crash point: applied=%d > intents=%d", applied, intents)
	}
	if applied > 0 && !prepared {
		t.Fatalf("invalid crash point: applied=%d > 0 requires prepared", applied)
	}
	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := j.Begin(op, nil)
	if err != nil {
		_ = wal.ForceCloseForTest(j)
		t.Fatal(err)
	}
	for i := 0; i < intents; i++ {
		if err := tx.Intent(plan[i].id, plan[i].action, plan[i].args); err != nil {
			_ = wal.ForceCloseForTest(j)
			t.Fatal(err)
		}
	}
	if prepared {
		if err := tx.Prepare(); err != nil {
			_ = wal.ForceCloseForTest(j)
			t.Fatal(err)
		}
	}
	s := stepDir{dir: dir}
	for i := 0; i < applied; i++ {
		fn := stepRegistry[plan[i].action]
		if err := fn(s, plan[i].args); err != nil {
			_ = wal.ForceCloseForTest(j)
			t.Fatalf("apply step %d: %v", plan[i].id, err)
		}
		if err := tx.StepDone(plan[i].id); err != nil {
			_ = wal.ForceCloseForTest(j)
			t.Fatal(err)
		}
	}
	if commit {
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := wal.ForceCloseForTest(j); err != nil {
		t.Fatal(err)
	}
}

// recoverAndFingerprint runs recoverJournal on dir, closes the returned
// journal, then captures a fingerprint of the directory.
func recoverAndFingerprint(t *testing.T, dir string) fingerprint {
	t.Helper()
	j, err := recoverJournal(dir)
	if err != nil {
		t.Fatalf("recoverJournal: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}
	return fingerprintDir(t, dir)
}

// snapshotCreatePlan builds the same plan that *Replica.createDisk uses
// for snap "name" given a starting Info+oldHead state.
func snapshotCreatePlan(t *testing.T, info Info, oldHead, snapName string) (newInfo Info, plan []stepPlan) {
	t.Helper()
	newHead := "volume-head-001.img"
	newSnap := "volume-snap-" + snapName + ".img"
	newInfo = info
	newInfo.Head = newHead
	newInfo.Parent = newSnap
	newInfo.Dirty = true
	plan = []stepPlan{
		{1, wal.ActionCreateHead, mustJSONT(t, CreateHeadArgs{
			HeadName: newHead, Size: info.Size,
			Meta: disk{Name: newHead, Parent: newSnap},
		})},
		{2, wal.ActionLinkAsSnapshot, mustJSONT(t, LinkAsSnapshotArgs{
			SourceImage: oldHead, DestSnap: newSnap,
		})},
		{3, wal.ActionUpdateVolumeMeta, mustJSONT(t, UpdateVolumeMetaArgs{
			Info: newInfo,
		})},
		{4, wal.ActionUpdateSnapMeta, mustJSONT(t, UpdateSnapMetaArgs{
			SnapName: newSnap,
			Meta: disk{
				Name: newSnap, UserCreated: true, Created: "now",
			},
		})},
		{5, wal.ActionDeleteOldHead, mustJSONT(t, DeleteOldHeadArgs{
			HeadName: oldHead,
		})},
	}
	return newInfo, plan
}

// setupSnapCreatePreOp creates a fresh dir with one head image+meta and a
// volume.meta pointing at it. Returns initial Info and oldHead name.
func setupSnapCreatePreOp(t *testing.T) (string, Info, string) {
	t.Helper()
	dir := t.TempDir()
	const oldHead = "volume-head-000.img"
	info := Info{Size: 4096, Head: oldHead, SectorSize: 512}
	s := stepDir{dir: dir}
	if err := s.writeJSONAtomic(volumeMetaData, &info); err != nil {
		t.Fatal(err)
	}
	if err := applyCreateHead(s, mustJSONT(t, CreateHeadArgs{
		HeadName: oldHead, Size: 4096, Meta: disk{Name: oldHead},
	})); err != nil {
		t.Fatal(err)
	}
	return dir, info, oldHead
}

// captureSnapCreatePostOp returns the post-op fingerprint by running the
// full plan to completion + commit + journal cleanup, on a separate dir.
func captureSnapCreatePostOp(t *testing.T) fingerprint {
	t.Helper()
	dir, info, oldHead := setupSnapCreatePreOp(t)
	_, plan := snapshotCreatePlan(t, info, oldHead, "s1")
	driveCrash(t, dir, wal.OpSnapCreate, plan, len(plan), len(plan), true, true)
	// Use Open + Checkpoint (no recovery work) to ensure post state is
	// stable, then fingerprint.
	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	return fingerprintDir(t, dir)
}

// TestCrashMatrixSnapCreate enumerates every meaningful crash point:
//
//	pre-prepare:  intents=0..N, prepared=false, applied=0     -> pre-op
//	prepared:     intents=N,    prepared=true,  applied=0..N  -> post-op
//
// "intents=k, prepared=false" with k>0 models a crash mid-Intent loop,
// which leaves a torn intent set the recovery driver must discard.
func TestCrashMatrixSnapCreate(t *testing.T) {
	postOpFP := captureSnapCreatePostOp(t)

	preDir, info, oldHead := setupSnapCreatePreOp(t)
	preOpFP := fingerprintDir(t, preDir)
	_, refPlan := snapshotCreatePlan(t, info, oldHead, "s1")
	N := len(refPlan)

	// Pre-prepare crashes: torn or empty intent set -> pre-op.
	for intents := 0; intents <= N; intents++ {
		intents := intents
		t.Run(fmt.Sprintf("pre-prepare/intents=%d", intents), func(t *testing.T) {
			dir, info, oldHead := setupSnapCreatePreOp(t)
			_, plan := snapshotCreatePlan(t, info, oldHead, "s1")
			driveCrash(t, dir, wal.OpSnapCreate, plan, intents, 0, false, false)
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, preOpFP) {
				t.Fatalf("expected pre-op:\n%s", fpDiff(got, preOpFP))
			}
		})
	}

	// Prepared crashes: redoable -> post-op.
	for applied := 0; applied <= N; applied++ {
		applied := applied
		t.Run(fmt.Sprintf("prepared/applied=%d", applied), func(t *testing.T) {
			dir, info, oldHead := setupSnapCreatePreOp(t)
			_, plan := snapshotCreatePlan(t, info, oldHead, "s1")
			driveCrash(t, dir, wal.OpSnapCreate, plan, N, applied, true, false)
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, postOpFP) {
				t.Fatalf("expected post-op:\n%s", fpDiff(got, postOpFP))
			}
		})
	}
}

// ---- Revert matrix ----

// setupRevertPreOp creates: snap-base existing on disk, head-001
// pointing at snap-base. Returns dir, info, oldHead, target.
func setupRevertPreOp(t *testing.T) (dir string, info Info, oldHead, target string) {
	t.Helper()
	dir = t.TempDir()
	target = "volume-snap-base.img"
	oldHead = "volume-head-001.img"
	s := stepDir{dir: dir}
	// snap-base
	if err := applyCreateHead(s, mustJSONT(t, CreateHeadArgs{
		HeadName: target, Size: 4096, Meta: disk{Name: target, UserCreated: true},
	})); err != nil {
		t.Fatal(err)
	}
	// head-001 -> parent=snap-base
	if err := applyCreateHead(s, mustJSONT(t, CreateHeadArgs{
		HeadName: oldHead, Size: 4096, Meta: disk{Name: oldHead, Parent: target},
	})); err != nil {
		t.Fatal(err)
	}
	info = Info{Size: 4096, Head: oldHead, Parent: target, SectorSize: 512}
	if err := s.writeJSONAtomic(volumeMetaData, &info); err != nil {
		t.Fatal(err)
	}
	return
}

func revertPlan(t *testing.T, info Info, oldHead, target string) (Info, []stepPlan) {
	t.Helper()
	newHead := "volume-head-002.img"
	newInfo := info
	newInfo.Head = newHead
	newInfo.Parent = target
	newInfo.Dirty = true
	plan := []stepPlan{
		{1, wal.ActionCreateHead, mustJSONT(t, CreateHeadArgs{
			HeadName: newHead, Size: info.Size,
			Meta: disk{Name: newHead, Parent: target, Created: "now"},
		})},
		{2, wal.ActionUpdateVolumeMeta, mustJSONT(t, UpdateVolumeMetaArgs{
			Info: newInfo,
		})},
		{3, wal.ActionDeleteOldHead, mustJSONT(t, DeleteOldHeadArgs{
			HeadName: oldHead,
		})},
	}
	return newInfo, plan
}

func captureRevertPostOp(t *testing.T) fingerprint {
	t.Helper()
	dir, info, oldHead, target := setupRevertPreOp(t)
	_, plan := revertPlan(t, info, oldHead, target)
	driveCrash(t, dir, wal.OpSnapRevert, plan, len(plan), len(plan), true, true)
	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	return fingerprintDir(t, dir)
}

func TestCrashMatrixRevert(t *testing.T) {
	postOpFP := captureRevertPostOp(t)

	preDir, info, oldHead, target := setupRevertPreOp(t)
	preOpFP := fingerprintDir(t, preDir)
	_, refPlan := revertPlan(t, info, oldHead, target)
	N := len(refPlan)

	for intents := 0; intents <= N; intents++ {
		intents := intents
		t.Run(fmt.Sprintf("pre-prepare/intents=%d", intents), func(t *testing.T) {
			dir, info, oldHead, target := setupRevertPreOp(t)
			_, plan := revertPlan(t, info, oldHead, target)
			driveCrash(t, dir, wal.OpSnapRevert, plan, intents, 0, false, false)
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, preOpFP) {
				t.Fatalf("expected pre-op:\n%s", fpDiff(got, preOpFP))
			}
		})
	}
	for applied := 0; applied <= N; applied++ {
		applied := applied
		t.Run(fmt.Sprintf("prepared/applied=%d", applied), func(t *testing.T) {
			dir, info, oldHead, target := setupRevertPreOp(t)
			_, plan := revertPlan(t, info, oldHead, target)
			driveCrash(t, dir, wal.OpSnapRevert, plan, N, applied, true, false)
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, postOpFP) {
				t.Fatalf("expected post-op:\n%s", fpDiff(got, postOpFP))
			}
		})
	}
}

// ---- RemoveDiffDisk matrix ----

// setupRemovePreOp creates: snap-a (parent ""), snap-b (parent snap-a),
// volume.meta pointing at snap-b as Head (we treat snap-b as head for
// this lower-level test so we can exercise the multi-step plan). The
// op being journaled is "remove snap-a"; after recovery snap-b's
// parent should become "".
func setupRemovePreOp(t *testing.T) (dir, removeName, child string, childMetaPre disk) {
	t.Helper()
	dir = t.TempDir()
	removeName = "volume-snap-a.img"
	child = "volume-snap-b.img"
	s := stepDir{dir: dir}
	if err := applyCreateHead(s, mustJSONT(t, CreateHeadArgs{
		HeadName: removeName, Size: 4096,
		Meta: disk{Name: removeName, UserCreated: true},
	})); err != nil {
		t.Fatal(err)
	}
	childMetaPre = disk{Name: child, Parent: removeName, UserCreated: true}
	if err := applyCreateHead(s, mustJSONT(t, CreateHeadArgs{
		HeadName: child, Size: 4096, Meta: childMetaPre,
	})); err != nil {
		t.Fatal(err)
	}
	if err := s.writeJSONAtomic(volumeMetaData, &Info{
		Size: 4096, Head: child, Parent: removeName, SectorSize: 512,
	}); err != nil {
		t.Fatal(err)
	}
	return
}

func removePlan(t *testing.T, child string, childMetaPre disk, removeName string) []stepPlan {
	t.Helper()
	newChildMeta := childMetaPre
	newChildMeta.Parent = "" // grandparent is "" since removeName was top
	return []stepPlan{
		{1, wal.ActionUpdateSnapMeta, mustJSONT(t, UpdateSnapMetaArgs{
			SnapName: child, Meta: newChildMeta,
		})},
		{2, wal.ActionRmDisk, mustJSONT(t, RmDiskArgs{Name: removeName})},
	}
}

func captureRemovePostOp(t *testing.T) fingerprint {
	t.Helper()
	dir, removeName, child, childMetaPre := setupRemovePreOp(t)
	plan := removePlan(t, child, childMetaPre, removeName)
	driveCrash(t, dir, wal.OpSnapRemove, plan, len(plan), len(plan), true, true)
	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	return fingerprintDir(t, dir)
}

func TestCrashMatrixRemoveDiffDisk(t *testing.T) {
	postOpFP := captureRemovePostOp(t)

	preDir, _, _, _ := setupRemovePreOp(t)
	preOpFP := fingerprintDir(t, preDir)

	_, _, _, refMeta := setupRemovePreOp(t)
	refPlan := removePlan(t, "volume-snap-b.img", refMeta, "volume-snap-a.img")
	N := len(refPlan)

	for intents := 0; intents <= N; intents++ {
		intents := intents
		t.Run(fmt.Sprintf("pre-prepare/intents=%d", intents), func(t *testing.T) {
			dir, removeName, child, childMetaPre := setupRemovePreOp(t)
			plan := removePlan(t, child, childMetaPre, removeName)
			driveCrash(t, dir, wal.OpSnapRemove, plan, intents, 0, false, false)
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, preOpFP) {
				t.Fatalf("expected pre-op:\n%s", fpDiff(got, preOpFP))
			}
		})
	}
	for applied := 0; applied <= N; applied++ {
		applied := applied
		t.Run(fmt.Sprintf("prepared/applied=%d", applied), func(t *testing.T) {
			dir, removeName, child, childMetaPre := setupRemovePreOp(t)
			plan := removePlan(t, child, childMetaPre, removeName)
			driveCrash(t, dir, wal.OpSnapRemove, plan, N, applied, true, false)
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, postOpFP) {
				t.Fatalf("expected post-op:\n%s", fpDiff(got, postOpFP))
			}
		})
	}
}

// TestCrashMatrixSnapCreateThroughReplicaAfterRecovery is an end-to-end
// sanity check: after each crash point + recovery, opening a real
// *Replica must succeed and chains must be consistent.
func TestCrashMatrixSnapCreateThroughReplicaAfterRecovery(t *testing.T) {
	cases := []struct {
		name           string
		intents        int
		prepared       bool
		applied        int
		expectPostHead bool
	}{
		{"begin-only", 0, false, 0, false},
		{"intents-1-no-prepare", 1, false, 0, false},
		{"intents-3-no-prepare", 3, false, 0, false},
		{"intents-N-no-prepare", 5, false, 0, false},
		{"prepared-applied-0", 5, true, 0, true},
		{"prepared-applied-2", 5, true, 2, true},
		{"prepared-applied-N", 5, true, 5, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir, info, oldHead := setupSnapCreatePreOp(t)
			_, plan := snapshotCreatePlan(t, info, oldHead, "s1")
			driveCrash(t, dir, wal.OpSnapCreate, plan, tc.intents, tc.applied, tc.prepared, false)
			r, err := New(context.Background(), 4096, 512, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer r.Close()
			if !tc.expectPostHead {
				if r.info.Head != oldHead {
					t.Fatalf("expected pre-op head %q, got %q", oldHead, r.info.Head)
				}
				return
			}
			if r.info.Head != "volume-head-001.img" {
				t.Fatalf("expected post-op head volume-head-001.img, got %q", r.info.Head)
			}
			if !strings.HasPrefix(r.info.Parent, "volume-snap-s1") {
				t.Fatalf("expected parent volume-snap-s1*, got %q", r.info.Parent)
			}
		})
	}
}

// ---- Nested recovery (recovery-of-recovery) ----
//
// These tests verify that crash-during-recovery converges. Setup: a
// prepared txn with 0 steps applied (i.e. the original writer crashed
// right after PREPARE). The test then simulates a recovery process
// that runs partway and crashes itself, by directly invoking the
// stepRegistry against the replica directory and optionally writing
// STEP_DONE to the journal -- the same operations recoverJournal does
// internally. After the simulated recovery-crash, the real
// recoverJournal is invoked to finish; the final fingerprint must
// match the post-op fingerprint of a crash-free run.
//
// The (applyK, stepDoneJ) parameters model the two failure points
// inside recoverJournal's inner loop:
//
//	apply step k to disk (idempotent)   <-- crash here: stepDoneJ < applyK
//	write STEP_DONE record + fsync      <-- crash here: stepDoneJ == applyK
//
// stepDoneJ must be <= applyK by construction (the journal record
// follows the disk action).

// partialRecover simulates recoverJournal interrupted mid-flight.
// It opens the journal, picks up the single pending prepared txn,
// applies the first `applyK` not-yet-completed steps to disk, writes
// STEP_DONE for the first `stepDoneJ` of them, then force-closes
// without commit/checkpoint.
func partialRecover(t *testing.T, dir string, applyK, stepDoneJ int) {
	t.Helper()
	if stepDoneJ > applyK {
		t.Fatalf("stepDoneJ=%d > applyK=%d (impossible: STEP_DONE follows disk apply)", stepDoneJ, applyK)
	}
	j, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := j.Recover()
	if err != nil {
		_ = wal.ForceCloseForTest(j)
		t.Fatal(err)
	}
	if len(a.Pending) != 1 {
		_ = wal.ForceCloseForTest(j)
		t.Fatalf("expected exactly 1 pending txn, got %d", len(a.Pending))
	}
	pt := a.Pending[0]
	if !pt.Prepared {
		_ = wal.ForceCloseForTest(j)
		t.Fatalf("expected prepared txn for nested-recovery test")
	}

	tx, err := wal.AdoptTxn(j, pt.ID, pt.Op)
	if err != nil {
		_ = wal.ForceCloseForTest(j)
		t.Fatalf("adopt txn %d: %v", pt.ID, err)
	}
	s := stepDir{dir: dir}
	applied := 0
	for _, intent := range pt.PendingIntents {
		if pt.CompletedSteps[intent.StepID] {
			continue
		}
		if applied >= applyK {
			break
		}
		fn := stepRegistry[intent.Action]
		if fn == nil {
			_ = wal.ForceCloseForTest(j)
			t.Fatalf("unknown action %q", intent.Action)
		}
		if err := fn(s, intent.Args); err != nil {
			_ = wal.ForceCloseForTest(j)
			t.Fatalf("apply step %d (%s): %v", intent.StepID, intent.Action, err)
		}
		if applied < stepDoneJ {
			if err := tx.StepDone(intent.StepID); err != nil {
				_ = wal.ForceCloseForTest(j)
				t.Fatalf("StepDone %d: %v", intent.StepID, err)
			}
		}
		applied++
	}
	if err := wal.ForceCloseForTest(j); err != nil {
		t.Fatal(err)
	}
}

// TestNestedRecoverySnapCreate verifies every realistic crash point of
// the recovery path itself. recoverJournal's inner loop is
//
//	for each not-yet-completed step:
//	    apply step k to disk          <-- crash A: applyK=k, stepDoneJ=k-1
//	    write STEP_DONE k + fsync     <-- crash B: applyK=k, stepDoneJ=k
//	    (proceed to step k+1)
//
// so at any in-flight moment applyK - stepDoneJ is 0 or 1. Combinations
// outside that window are not reachable from production code (and
// would falsely signal a bug because steps depend on each other in
// order: e.g. replaying step 2 LINK after step 5 DELETE_OLD_HEAD has
// run cannot succeed -- the source inode is gone).
func TestNestedRecoverySnapCreate(t *testing.T) {
	postOp := captureSnapCreatePostOp(t)
	const N = 5
	type pt struct{ applyK, stepDoneJ int }
	var cases []pt
	cases = append(cases, pt{0, 0}) // recovery hasn't started
	for k := 1; k <= N; k++ {
		cases = append(cases, pt{k, k - 1}) // crash A: pre-STEP_DONE
		cases = append(cases, pt{k, k})     // crash B: post-STEP_DONE
	}
	for _, c := range cases {
		c := c
		t.Run(fmt.Sprintf("applyK=%d/stepDoneJ=%d", c.applyK, c.stepDoneJ), func(t *testing.T) {
			dir, info, oldHead := setupSnapCreatePreOp(t)
			_, plan := snapshotCreatePlan(t, info, oldHead, "s1")

			// First crash: writer prepared but did 0 steps.
			driveCrash(t, dir, wal.OpSnapCreate, plan, N, 0, true, false)

			// Second crash: recovery makes partial progress.
			partialRecover(t, dir, c.applyK, c.stepDoneJ)

			// Final recovery must converge to post-op.
			got := recoverAndFingerprint(t, dir)
			if !fpEqual(got, postOp) {
				t.Fatalf("nested recovery did not converge to post-op:\n%s",
					fpDiff(got, postOp))
			}
		})
	}
}

// TestTripleCrashRecoveryConverges chains three partial recoveries
// (writer crash + two recovery crashes) and asserts the fourth attempt
// reaches post-op. Exercises the "monotonic progress" property: each
// successful STEP_DONE is durable, so a chain of crashes can only
// move the CompletedSteps set forward.
func TestTripleCrashRecoveryConverges(t *testing.T) {
	postOp := captureSnapCreatePostOp(t)
	dir, info, oldHead := setupSnapCreatePreOp(t)
	_, plan := snapshotCreatePlan(t, info, oldHead, "s1")

	// Crash 1: writer crashed after PREPARE.
	driveCrash(t, dir, wal.OpSnapCreate, plan, len(plan), 0, true, false)

	// Crash 2: recovery applied step 1 to disk, did NOT write STEP_DONE.
	// (applyK=1, stepDoneJ=0 -- the realistic "after disk apply, before
	// journal write" crash point.)
	partialRecover(t, dir, 1, 0)

	// Crash 3: recovery redid step 1 (idempotent), wrote STEP_DONE 1,
	// applied step 2, wrote STEP_DONE 2, applied step 3, crashed before
	// STEP_DONE 3. (applyK=3, stepDoneJ=2.)
	partialRecover(t, dir, 3, 2)

	// Final recovery: must redo step 3 (idempotent), then 4 and 5.
	got := recoverAndFingerprint(t, dir)
	if !fpEqual(got, postOp) {
		t.Fatalf("triple-crash recovery did not converge to post-op:\n%s",
			fpDiff(got, postOp))
	}
}
