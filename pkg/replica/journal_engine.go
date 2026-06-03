package replica

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-common-libs/wal"

	"github.com/longhorn/longhorn-engine/pkg/util"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

// This file implements the journal-driven step engine for snapshot chain
// operations. Each Action corresponds to a single idempotent filesystem
// transformation; the args struct attached to its INTENT record captures
// the full target state so that re-applying the step (during recovery
// after a crash, or during a runtime retry) is content-deterministic.
//
// Apply functions take only a directory path and serialized args -- they
// do not touch the live *Replica struct. This lets the recovery path run
// before the Replica is fully constructed.

// stepDir is the minimal context shared by every Apply.
type stepDir struct {
	dir string
}

func (s stepDir) path(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(s.dir, name)
}

// writeJSONAtomic writes obj as JSON to <dir>/<file> using the
// tmp-file + fsync + rename + fsync(dir) pattern. Mirrors
// (*Replica).encodeToFile but is callable without a Replica.
func (s stepDir) writeJSONAtomic(file string, obj interface{}) error {
	tmpPath := s.path(file + tmpFileSuffix)
	finalPath := s.path(file)

	f, err := os.Create(tmpPath)
	if err != nil {
		return errors.Wrapf(err, "failed to create %v", tmpPath)
	}
	if err := json.NewEncoder(f).Encode(obj); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return errors.Wrapf(err, "failed to fsync %v", tmpPath)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return util.FsyncDir(s.dir)
}

// mustJSON marshals v to JSON or panics. Step-arg structs are pure data
// (fixed field set, no funcs/channels), so encoding cannot fail at
// runtime; treating an unexpected encoder error as fatal keeps callers
// readable.
func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ---- Step args ----

// CreateHeadArgs carries the full target state for ActionCreateHead.
type CreateHeadArgs struct {
	HeadName string `json:"head_name"` // e.g. "volume-head-001.img"
	Size     int64  `json:"size"`
	Meta     disk   `json:"meta"`
}

// LinkAsSnapshotArgs is the args payload for ActionLinkAsSnapshot.
type LinkAsSnapshotArgs struct {
	SourceImage string `json:"source_image"` // e.g. "volume-head-000.img"
	DestSnap    string `json:"dest_snap"`    // e.g. "volume-snap-NAME.img"
}

// UpdateVolumeMetaArgs is the args payload for ActionUpdateVolumeMeta.
type UpdateVolumeMetaArgs struct {
	Info Info `json:"info"`
}

// UpdateSnapMetaArgs is the args payload for ActionUpdateSnapMeta.
type UpdateSnapMetaArgs struct {
	SnapName string `json:"snap_name"`
	Meta     disk   `json:"meta"`
}

// DeleteOldHeadArgs is the args payload for ActionDeleteOldHead.
type DeleteOldHeadArgs struct {
	HeadName string `json:"head_name"`
}

// RmDiskArgs is the args payload for ActionRmDisk. Removes the image,
// metadata, and (if present) checksum file of a snapshot or head.
type RmDiskArgs struct {
	Name string `json:"name"`
}

// ---- Txn params (TXN_BEGIN payloads) ----
//
// One typed struct per Op so journal-dump shows a stable field order and
// callers can't typo a key. These structs are appended to TXN_BEGIN as
// the "params" preview and are otherwise informational.

// SnapCreateParams is the TXN_BEGIN params for OpSnapCreate.
type SnapCreateParams struct {
	Snapshot    string `json:"snapshot"`
	OldHead     string `json:"old_head"`
	NewHead     string `json:"new_head"`
	NewSnap     string `json:"new_snap"`
	UserCreated bool   `json:"user_created"`
	Size        int64  `json:"size"`
}

// SnapRevertParams is the TXN_BEGIN params for OpSnapRevert.
type SnapRevertParams struct {
	Parent  string `json:"parent"`
	OldHead string `json:"old_head"`
	NewHead string `json:"new_head"`
	Created string `json:"created"`
}

// SnapRemoveParams is the TXN_BEGIN params for OpSnapRemove.
type SnapRemoveParams struct {
	Name  string `json:"name"`
	Force bool   `json:"force"`
	Child string `json:"child"`
}

// ---- Apply functions (idempotent) ----

func applyCreateHead(s stepDir, raw []byte) error {
	var a CreateHeadArgs
	if err := json.Unmarshal(raw, &a); err != nil {
		return errors.Wrap(err, "decode CREATE_HEAD args")
	}
	imgPath := s.path(a.HeadName)

	// Truncate is idempotent: same size has no effect; stale leftover
	// from a previous attempt is normalised to the requested size.
	if err := syscall.Truncate(imgPath, a.Size); err != nil {
		// Truncate fails if the file does not exist; in that case create.
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "truncate %v", imgPath)
		}
		f, errCreate := os.OpenFile(imgPath, os.O_RDWR|os.O_CREATE, 0666)
		if errCreate != nil {
			return errors.Wrapf(errCreate, "create %v", imgPath)
		}
		_ = f.Close()
		if err := syscall.Truncate(imgPath, a.Size); err != nil {
			return errors.Wrapf(err, "truncate %v", imgPath)
		}
	}
	return s.writeJSONAtomic(a.HeadName+diskutil.DiskMetadataSuffix, &a.Meta)
}

func applyLinkAsSnapshot(s stepDir, raw []byte) error {
	var a LinkAsSnapshotArgs
	if err := json.Unmarshal(raw, &a); err != nil {
		return errors.Wrap(err, "decode LINK_AS_SNAPSHOT args")
	}
	srcImg := s.path(a.SourceImage)
	srcMeta := s.path(a.SourceImage + diskutil.DiskMetadataSuffix)
	dstImg := s.path(a.DestSnap)
	dstMeta := s.path(a.DestSnap + diskutil.DiskMetadataSuffix)
	dstChecksum := s.path(a.DestSnap + diskutil.DiskChecksumSuffix)

	// Relink only the side that is missing or wrong; an already-correct
	// hardlink is left alone.
	imgSame, _ := sameInode(srcImg, dstImg)
	metaSame, _ := sameInode(srcMeta, dstMeta)
	if imgSame && metaSame {
		return nil
	}

	// Cleanup dstChecksum proactively: checksums are recomputed lazily
	// after snapshot create, so a stale checksum from a prior partial
	// attempt must not outlive the relinked image.
	if err := os.RemoveAll(dstChecksum); err != nil {
		return errors.Wrapf(err, "cleanup %v before link", dstChecksum)
	}
	if !imgSame {
		if err := os.RemoveAll(dstImg); err != nil {
			return errors.Wrapf(err, "cleanup %v before link", dstImg)
		}
		if err := os.Link(srcImg, dstImg); err != nil {
			return errors.Wrapf(err, "link %v -> %v", srcImg, dstImg)
		}
	}
	if !metaSame {
		if err := os.RemoveAll(dstMeta); err != nil {
			return errors.Wrapf(err, "cleanup %v before link", dstMeta)
		}
		if err := os.Link(srcMeta, dstMeta); err != nil {
			return errors.Wrapf(err, "link %v -> %v", srcMeta, dstMeta)
		}
	}
	return util.FsyncDir(s.dir)
}

func applyUpdateVolumeMeta(s stepDir, raw []byte) error {
	var a UpdateVolumeMetaArgs
	if err := json.Unmarshal(raw, &a); err != nil {
		return errors.Wrap(err, "decode UPDATE_VOLUME_META args")
	}
	return s.writeJSONAtomic(volumeMetaData, &a.Info)
}

func applyUpdateSnapMeta(s stepDir, raw []byte) error {
	var a UpdateSnapMetaArgs
	if err := json.Unmarshal(raw, &a); err != nil {
		return errors.Wrap(err, "decode UPDATE_SNAP_META args")
	}
	return s.writeJSONAtomic(a.SnapName+diskutil.DiskMetadataSuffix, &a.Meta)
}

func applyDeleteOldHead(s stepDir, raw []byte) error {
	var a DeleteOldHeadArgs
	if err := json.Unmarshal(raw, &a); err != nil {
		return errors.Wrap(err, "decode DELETE_OLD_HEAD args")
	}
	return removeDiskFiles(s, a.HeadName)
}

func applyRmDisk(s stepDir, raw []byte) error {
	var a RmDiskArgs
	if err := json.Unmarshal(raw, &a); err != nil {
		return errors.Wrap(err, "decode RM_DISK args")
	}
	return removeDiskFiles(s, a.Name)
}

// removeDiskFiles deletes <name>, <name>.meta and <name>.checksum from
// dir if they exist. Missing files are ignored so the step is
// idempotent under recovery replay.
func removeDiskFiles(s stepDir, name string) error {
	if name == "" {
		return nil
	}
	for _, suffix := range []string{"", diskutil.DiskMetadataSuffix, diskutil.DiskChecksumSuffix} {
		p := s.path(name + suffix)
		if err := os.RemoveAll(p); err != nil {
			return errors.Wrapf(err, "remove %v", p)
		}
	}
	return util.FsyncDir(s.dir)
}

// ---- Registry ----

type stepFunc func(s stepDir, raw []byte) error

var stepRegistry = map[wal.Action]stepFunc{
	wal.ActionCreateHead:       applyCreateHead,
	wal.ActionLinkAsSnapshot:   applyLinkAsSnapshot,
	wal.ActionUpdateVolumeMeta: applyUpdateVolumeMeta,
	wal.ActionUpdateSnapMeta:   applyUpdateSnapMeta,
	wal.ActionDeleteOldHead:    applyDeleteOldHead,
	wal.ActionRmDisk:           applyRmDisk,
}

// ---- Recovery driver ----

// recoverJournal scans the journal in dir, replays any unfinished
// transactions, and checkpoints. It does not require an open Replica
// and is therefore safe to call from construct() before readMetadata().
//
// The returned journal is open with a held flock and ready for the
// caller to use for new transactions; close it via wal.Close().
func recoverJournal(dir string) (*wal.Journal, error) {
	j, err := wal.Open(dir)
	if err != nil {
		return nil, errors.Wrap(err, "open journal")
	}
	analysis, err := j.Recover()
	if err != nil {
		_ = j.Close()
		return nil, errors.Wrap(err, "recover journal")
	}

	s := stepDir{dir: dir}
	for _, pt := range analysis.Pending {
		if !pt.Prepared {
			// Either no intents at all, or a torn intent set: the
			// plan is not durable as a whole, so we cannot safely
			// redo it. Abort and discard.
			tx, err := wal.AdoptTxn(j, pt.ID, pt.Op)
			if err != nil {
				_ = j.Close()
				return nil, errors.Wrapf(err, "adopt un-prepared txn %d", pt.ID)
			}
			if err := tx.Abort(); err != nil {
				_ = j.Close()
				return nil, errors.Wrapf(err, "abort un-prepared txn %d", pt.ID)
			}
			logrus.Infof("Recovery: aborted un-prepared txn %d (%s) — discarded %d torn intent(s)",
				pt.ID, pt.Op, len(pt.PendingIntents))
			continue
		}
		logrus.Infof("Recovery: replaying txn %d (%s) from step %d",
			pt.ID, pt.Op, firstIncompleteStep(pt))
		tx, err := wal.AdoptTxn(j, pt.ID, pt.Op)
		if err != nil {
			_ = j.Close()
			return nil, errors.Wrapf(err, "adopt prepared txn %d", pt.ID)
		}
		for _, intent := range pt.PendingIntents {
			if pt.CompletedSteps[intent.StepID] {
				continue
			}
			fn, ok := stepRegistry[intent.Action]
			if !ok {
				// Do not Abort: the txn is prepared and may be
				// recoverable by a future binary that knows this
				// action. Leave it pending and fail to start.
				_ = j.Close()
				return nil, fmt.Errorf("recovery: unknown action %q in txn %d step %d",
					intent.Action, pt.ID, intent.StepID)
			}
			if err := fn(s, intent.Args); err != nil {
				// Apply functions are idempotent. Leave the txn
				// pending so a subsequent restart retries; aborting
				// here would discard the only thing that makes the
				// (already-prepared) operation recoverable.
				_ = j.Close()
				return nil, errors.Wrapf(err, "recovery: apply step %d (%s) of txn %d",
					intent.StepID, intent.Action, pt.ID)
			}
			if err := tx.StepDone(intent.StepID); err != nil {
				_ = j.Close()
				return nil, errors.Wrapf(err, "recovery: STEP_DONE for txn %d step %d",
					pt.ID, intent.StepID)
			}
		}
		if err := tx.Commit(); err != nil {
			_ = j.Close()
			return nil, errors.Wrapf(err, "recovery: commit txn %d", pt.ID)
		}
	}

	if err := j.Checkpoint(); err != nil {
		_ = j.Close()
		return nil, errors.Wrap(err, "checkpoint after recovery")
	}
	return j, nil
}

func firstIncompleteStep(pt wal.PendingTxn) uint32 {
	for _, intent := range pt.PendingIntents {
		if !pt.CompletedSteps[intent.StepID] {
			return intent.StepID
		}
	}
	return 0
}

// sameInode returns true if a and b refer to the same inode (i.e. one is
// a hardlink of the other). Errors (e.g. either path missing) yield
// (false, err).
func sameInode(a, b string) (bool, error) {
	sa, err := os.Stat(a)
	if err != nil {
		return false, err
	}
	sb, err := os.Stat(b)
	if err != nil {
		return false, err
	}
	stA, okA := sa.Sys().(*syscall.Stat_t)
	stB, okB := sb.Sys().(*syscall.Stat_t)
	if !okA || !okB {
		return false, nil
	}
	return stA.Dev == stB.Dev && stA.Ino == stB.Ino, nil
}
