// Package wal implements a crash-safe append-only write-ahead log for
// operations on a target directory.
//
// On-disk layout:
//
//	+--------+--------+--------+------------+-----------+------------+----------+
//	| Magic  | Ver    | Type   | PayloadLen | HeaderCRC | PayloadCRC | Payload  |
//	| 4 B    | 2 B    | 2 B    | 4 B        | 4 B       | 4 B        | N B      |
//	+--------+--------+--------+------------+-----------+------------+----------+
//
// All integers are little-endian. Each record carries two independent
// CRC32C (Castagnoli) sums: HeaderCRC covers the framing bytes
// (Magic..PayloadLen) and PayloadCRC covers the payload. A torn tail at
// the end of the file (partial header / partial payload) is detected and
// silently truncated on Open: only fully-synced records are observed by
// recovery. Mid-stream corruption (bad magic / version / CRC / oversize
// payload) is treated as a fatal error so OpenWithQuarantine can rename
// the file aside for offline inspection instead of silently dropping
// durable records.
//
// Torn-tail vs. corruption boundary: a crash mid-append leaves a short
// (partial) final record, detected as a torn tail (a short read of the
// header or payload) and truncated. The separate HeaderCRC lets recovery
// validate PayloadLen BEFORE reading the payload, so a bit flip in an
// earlier record's length is caught as mid-stream corruption (a
// full-length header that fails its CRC) instead of being mistaken for a
// torn tail — which would otherwise consume the following valid records
// and truncate them away. Only a genuinely short trailing read is treated
// as torn; any full-length record that fails a CRC is quarantined, so the
// WAL never silently drops a record it cannot prove is torn.
//
// Concurrency model: a Journal owns the data file and an advisory flock
// on a dedicated lock file (LockFileName) for the lifetime of an open
// Journal. A separate lock file is required because Unix rename is not
// blocked by an advisory flock on the renamed file; using one
// guarantees OpenWithQuarantine's data-file rename is exclusive against
// concurrent quarantine attempts in other processes. All write methods
// serialize through an internal mutex; the caller's own top-level lock
// typically already serializes operations, so this mutex is just
// defensive.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gofrs/flock"
)

// FileName is the journal data file name inside the target directory.
const FileName = "journal.log"

// LockFileName is the dedicated lock file used for cross-process
// exclusion. It is held with flock for the lifetime of an open
// Journal. A dedicated file (separate from the data file) is required
// so OpenWithQuarantine can safely rename the data file away without
// dropping the lock — Unix rename is not blocked by an advisory
// flock, so locking the data file directly would allow two concurrent
// callers to produce a split-brain journal.
const LockFileName = "journal.lock"

const (
	magic   uint32 = 0x4C484A4C // "LHJL" (Longhorn Journal)
	version uint16 = 2

	headerSize = 4 + 2 + 2 + 4 + 4 + 4 // 20 bytes

	// MaxPayloadSize bounds a single record. Real records are tiny (<1 KB);
	// the cap is defensive against corruption that yields huge lengths.
	MaxPayloadSize = 1 << 20 // 1 MiB
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrJournalLocked is returned by Open if another process holds the flock.
var ErrJournalLocked = errors.New("journal is locked by another process")

// ErrJournalPoisoned marks a journal that refused an append because a
// prior append failed AND its rollback also failed, potentially leaving
// a CRC-valid record on disk that the next Open would accept as complete
// (torn-tail handling cannot remove a fully-framed record). Once
// poisoned, every further append is refused so a caller retry cannot
// create a duplicate/ghost operation; the journal must be reopened or
// quarantined to clear the condition.
var ErrJournalPoisoned = errors.New("journal is poisoned")

// errFlockAcquire marks an Open failure caused by an underlying flock
// acquisition error (e.g. the filesystem does not support advisory
// locking, or the lock file cannot be created because the parent
// directory is missing). Such errors are not caused by a corrupt
// journal, so OpenWithQuarantine treats them like ErrJournalLocked
// and refuses to rename the data file aside.
var errFlockAcquire = errors.New("journal flock acquire failed")

// errTornTail marks a structural read failure that is consistent with a
// crash mid-write at the very end of the file (short read while reading
// a header or a payload). These are safely truncatable on Open.
// Anything else (bad magic, unsupported version, bad CRC, oversize
// payload) is treated as mid-stream corruption: Open refuses to touch
// the file so OpenWithQuarantine can rename it aside for inspection.
var errTornTail = errors.New("journal torn tail")

// errJournalCorrupt marks a failure that means the on-disk journal file
// itself is unreadable as a WAL: mid-stream structural corruption (bad
// magic / version / CRC / oversize payload), or the journal path being
// occupied by a non-regular file. Only these justify OpenWithQuarantine
// renaming the file aside. Transient infrastructure failures (EMFILE,
// EIO, ENOSPC, a missing parent directory, an fsync error) are left
// unmarked so a healthy journal is never quarantined for an environmental
// hiccup. Semantic broken-writer errors are not surfaced by Open at all;
// they come from Recover/Analyze and are handled by Quarantine.
var errJournalCorrupt = errors.New("journal is corrupt")

// Journal is an append-only WAL bound to a target directory.
type Journal struct {
	mu        sync.Mutex
	dir       string
	path      string
	f         *os.File
	lock      *flock.Flock
	nextTxnID TxnID
	inFlight  int
	closed    bool
	// recovered is true once this Journal's pre-existing on-disk pending
	// transactions have been accounted for: either the journal was empty
	// at Open (nothing to recover) or Recover has completed. Checkpoint and
	// Close-time truncation refuse to run while it is false, so a non-empty
	// journal that was opened but never analyzed cannot have its durable
	// records erased. len(recoveredPending)==0 alone is insufficient here
	// because a nil map (never recovered) also has length zero.
	recovered bool
	// recoveredPending maps each pending TxnID reported by the most recent
	// Recover() that has not yet been picked up by AdoptTxn to the on-disk
	// state AdoptTxn needs to reconstruct it. Their records are still
	// durable on disk and MUST NOT be truncated by Checkpoint or by Close's
	// clean-shutdown checkpoint; otherwise the next process start has
	// nothing to replay.
	recoveredPending map[TxnID]*recoveredTxn
	// poisoned is set when a failed append could not be rolled back, so a
	// possibly-complete record may remain on disk. While non-nil every
	// further append is refused (wrapping ErrJournalPoisoned) to stop a
	// caller retry from writing a duplicate; it is cleared only by reopening
	// (a new Journal) or quarantining.
	poisoned error
}

// recoveredTxn captures the on-disk state of a pending transaction that
// Recover observed, so an AdoptTxn'd handle can enforce the same
// prepared-and-complete commit invariant as a locally-created one.
type recoveredTxn struct {
	prepared bool
	required map[uint32]struct{} // intent step IDs that must be StepDone before commit
	done     map[uint32]struct{} // steps already durable before adoption
}

// Open opens (or creates) the journal in dir, acquires an exclusive
// non-blocking flock on a dedicated lock file (LockFileName),
// validates the existing journal data file by scanning to the first
// torn-tail record (which is truncated) and returns the Journal.
// Mid-stream corruption (bad magic / version / CRC / oversize
// payload) returns an error without modifying the data file so
// OpenWithQuarantine can rename it aside.
//
// Callers must call Recover once on the returned Journal before any
// Begin whenever the journal may be non-empty, so newly-issued TxnIDs do
// not collide with txns already durable on disk. Begin refuses until
// recovery state is established; a freshly created empty journal is
// recovered at Open and needs no explicit Recover.
func Open(dir string) (*Journal, error) {
	lk, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	j, err := openLogLocked(dir, lk)
	if err != nil {
		_ = lk.Unlock()
		return nil, err
	}
	return j, nil
}

// acquireDirLock takes the directory's exclusive non-blocking flock on
// the dedicated lock file. Returns ErrJournalLocked if another process
// already holds it, or an errFlockAcquire-marked error if the
// underlying flock syscall itself failed (e.g. filesystem doesn't
// support locking, parent directory missing).
func acquireDirLock(dir string) (*flock.Flock, error) {
	lockPath := filepath.Join(dir, LockFileName)
	lk := flock.New(lockPath)
	ok, err := lk.TryLock()
	if err != nil {
		return nil, errors.Mark(errors.Wrap(err, "failed to acquire journal flock"), errFlockAcquire)
	}
	if !ok {
		return nil, ErrJournalLocked
	}
	return lk, nil
}

// openLogLocked opens or creates the journal data file and validates
// it. The caller must already hold lk and is responsible for releasing
// it on error or via Journal.Close. On error the data file is closed
// but lk is left held so the caller can use it for quarantine.
func openLogLocked(dir string, lk *flock.Flock) (*Journal, error) {
	path := filepath.Join(dir, FileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		werr := errors.Wrap(err, "failed to open journal")
		// If the journal path exists but is not a regular file (e.g. a
		// directory occupies it), the on-disk object is unusable as a WAL:
		// mark it corrupt so OpenWithQuarantine renames it aside. Any other
		// open failure (EMFILE, EACCES, or ENOENT for a missing parent) is
		// infrastructure and left unmarked so a healthy file is untouched.
		if fi, statErr := os.Lstat(path); statErr == nil && !fi.Mode().IsRegular() {
			werr = errors.Mark(werr, errJournalCorrupt)
		}
		return nil, werr
	}
	// A journal path that opened successfully but is not a regular file (a
	// FIFO, device, socket, or a symlink pointing elsewhere) is unusable and
	// unsafe as a WAL: I/O may block, misbehave, or write outside dir. Reject
	// it as corrupt so OpenWithQuarantine renames it aside. Check the opened
	// fd (catches FIFO/device/socket) and the path entry (catches a symlink
	// whose target is regular) before syncing or validating anything.
	if fi, statErr := f.Stat(); statErr != nil {
		_ = f.Close()
		return nil, errors.Wrap(statErr, "stat journal")
	} else if !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.Mark(errors.Errorf("journal path is not a regular file (mode %s)", fi.Mode()), errJournalCorrupt)
	}
	if li, statErr := os.Lstat(path); statErr != nil {
		_ = f.Close()
		return nil, errors.Wrap(statErr, "lstat journal")
	} else if !li.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.Mark(errors.Errorf("journal path is a symlink or non-regular file (mode %s)", li.Mode()), errJournalCorrupt)
	}
	// fsync the parent directory so the journal.log dirent is durable
	// after a crash. Without this, a freshly-created journal can vanish
	// even though its records were fsynced individually.
	if err := syncDir(dir); err != nil {
		_ = f.Close()
		return nil, errors.Wrap(err, "sync journal parent directory")
	}
	j := &Journal{
		dir:       dir,
		path:      path,
		f:         f,
		lock:      lk,
		nextTxnID: 1,
	}
	count, err := j.validateAndTruncateTorn()
	if err != nil {
		_ = f.Close()
		// Do NOT release lk; caller (OpenWithQuarantine) may need it
		// to safely rename the data file aside.
		return nil, err
	}
	// An empty (or fully torn-away) journal has no pre-existing pending
	// transactions, so it is immediately safe to checkpoint/truncate. A
	// non-empty journal stays "not recovered" until Recover runs, so its
	// durable records cannot be erased by Checkpoint or a clean Close
	// before they have been analyzed.
	j.recovered = count == 0
	return j, nil
}

// syncDir fsyncs a directory so that pending dirent operations (file
// creates, renames) are made durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// QuarantineInfo describes a journal file that could not be opened
// cleanly and was renamed aside so the caller could continue with a
// fresh empty journal. The original bytes are preserved at
// QuarantinedPath for offline inspection (e.g. via journal-dump).
//
// Retention is the caller's responsibility: the quarantined file (and
// any journal.log.broken-* siblings from earlier incidents) is never
// removed by this package, so the caller must surface it to operators
// and reap it once inspected to avoid unbounded accumulation.
type QuarantineInfo struct {
	OriginalPath    string
	QuarantinedPath string
	OpenError       error
}

func (q *QuarantineInfo) Error() string {
	return fmt.Sprintf("journal quarantined: %s -> %s (cause: %v)",
		q.OriginalPath, q.QuarantinedPath, q.OpenError)
}

// OpenWithQuarantine behaves like Open, but if Open fails because the
// on-disk journal is genuinely unreadable — mid-stream structural
// corruption (bad magic / version / CRC / oversize payload), or the
// journal path being occupied by a non-regular file — it renames the
// existing journal.log to journal.log.broken-<unix-nanos> and retries
// Open once against a fresh empty file.
//
// It deliberately does NOT quarantine on ErrJournalLocked, a flock-layer
// failure, or any other infrastructure error (EMFILE, EIO, ENOSPC, a
// missing parent directory, an fsync failure): those leave a
// possibly-healthy journal untouched so a transient hiccup cannot destroy
// a good WAL. Semantic broken-writer errors are not detected here at all —
// Open only validates framing, so a structurally valid but logically
// impossible stream opens cleanly and is rejected later by
// Recover/Analyze; use Quarantine to replace such a journal.
//
// Concurrency: the dedicated lock file (LockFileName) is held across
// the whole quarantine operation including the rename and the
// re-open, so two concurrent callers cannot both rename and produce a
// split-brain journal. The loser of the race for the lock returns
// ErrJournalLocked.
//
// Return values:
//   - (j, nil,  nil):  Open succeeded on the first try; no quarantine.
//   - (j, info, nil):  quarantine occurred, the replacement journal is
//     j, the renamed-aside path and original error are in info.
//   - (nil, info, err): quarantine succeeded but the retry Open failed;
//     the broken file is at info.QuarantinedPath and the caller has
//     neither a working journal nor a clean slate.
//   - (nil, nil, err): quarantine refused (ErrJournalLocked, a flock-
//     layer failure, an infrastructure error, or the source file
//     disappeared). The on-disk state is unchanged.
//
// On a successful quarantine the caller should log loudly and surface
// the diagnostic path to operators. Quarantining discards in-flight
// transactions that were durable in the broken file; the caller is
// responsible for reconciling on-disk state.
func OpenWithQuarantine(dir string) (*Journal, *QuarantineInfo, error) {
	lk, err := acquireDirLock(dir)
	if err != nil {
		return nil, nil, err
	}
	j, openErr := openLogLocked(dir, lk)
	if openErr == nil {
		return j, nil, nil
	}
	// Only a genuinely unreadable/corrupt journal is renamed aside. An
	// infrastructure failure (EMFILE, EIO, ENOSPC, a missing parent
	// directory, an fsync error) must leave the possibly-healthy file
	// untouched so a transient hiccup cannot destroy a good WAL.
	if !errors.Is(openErr, errJournalCorrupt) {
		_ = lk.Unlock()
		return nil, nil, openErr
	}
	return quarantineFileLocked(dir, lk, openErr)
}

// quarantineFileLocked renames the journal data file aside
// (journal.log.broken-<unix-nanos>) and reopens a fresh empty journal
// under the SAME lock lk, held continuously across the rename and re-open
// so no other process can race in. cause is the error that triggered the
// quarantine and is recorded in QuarantineInfo.OpenError. On any failure
// lk is unlocked before returning.
func quarantineFileLocked(dir string, lk *flock.Flock, cause error) (*Journal, *QuarantineInfo, error) {
	src := filepath.Join(dir, FileName)
	dst := filepath.Join(dir, fmt.Sprintf("%s.broken-%d", FileName, time.Now().UnixNano()))

	// Source might be missing (e.g. parent dir gone). With nothing to
	// rename, surface the original cause.
	if _, statErr := os.Lstat(src); statErr != nil {
		_ = lk.Unlock()
		return nil, nil, cause
	}
	if renameErr := os.Rename(src, dst); renameErr != nil {
		_ = lk.Unlock()
		return nil, nil, errors.Wrapf(renameErr, "quarantine journal %s -> %s (cause: %v)",
			src, dst, cause)
	}
	// Make the rename itself durable so a crash here doesn't resurrect
	// the broken file under the original name.
	if syncErr := syncDir(dir); syncErr != nil {
		_ = lk.Unlock()
		return nil, nil, errors.Wrap(syncErr, "sync journal parent directory after quarantine")
	}

	info := &QuarantineInfo{OriginalPath: src, QuarantinedPath: dst, OpenError: cause}
	j, retryErr := openLogLocked(dir, lk)
	if retryErr != nil {
		_ = lk.Unlock()
		return nil, info, errors.Wrap(retryErr, "re-open journal after quarantine")
	}
	return j, info, nil
}

// Quarantine renames the journal that j has open aside and reopens a fresh
// empty journal under the same lock, returning the replacement. It is for a
// caller whose Recover (or Analyze) reported a broken-writer error: a
// structurally valid but semantically impossible record stream that Open
// cannot detect and that retrying OpenWithQuarantine would therefore never
// replace. The original bytes are preserved at QuarantineInfo.QuarantinedPath
// for offline inspection; cause is recorded in QuarantineInfo.OpenError.
//
// j is consumed: its data-file fd is closed and its lock is transferred to
// the returned journal, so the caller must stop using j and switch to the
// returned Journal. j.Close remains safe to call — it becomes a no-op and
// never releases the transferred lock. On failure the lock is released and
// j is left closed.
//
// Quarantining discards the in-flight transactions that were durable in the
// broken file; the caller is responsible for reconciling on-disk state.
func Quarantine(j *Journal, cause error) (*Journal, *QuarantineInfo, error) {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil, nil, errors.New("journal is closed")
	}
	// Transfer the flock to the replacement: close our fd but do NOT unlock,
	// so the lock is held continuously across the rename+reopen. Mark this
	// handle closed and drop its lock reference so its own Close is a no-op
	// and can never release the transferred lock.
	lk := j.lock
	dir := j.dir
	if j.f != nil {
		_ = j.f.Close()
		j.f = nil
	}
	j.lock = nil
	j.closed = true
	j.mu.Unlock()
	return quarantineFileLocked(dir, lk, cause)
}

// validateAndTruncateTorn scans the file from offset 0. A short read at
// the very end of the file (torn header / torn payload) is truncated
// away and the file is fsynced; this is the normal crash-recovery path.
// Any other structural failure (bad magic, unsupported version, bad
// CRC, oversize payload) is treated as mid-stream corruption and
// returned as an error so OpenWithQuarantine can rename the file aside
// for offline inspection rather than silently discarding durable
// records.
func (j *Journal) validateAndTruncateTorn() (int, error) {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	r := &recordReader{f: j.f}
	count := 0
	for {
		off := r.off
		_, _, err := r.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if !errors.Is(err, errTornTail) {
				return count, errors.Mark(errors.Wrapf(err, "journal corruption at offset %d", off), errJournalCorrupt)
			}
			// Torn tail: truncate at the offset where the bad record starts.
			if errTrunc := j.f.Truncate(off); errTrunc != nil {
				return count, errors.Wrap(errTrunc, "truncate torn tail")
			}
			if errSeek := j.seekEnd(); errSeek != nil {
				return count, errSeek
			}
			if errSync := j.f.Sync(); errSync != nil {
				return count, errSync
			}
			// fsync the parent directory so the truncated length is durable;
			// matches the create path's syncDir to keep the dirent and file
			// metadata consistent across crashes.
			if errDir := syncDir(j.dir); errDir != nil {
				return count, errors.Wrap(errDir, "sync journal parent directory after truncate")
			}
			return count, nil
		}
		count++
	}
	return count, j.seekEnd()
}

func (j *Journal) seekEnd() error {
	_, err := j.f.Seek(0, io.SeekEnd)
	return err
}

// Scan returns all records currently in the journal in order. Used by
// recovery and by the journal-dump CLI. Scan normally observes only
// records that passed Open's validation; it can however surface a
// mid-stream corruption error if a write attempt failed and the
// in-process rollback (truncate+seek+sync after a Write or Sync error)
// was also unable to clean up (e.g. full disk, EROFS). Callers should
// treat that the same as Open's corruption surface: stop using the
// journal and quarantine.
func (j *Journal) Scan() (out []Record, retErr error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errors.New("journal is closed")
	}
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	// Restore the append offset after scanning. A failed re-seek would leave
	// the next append at the wrong position, so surface it — but only when
	// the scan itself succeeded, so a real corruption error is not masked.
	defer func() {
		if err := j.seekEnd(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	r := &recordReader{f: j.f}
	for {
		t, payload, err := r.next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			// Mid-stream corruption observed after Open succeeded.
			// This means a prior write attempt failed AND its in-process
			// truncate/sync rollback also failed (a rare double fault).
			return out, err
		}
		out = append(out, Record{Type: t, Payload: payload})
	}
}

// ScanFile reads all complete records from the journal file at path
// without acquiring the flock. It is intended for read-only debug tools
// (e.g. journal-dump) that need to inspect a journal that may belong to
// a running process. A torn tail (short read while reading a
// header or payload) is treated as a clean end of stream so callers see
// the same set of records recovery would observe. Mid-stream corruption
// (bad magic / version / CRC / oversize payload) is surfaced as an
// error alongside the records read so far.
//
// The snapshot is not atomic: because no flock is taken, a concurrent
// owner may be appending or checkpoint-truncating the file. Callers may
// therefore observe a torn tail for an in-progress append or a shorter
// record set straddling a truncation. The view is always bounded and
// self-consistent up to the returned records; re-run to observe later
// state.
func ScanFile(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []Record
	r := &recordReader{f: f}
	for {
		t, payload, err := r.next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			if errors.Is(err, errTornTail) {
				return out, nil
			}
			return out, err
		}
		out = append(out, Record{Type: t, Payload: payload})
	}
}

// Size returns the current journal file size.
func (j *Journal) Size() (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return 0, errors.New("journal is closed")
	}
	if j.f == nil {
		return 0, errors.New("journal file is nil")
	}
	st, err := j.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// Close releases the dedicated-lock-file flock and closes the journal
// data file's fd. If the journal's recovery state is known (it was empty
// at Open, or Recover has run) AND no transaction is in flight AND no
// recovered pending transaction is still awaiting AdoptTxn, Close also
// writes a final CHECKPOINT and truncates the journal to zero so the next
// Open starts empty; a checkpoint I/O failure here is non-fatal because
// the on-disk records remain a valid input for the next Open's recovery.
// If recovery state is unknown (a non-empty journal was opened but never
// recovered), or txns are still in flight, or recovered pending txns were
// never adopted, no checkpoint is attempted so recovery on the next Open
// can replay them. Safe to call multiple times.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true

	var firstErr error
	if j.f != nil {
		if j.recovered && j.inFlight == 0 && len(j.recoveredPending) == 0 {
			// Best-effort clean checkpoint so the next Open starts empty.
			// Intentionally do not propagate a checkpoint failure: the
			// pre-checkpoint records are still durable and recoverable on
			// the next Open, so the caller's view of "Close succeeded"
			// remains correct.
			_ = j.checkpointLocked()
		}
		if err := j.f.Close(); err != nil {
			firstErr = err
		}
	}
	if j.lock != nil {
		if err := j.lock.Unlock(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Checkpoint writes a CHECKPOINT record, truncates the journal to zero, and
// fsyncs. Callers must only checkpoint when no transaction is in flight
// AND no recovered pending transaction is still awaiting AdoptTxn;
// otherwise the truncate would discard durable records belonging to
// in-flight or unadopted recovered txns and crash-recovery would never
// see them. Checkpoint also refuses until the journal's recovery state is
// known: on a journal that was non-empty at Open, Recover must run first,
// so un-analyzed durable pending transactions are never truncated away.
func (j *Journal) Checkpoint() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	// Guard here rather than in checkpointLocked: Close sets j.closed before
	// calling checkpointLocked for its clean-shutdown truncation, so a guard
	// there would skip it. A public Checkpoint after Close would otherwise
	// leak an OS closed-file error, and after Quarantine (j.f == nil) reach
	// appendRecordLocked and panic on the nil file.
	if j.closed {
		return errors.New("journal is closed")
	}
	return j.checkpointLocked()
}

func (j *Journal) checkpointLocked() error {
	if !j.recovered {
		return errors.New("refusing to checkpoint before recovery state is known; " +
			"call Recover after Open (a non-empty journal may hold durable pending transactions)")
	}
	if j.inFlight > 0 {
		return errors.Errorf("refusing to checkpoint with %d in-flight transaction(s)", j.inFlight)
	}
	if n := len(j.recoveredPending); n > 0 {
		return errors.Errorf("refusing to checkpoint with %d recovered pending transaction(s) not yet adopted", n)
	}
	payload, err := json.Marshal(CheckpointPayload{NextTxnID: j.nextTxnID})
	if err != nil {
		return err
	}
	if err := j.appendRecordLocked(RecCheckpoint, payload); err != nil {
		return err
	}
	// The CHECKPOINT record above is written and synced before this truncate
	// on purpose: if we crash in the window between the two, the next Open
	// still finds a durable CHECKPOINT carrying NextTxnID and an empty
	// pending set, so recovery is a no-op. After the truncate the file is
	// simply empty. The extra record per checkpoint is the deliberate cost
	// of covering that crash window.
	if err := j.f.Truncate(0); err != nil {
		return err
	}
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return j.f.Sync()
}

// Begin starts a new transaction. paramsJSON may be nil. Begin refuses
// until recovery state is established, because a reopened non-empty
// journal initializes nextTxnID to 1 and issuing from there would collide
// with a TxnID already durable on disk and make the journal
// unrecoverable. A freshly created (empty) journal is recovered at Open,
// so it needs no explicit Recover; a non-empty journal must first run
// Recover (or the manual Scan+Analyze+SetNextTxnID path followed by
// MarkRecovered).
func (j *Journal) Begin(op Op, paramsJSON []byte) (*Txn, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errors.New("journal is closed")
	}
	if !j.recovered {
		return nil, errors.New("refusing to Begin before recovery state is known; " +
			"call Recover after Open (or the manual Scan+Analyze+SetNextTxnID path then MarkRecovered) " +
			"so a new TxnID cannot collide with a transaction already durable on disk")
	}
	id := j.nextTxnID
	payload, err := json.Marshal(TxnBeginPayload{TxnID: id, Op: op, Params: paramsJSON})
	if err != nil {
		return nil, err
	}
	if err := j.appendRecordLocked(RecTxnBegin, payload); err != nil {
		return nil, err
	}
	// Only consume the id and bump bookkeeping after the record is
	// durably appended; a failed append must not leave an ID hole.
	j.nextTxnID = id + 1
	j.inFlight++
	return &Txn{j: j, id: id, op: op}, nil
}

// Recover scans the journal, analyzes it, and advances the in-memory
// next TxnID to the value reported by Analyze. Must be called once on a
// freshly-opened Journal before any Begin or AdoptTxn so newly-issued
// TxnIDs do not collide with txns still durable on disk. Recover
// returns an error if any transaction is already in flight (it would
// otherwise install a stale recovered-pending snapshot that includes
// the live txn's id and block all future checkpoint/truncation).
// Callers should prefer Recover over manually invoking Scan, Analyze,
// and SetNextTxnID, so a missing SetNextTxnID call cannot cause new
// Begin transactions to collide with txns that are already durable on
// disk.
//
// Recover also remembers the set of returned pending TxnIDs so that
// Checkpoint and Close-time truncation will refuse to discard their
// on-disk records until each one has been picked up by AdoptTxn and
// resolved (Commit or Abort), and so that AdoptTxn will reject ids that
// were not actually observed pending on disk. Callers that use the
// manual Scan+Analyze+SetNextTxnID path do NOT get either safety net.
func (j *Journal) Recover() (*Analysis, error) {
	recs, err := j.Scan()
	if err != nil {
		return nil, err
	}
	a, err := Analyze(recs)
	if err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.inFlight > 0 {
		return nil, errors.Errorf("Recover called with %d in-flight transaction(s); "+
			"call Recover once on a freshly-opened Journal before any Begin/AdoptTxn", j.inFlight)
	}
	if a.NextTxnID > j.nextTxnID {
		j.nextTxnID = a.NextTxnID
	}
	j.recovered = true
	j.recoveredPending = make(map[TxnID]*recoveredTxn, len(a.Pending))
	for _, p := range a.Pending {
		rt := &recoveredTxn{
			prepared: p.Prepared,
			required: make(map[uint32]struct{}, len(p.PendingIntents)),
			done:     make(map[uint32]struct{}, len(p.CompletedSteps)),
		}
		for _, in := range p.PendingIntents {
			rt.required[in.StepID] = struct{}{}
		}
		for stepID, ok := range p.CompletedSteps {
			if ok {
				rt.done[stepID] = struct{}{}
			}
		}
		j.recoveredPending[p.ID] = rt
	}
	return a, nil
}

// SetNextTxnID forces the next-issued TxnID, monotonically (it never
// regresses). Recover advances nextTxnID inline; SetNextTxnID is
// intended for callers that drive the manual Scan + Analyze path
// instead of Recover. Must be called before any Begin or AdoptTxn.
// SetNextTxnID alone does not unblock Begin: the manual path must also
// call MarkRecovered once it has established the next ID.
func (j *Journal) SetNextTxnID(id TxnID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if id > j.nextTxnID {
		j.nextTxnID = id
	}
}

// MarkRecovered records that recovery has been completed, unblocking
// Begin (and permitting Checkpoint / clean-Close truncation). Recover
// does this itself; MarkRecovered is for callers driving the manual
// Scan + Analyze + SetNextTxnID path, who must call it once the next
// TxnID has been established. It does NOT install the recovered-pending
// safety net, so a manual-path caller remains responsible for not
// checkpointing away transactions that are still pending on disk.
func (j *Journal) MarkRecovered() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recovered = true
}

// AdoptTxn returns a Txn handle for an existing in-flight transaction
// already present in the journal (typically discovered by recovery).
// The caller is expected to call StepDone for each replayed step and
// then Commit (or Abort). No TXN_BEGIN is written. Returns an error if
// the journal has already been closed, if Recover was not called
// beforehand, or if id is not in the set of pending TxnIDs the most
// recent Recover() returned (rejecting double-adopt, ids whose
// COMMIT/ABORT is already durable, and phantom / typo ids whose
// TXN_BEGIN is not on disk).
//
// On success, id is removed from the recovered-pending set so a
// subsequent Checkpoint or clean Close may truncate the journal once
// this Txn's Commit/Abort drops inFlight back to zero.
func AdoptTxn(j *Journal, id TxnID, op Op) (*Txn, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errors.New("journal is closed")
	}
	rt, ok := j.recoveredPending[id]
	if !ok {
		return nil, errors.Errorf("AdoptTxn id %d is not a recovered pending transaction "+
			"(did you forget to call Recover, already adopt it, or pass a stale id?)", id)
	}
	delete(j.recoveredPending, id)
	j.inFlight++
	// rt is now orphaned from the map, so its maps can be handed to the Txn.
	// Seeding prepared/steps/done lets Commit enforce the
	// prepared-and-complete invariant across the adoption boundary; adopted
	// marks the handle so Prepare cannot seal a partial post-crash intent set.
	return &Txn{j: j, id: id, op: op, adopted: true, prepared: rt.prepared, steps: rt.required, done: rt.done}, nil
}

// appendRecordLocked writes one record and fdatasyncs the journal file.
// Caller must hold j.mu. If a write or fdatasync fails and the rollback
// that would remove the partial/complete record also fails, the journal
// is poisoned: this and all later appends return an ErrJournalPoisoned-
// marked error so a retry cannot leave a duplicate record on disk.
func (j *Journal) appendRecordLocked(t RecordType, payload []byte) error {
	if j.poisoned != nil {
		return j.poisoned
	}
	if len(payload) > MaxPayloadSize {
		return errors.Errorf("journal record payload too large: %d", len(payload))
	}
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], magic)
	binary.LittleEndian.PutUint16(buf[4:6], version)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(t))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(payload)))
	// Two independent CRC32C sums: HeaderCRC over the framing (buf[0:12])
	// protects PayloadLen so recovery can trust it before reading the
	// payload, and PayloadCRC protects the payload bytes. Keeping them
	// separate lets recovery distinguish a torn tail from a length whose
	// bit flip would otherwise swallow the records that follow it.
	binary.LittleEndian.PutUint32(buf[12:16], crc32.Checksum(buf[0:12], crcTable))
	binary.LittleEndian.PutUint32(buf[16:20], crc32.Checksum(payload, crcTable))
	copy(buf[headerSize:], payload)

	// Capture the offset before writing so we can roll back a partial
	// write. Otherwise, a later successful append would sit after a
	// torn record and be truncated together with it on next Open.
	startOff, err := j.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "journal tell")
	}
	if _, err := j.f.Write(buf); err != nil {
		if rbErr := j.rollbackToLocked(startOff); rbErr != nil {
			return j.poisonLocked(err, rbErr)
		}
		return errors.Wrap(err, "journal write")
	}
	// fdatasync (not a full fsync): an append only changes the file's data
	// plus its size, and fdatasync(2) flushes the size metadata needed to
	// read the appended bytes back. Skipping the unrelated inode metadata
	// (mtime/atime) is cheaper on the per-record hot path. Platforms without
	// fdatasync fall back to a full fsync (see dataSync).
	if err := dataSync(j.f); err != nil {
		if rbErr := j.rollbackToLocked(startOff); rbErr != nil {
			return j.poisonLocked(err, rbErr)
		}
		return errors.Wrap(err, "journal sync")
	}
	return nil
}

// poisonLocked records that a failed append could not be rolled back and
// returns the poison error. Caller must hold j.mu. Both the original
// append error and the rollback error are captured in the message, and
// ErrJournalPoisoned is the wrapped cause so callers can detect it via
// errors.Is and quarantine.
func (j *Journal) poisonLocked(appendErr, rollbackErr error) error {
	j.poisoned = errors.Wrapf(ErrJournalPoisoned,
		"append failed and rollback failed (a possibly-complete record may remain on disk; "+
			"append error: %v; rollback error: %v)", appendErr, rollbackErr)
	return j.poisoned
}

// rollbackToLocked truncates the journal back to off and re-seeks after a
// failed append. It returns an error if any step fails; the caller must
// then poison the journal, because a leftover CRC-valid record could be
// accepted by the next Open and turned into a duplicate operation.
func (j *Journal) rollbackToLocked(off int64) error {
	if err := j.f.Truncate(off); err != nil {
		return err
	}
	if _, err := j.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	return j.f.Sync()
}

// Txn is a single in-flight transaction.
type Txn struct {
	j        *Journal
	id       TxnID
	op       Op
	prepared bool
	// terminal is the end record (TXN_COMMIT or TXN_ABORT) durably written
	// for this transaction, or 0 while still in flight. It makes a repeat of
	// the same outcome idempotent while rejecting the opposite outcome, so a
	// Commit after a durable Abort (or vice versa) cannot be mistaken for
	// success.
	terminal RecordType
	// adopted marks a handle reconstructed by AdoptTxn from an on-disk
	// pending transaction. Its intent set is already sealed on disk, so
	// Prepare is refused: a recovered transaction that was not prepared
	// before the crash must only be aborted, never sealed and committed.
	adopted bool
	// steps is the set of step IDs that must be completed before commit:
	// intents recorded via Intent, or the recovered intent set for an
	// adopted transaction.
	steps map[uint32]struct{}
	// done is the set of step IDs marked applied via StepDone, seeded with
	// the already-durable steps for an adopted transaction.
	done map[uint32]struct{}
}

// ID returns the transaction id.
func (t *Txn) ID() TxnID { return t.id }

// Op returns the operation kind.
func (t *Txn) Op() Op { return t.op }

// Intent records that step stepID with the given action is about to start.
// argsJSON may be nil. Each stepID must be unique within a transaction; a
// duplicate returns an error without writing a record, because STEP_DONE
// completion is keyed by stepID and reusing one would let a single
// StepDone mask an unapplied step during recovery.
func (t *Txn) Intent(stepID uint32, action Action, argsJSON []byte) error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	// Duplicate step IDs are rejected: STEP_DONE completion is keyed by
	// StepID, so two intents sharing an ID would be marked done together
	// and the second could be skipped on crash recovery.
	if _, dup := t.steps[stepID]; dup {
		return errors.Errorf("duplicate intent step %d for txn %d", stepID, t.id)
	}
	payload, err := json.Marshal(IntentPayload{TxnID: t.id, StepID: stepID, Action: action, Args: argsJSON})
	if err != nil {
		return err
	}
	if err := t.j.appendRecordLocked(RecIntent, payload); err != nil {
		return err
	}
	if t.steps == nil {
		t.steps = make(map[uint32]struct{})
	}
	t.steps[stepID] = struct{}{}
	return nil
}

// StepDone records that step stepID has been durably applied.
func (t *Txn) StepDone(stepID uint32) error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	payload, err := json.Marshal(StepDonePayload{TxnID: t.id, StepID: stepID})
	if err != nil {
		return err
	}
	if err := t.j.appendRecordLocked(RecStepDone, payload); err != nil {
		return err
	}
	if t.done == nil {
		t.done = make(map[uint32]struct{})
	}
	t.done[stepID] = struct{}{}
	return nil
}

// Commit closes the transaction with TXN_COMMIT. It refuses unless the
// transaction has been prepared and every recorded intent step has a
// STEP_DONE, mirroring the invariant Analyze enforces on recovery: a
// premature commit that a later crash truncates before Close would
// otherwise turn a "successful" operation into an unrecoverable WAL.
func (t *Txn) Commit() error { return t.end(RecTxnCommit) }

// Abort closes the transaction with TXN_ABORT.
func (t *Txn) Abort() error { return t.end(RecTxnAbort) }

// Prepare records that the full intent set has been written and the
// transaction is now safe to redo on recovery. Callers must Prepare
// after writing every Intent and before Apply'ing the first step.
// Prepare may only be called once per transaction; a second call
// returns an error without writing another record. Prepare is also
// refused on an adopted (recovery-reconstructed) transaction: its
// intent set is already sealed on disk, so one that was not prepared
// before the crash must be aborted rather than sealed and committed.
func (t *Txn) Prepare() error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	if t.adopted {
		return errors.Errorf("refusing to Prepare adopted transaction %d: its intent set is sealed on disk; "+
			"a recovered transaction not prepared before the crash must be aborted, not committed", t.id)
	}
	if t.prepared {
		return errors.Errorf("transaction %d already prepared", t.id)
	}
	payload, err := json.Marshal(TxnEndPayload{TxnID: t.id})
	if err != nil {
		return err
	}
	if err := t.j.appendRecordLocked(RecTxnPrepare, payload); err != nil {
		return err
	}
	t.prepared = true
	return nil
}

// checkOpenLocked verifies the transaction can still accept writes.
// Caller must hold t.j.mu.
func (t *Txn) checkOpenLocked() error {
	if t.j.closed {
		return errors.New("journal is closed")
	}
	if t.terminal != 0 {
		return errors.New("transaction is closed")
	}
	return nil
}

func (t *Txn) end(rt RecordType) error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if t.terminal != 0 {
		// A repeat of the same outcome is idempotent; the opposite outcome is
		// rejected so an aborted transaction can never be reported committed.
		if t.terminal == rt {
			return nil
		}
		return errors.Errorf("transaction %d already finished with %s; refusing to apply %s",
			t.id, t.terminal, rt)
	}
	if t.j.closed {
		return errors.New("journal is closed")
	}
	if rt == RecTxnCommit {
		if !t.prepared {
			return errors.Errorf("refusing to commit unprepared transaction %d", t.id)
		}
		for stepID := range t.steps {
			if _, ok := t.done[stepID]; !ok {
				return errors.Errorf("refusing to commit transaction %d with incomplete step %d", t.id, stepID)
			}
		}
	}
	if rt == RecTxnAbort && t.prepared {
		// After Prepare, recovery promises to roll the transaction forward, so
		// a step may already be applied without a durable STEP_DONE. Aborting
		// would mark it finished and let a checkpoint erase the only replay
		// plan, leaving partial filesystem state; it must be committed instead.
		return errors.Errorf("refusing to abort prepared transaction %d: recovery rolls it forward, so it must be committed", t.id)
	}
	payload, err := json.Marshal(TxnEndPayload{TxnID: t.id})
	if err != nil {
		return err
	}
	if err := t.j.appendRecordLocked(rt, payload); err != nil {
		return err
	}
	t.terminal = rt
	t.j.inFlight--
	return nil
}

// recordReader iterates records starting at the current file offset.
type recordReader struct {
	f   *os.File
	off int64
}

// next reads one record. Returns io.EOF if cleanly past the end. Short
// reads in the header or payload (consistent with a crash mid-write at
// the very end of the file) are wrapped with errTornTail so the caller
// can safely truncate. All other structural failures (bad magic,
// unsupported version, bad header/payload CRC, oversize payload) return
// plain errors: the caller is expected to surface those for quarantine
// rather than silently truncating durable records.
func (r *recordReader) next() (RecordType, []byte, error) {
	hdr := make([]byte, headerSize)
	n, err := io.ReadFull(r.f, hdr)
	if errors.Is(err, io.EOF) {
		return 0, nil, io.EOF
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, nil, errors.Wrapf(errTornTail, "torn header: %d bytes at offset %d", n, r.off)
	}
	if err != nil {
		return 0, nil, err
	}
	gotMagic := binary.LittleEndian.Uint32(hdr[0:4])
	if gotMagic != magic {
		return 0, nil, errors.Errorf("bad magic 0x%08x at offset %d", gotMagic, r.off)
	}
	gotVer := binary.LittleEndian.Uint16(hdr[4:6])
	if gotVer != version {
		return 0, nil, errors.Errorf("unsupported journal version %d", gotVer)
	}
	// Validate the header CRC before trusting PayloadLen. A full header was
	// read, so a mismatch is genuine mid-stream corruption (e.g. a bit flip
	// in PayloadLen), NOT a torn tail: returning a plain error routes it to
	// quarantine instead of reading an inflated length that would consume
	// and then truncate away the valid records that follow.
	wantHdrCRC := binary.LittleEndian.Uint32(hdr[12:16])
	gotHdrCRC := crc32.Checksum(hdr[0:12], crcTable)
	if gotHdrCRC != wantHdrCRC {
		return 0, nil, errors.Errorf("bad header CRC at offset %d: got 0x%08x want 0x%08x", r.off, gotHdrCRC, wantHdrCRC)
	}
	t := RecordType(binary.LittleEndian.Uint16(hdr[6:8]))
	plen := binary.LittleEndian.Uint32(hdr[8:12])
	wantCRC := binary.LittleEndian.Uint32(hdr[16:20])
	if plen > MaxPayloadSize {
		return 0, nil, errors.Errorf("payload length %d over cap at offset %d", plen, r.off)
	}
	payload := make([]byte, plen)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return 0, nil, errors.Wrapf(errTornTail, "torn payload at offset %d: %v", r.off, err)
		}
		return 0, nil, errors.Wrap(err, "read payload")
	}
	gotCRC := crc32.Checksum(payload, crcTable)
	if gotCRC != wantCRC {
		return 0, nil, errors.Errorf("bad CRC at offset %d: got 0x%08x want 0x%08x", r.off, gotCRC, wantCRC)
	}
	r.off += int64(headerSize) + int64(plen)
	return t, payload, nil
}
