// Package wal implements a crash-safe append-only write-ahead log for
// snapshot chain operations on a replica directory.
//
// On-disk layout:
//
//	+--------+--------+--------+------------+--------+----------+
//	| Magic  | Ver    | Type   | PayloadLen | CRC32C | Payload  |
//	| 4 B    | 2 B    | 2 B    | 4 B        | 4 B    | N B      |
//	+--------+--------+--------+------------+--------+----------+
//
// All integers are little-endian. CRC32C (Castagnoli) covers the rest of the
// header (Magic..PayloadLen) followed by the payload bytes. A torn tail at
// the end of the file (partial header / partial payload) is detected and
// silently truncated on Open: only fully-fsynced records are observed by
// recovery. Mid-stream corruption (bad magic / version / CRC / oversize
// payload) is treated as a fatal error so OpenWithQuarantine can rename
// the file aside for offline inspection instead of silently dropping
// durable records.
//
// Concurrency model: a Journal owns the data file and an advisory flock
// on a dedicated lock file (LockFileName) for the lifetime of the
// replica. A separate lock file is required because Unix rename is not
// blocked by an advisory flock on the renamed file; using one
// guarantees OpenWithQuarantine's data-file rename is exclusive against
// concurrent quarantine attempts in other processes. All write methods
// serialize through an internal mutex; the replica's existing top-level
// lock already serializes snapshot operations, this mutex is just
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

// FileName is the journal data file name inside the replica directory.
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
	version uint16 = 1

	headerSize = 4 + 2 + 2 + 4 + 4 // 16 bytes

	// MaxPayloadSize bounds a single record. Real records are tiny (<1 KB);
	// the cap is defensive against corruption that yields huge lengths.
	MaxPayloadSize = 1 << 20 // 1 MiB
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrJournalLocked is returned by Open if another process holds the flock.
var ErrJournalLocked = errors.New("journal is locked by another process")

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

// Journal is an append-only WAL bound to a replica directory.
type Journal struct {
	mu        sync.Mutex
	dir       string
	path      string
	f         *os.File
	lock      *flock.Flock
	nextTxnID TxnID
	inFlight  int
	closed    bool
	// recoveredPending is the set of pending TxnIDs reported by the most
	// recent Recover() that have not yet been picked up by AdoptTxn. Their
	// records are still durable on disk and MUST NOT be truncated by
	// Checkpoint or by Close's clean-shutdown checkpoint; otherwise the
	// next process start has nothing to replay.
	recoveredPending map[TxnID]struct{}
}

// Open opens (or creates) the journal in dir, acquires an exclusive
// non-blocking flock on a dedicated lock file (LockFileName),
// validates the existing journal data file by scanning to the first
// torn-tail record (which is truncated) and returns the Journal.
// Mid-stream corruption (bad magic / version / CRC / oversize
// payload) returns an error without modifying the data file so
// OpenWithQuarantine can rename it aside.
//
// Callers should normally call Recover once on the returned Journal
// before any Begin so newly-issued TxnIDs do not collide with txns
// already durable on disk.
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
		return nil, errors.Wrap(err, "failed to open journal")
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
	if _, err := j.validateAndTruncateTorn(); err != nil {
		_ = f.Close()
		// Do NOT release lk; caller (OpenWithQuarantine) may need it
		// to safely rename the data file aside.
		return nil, err
	}
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
// cleanly and was renamed aside so the replica could continue with a
// fresh empty journal. The original bytes are preserved at
// QuarantinedPath for offline inspection (e.g. via journal-dump).
type QuarantineInfo struct {
	OriginalPath    string
	QuarantinedPath string
	OpenError       error
}

func (q *QuarantineInfo) Error() string {
	return fmt.Sprintf("journal quarantined: %s -> %s (cause: %v)",
		q.OriginalPath, q.QuarantinedPath, q.OpenError)
}

// OpenWithQuarantine behaves like Open. If Open fails for a reason
// other than ErrJournalLocked or a flock-layer failure (which mean the
// journal is either owned by another process or sitting on a
// non-locking filesystem and must not be touched), the existing
// journal.log is renamed to journal.log.broken-<unix-nanos> and Open
// is retried once against a fresh empty file.
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
//   - (nil, nil, err): quarantine refused (ErrJournalLocked, flock-
//     layer failure, source file disappeared). The on-disk state is
//     unchanged.
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

	src := filepath.Join(dir, FileName)
	dst := filepath.Join(dir, fmt.Sprintf("%s.broken-%d", FileName, time.Now().UnixNano()))

	// Source might be missing (e.g. parent dir gone). With nothing to
	// rename, surface the original error.
	if _, statErr := os.Lstat(src); statErr != nil {
		_ = lk.Unlock()
		return nil, nil, openErr
	}
	if renameErr := os.Rename(src, dst); renameErr != nil {
		_ = lk.Unlock()
		return nil, nil, errors.Wrapf(renameErr, "quarantine journal %s -> %s (original open error: %v)",
			src, dst, openErr)
	}
	// Make the rename itself durable so a crash here doesn't resurrect
	// the broken file under the original name.
	if syncErr := syncDir(dir); syncErr != nil {
		_ = lk.Unlock()
		return nil, nil, errors.Wrap(syncErr, "sync journal parent directory after quarantine")
	}

	info := &QuarantineInfo{OriginalPath: src, QuarantinedPath: dst, OpenError: openErr}
	// Retry under the SAME lock so no other process can race in between.
	j, retryErr := openLogLocked(dir, lk)
	if retryErr != nil {
		_ = lk.Unlock()
		return nil, info, errors.Wrap(retryErr, "re-open journal after quarantine")
	}
	return j, info, nil
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
				return count, errors.Wrapf(err, "journal corruption at offset %d", off)
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
func (j *Journal) Scan() ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errors.New("journal is closed")
	}
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	defer func() { _ = j.seekEnd() }()
	var out []Record
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
// a running replica process. A torn tail (short read while reading a
// header or payload) is treated as a clean end of stream so callers see
// the same set of records recovery would observe. Mid-stream corruption
// (bad magic / version / CRC / oversize payload) is surfaced as an
// error alongside the records read so far.
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
// data file's fd. If no transaction is in flight AND no recovered
// pending transaction is still awaiting AdoptTxn, Close also writes a
// final CHECKPOINT and truncates the journal to zero so the next Open
// starts empty; a checkpoint I/O failure here is non-fatal because the
// on-disk records remain a valid input for the next Open's recovery.
// With txns still in flight (e.g. the process is exiting mid-operation)
// or with recovered pending txns that the caller never adopted, no
// checkpoint is attempted so recovery on the next Open can replay them.
// Safe to call multiple times.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true

	var firstErr error
	if j.f != nil {
		if j.inFlight == 0 && len(j.recoveredPending) == 0 {
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
// see them.
func (j *Journal) Checkpoint() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.checkpointLocked()
}

func (j *Journal) checkpointLocked() error {
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
	// Truncate to zero. The CHECKPOINT we just wrote is its own marker; on
	// the next Open the file will simply be empty and recovery has nothing
	// to do.
	if err := j.f.Truncate(0); err != nil {
		return err
	}
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return j.f.Sync()
}

// Begin starts a new transaction. paramsJSON may be nil.
func (j *Journal) Begin(op Op, paramsJSON []byte) (*Txn, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, errors.New("journal is closed")
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
	j.recoveredPending = make(map[TxnID]struct{}, len(a.Pending))
	for _, p := range a.Pending {
		j.recoveredPending[p.ID] = struct{}{}
	}
	return a, nil
}

// SetNextTxnID forces the next-issued TxnID, monotonically (it never
// regresses). Recover advances nextTxnID inline; SetNextTxnID is
// intended for callers that drive the manual Scan + Analyze path
// instead of Recover. Must be called before any Begin or AdoptTxn.
func (j *Journal) SetNextTxnID(id TxnID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if id > j.nextTxnID {
		j.nextTxnID = id
	}
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
	if _, ok := j.recoveredPending[id]; !ok {
		return nil, errors.Errorf("AdoptTxn id %d is not a recovered pending transaction "+
			"(did you forget to call Recover, already adopt it, or pass a stale id?)", id)
	}
	delete(j.recoveredPending, id)
	j.inFlight++
	return &Txn{j: j, id: id, op: op}, nil
}

// appendRecordLocked writes one record and fsyncs the journal file. Caller
// must hold j.mu.
func (j *Journal) appendRecordLocked(t RecordType, payload []byte) error {
	if len(payload) > MaxPayloadSize {
		return errors.Errorf("journal record payload too large: %d", len(payload))
	}
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], magic)
	binary.LittleEndian.PutUint16(buf[4:6], version)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(t))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(payload)))
	// Compute CRC over header[0:12] + payload, then patch into header[12:16].
	// Use crc32.Update to avoid the per-record hash.Hash allocation.
	crcSum := crc32.Update(0, crcTable, buf[0:12])
	crcSum = crc32.Update(crcSum, crcTable, payload)
	binary.LittleEndian.PutUint32(buf[12:16], crcSum)
	copy(buf[16:], payload)

	// Capture the offset before writing so we can roll back a partial
	// write. Otherwise, a later successful append would sit after a
	// torn record and be truncated together with it on next Open.
	startOff, err := j.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "journal tell")
	}
	if _, err := j.f.Write(buf); err != nil {
		j.rollbackToLocked(startOff)
		return errors.Wrap(err, "journal write")
	}
	// fdatasync would be cheaper, but Go only exposes File.Sync (fsync).
	// The journal file's metadata rarely changes after first creation, so
	// the cost difference is negligible.
	if err := j.f.Sync(); err != nil {
		j.rollbackToLocked(startOff)
		return errors.Wrap(err, "journal sync")
	}
	return nil
}

// rollbackToLocked truncates the journal back to off and re-seeks. It is
// best-effort: if the underlying fs is failing, the next Open's
// torn-tail logic will still drop the partial record.
func (j *Journal) rollbackToLocked(off int64) {
	_ = j.f.Truncate(off)
	_, _ = j.f.Seek(off, io.SeekStart)
	_ = j.f.Sync()
}

// Txn is a single in-flight transaction.
type Txn struct {
	j        *Journal
	id       TxnID
	op       Op
	prepared bool
	closed   bool
}

// ID returns the transaction id.
func (t *Txn) ID() TxnID { return t.id }

// Op returns the operation kind.
func (t *Txn) Op() Op { return t.op }

// Intent records that step stepID with the given action is about to start.
// argsJSON may be nil.
func (t *Txn) Intent(stepID uint32, action Action, argsJSON []byte) error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
	}
	payload, err := json.Marshal(IntentPayload{TxnID: t.id, StepID: stepID, Action: action, Args: argsJSON})
	if err != nil {
		return err
	}
	return t.j.appendRecordLocked(RecIntent, payload)
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
	return t.j.appendRecordLocked(RecStepDone, payload)
}

// Commit closes the transaction with TXN_COMMIT.
func (t *Txn) Commit() error { return t.end(RecTxnCommit) }

// Abort closes the transaction with TXN_ABORT.
func (t *Txn) Abort() error { return t.end(RecTxnAbort) }

// Prepare records that the full intent set has been written and the
// transaction is now safe to redo on recovery. Callers must Prepare
// after writing every Intent and before Apply'ing the first step.
// Prepare may only be called once per transaction; a second call
// returns an error without writing another record.
func (t *Txn) Prepare() error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if err := t.checkOpenLocked(); err != nil {
		return err
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
	if t.closed {
		return errors.New("transaction is closed")
	}
	return nil
}

func (t *Txn) end(rt RecordType) error {
	t.j.mu.Lock()
	defer t.j.mu.Unlock()
	if t.closed {
		return nil
	}
	if t.j.closed {
		return errors.New("journal is closed")
	}
	payload, err := json.Marshal(TxnEndPayload{TxnID: t.id})
	if err != nil {
		return err
	}
	if err := t.j.appendRecordLocked(rt, payload); err != nil {
		return err
	}
	t.closed = true
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
// unsupported version, bad CRC, oversize payload) return plain errors:
// the caller is expected to surface those for quarantine rather than
// silently truncating durable records.
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
	t := RecordType(binary.LittleEndian.Uint16(hdr[6:8]))
	plen := binary.LittleEndian.Uint32(hdr[8:12])
	wantCRC := binary.LittleEndian.Uint32(hdr[12:16])
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
	crcSum := crc32.Update(0, crcTable, hdr[0:12])
	crcSum = crc32.Update(crcSum, crcTable, payload)
	if crcSum != wantCRC {
		return 0, nil, errors.Errorf("bad CRC at offset %d: got 0x%08x want 0x%08x", r.off, crcSum, wantCRC)
	}
	r.off += int64(headerSize) + int64(plen)
	return t, payload, nil
}
