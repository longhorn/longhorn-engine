package replica

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gofrs/flock"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"

	xattrType "github.com/longhorn/sparse-tools/types"

	"github.com/longhorn/longhorn-engine/pkg/types"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

const (
	defaultHashMethod = "crc64"

	FileLockDirectory = "/host/var/lib/longhorn/.lock"
	HashLockFileName  = "hash"

	checkInterval = 10 * 1024 * 1024
)

type SnapshotHashStatus struct {
	StatusLock sync.RWMutex

	State             ProgressState
	Checksum          string
	Error             string
	SilentlyCorrupted bool
}

type SnapshotHashJob struct {
	sync.Mutex

	Ctx        context.Context
	CancelFunc context.CancelFunc

	SnapshotName string
	Rehash       bool

	SnapshotHashStatus
}

func NewSnapshotHashJob(ctx context.Context, cancel context.CancelFunc, snapshotName string, rehash bool) *SnapshotHashJob {
	return &SnapshotHashJob{
		Ctx:          ctx,
		CancelFunc:   cancel,
		SnapshotName: snapshotName,
		Rehash:       rehash,

		SnapshotHashStatus: SnapshotHashStatus{
			State: ProgressStateInProgress,
		},
	}
}

func (t *SnapshotHashJob) LockFile() (fileLock *flock.Flock, err error) {
	defer func() {
		if err != nil && fileLock != nil && fileLock.Path() != "" {
			if err := os.RemoveAll(fileLock.Path()); err != nil {
				logrus.WithError(err).Warnf("Failed to remove lock file %v", fileLock.Path())
			}
		}
	}()

	err = os.MkdirAll(FileLockDirectory, 0755)
	if err != nil {
		return nil, err
	}

	fileLock = flock.New(filepath.Join(FileLockDirectory, HashLockFileName))

	// Blocking lock
	err = fileLock.Lock()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to fetch the file lock for hashing snapshot %v", t.SnapshotName)
	}

	return fileLock, nil
}

func (t *SnapshotHashJob) UnlockFile(fileLock *flock.Flock) {
	if err := fileLock.Unlock(); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"snapshot": t.SnapshotName,
			"file":     fileLock.Path(),
		}).Warn("Failed to unlock the file lock")
	}
}

func (t *SnapshotHashJob) Execute() (err error) {
	var checksum string
	var changeTime string
	var lastHashedAt string
	var silentlyCorrupted bool

	defer func() {
		t.StatusLock.Lock()
		defer t.StatusLock.Unlock()
		t.Checksum = checksum
		t.SilentlyCorrupted = silentlyCorrupted

		if err == nil {
			t.State = ProgressStateComplete

			logrus.Infof("Snapshot %v checksum %v", t.SnapshotName, checksum)

			// Checksum is not recalculated in this task, so return directly.
			if lastHashedAt == "" {
				return
			}

			if err = SetSnapshotHashInfoToChecksumFile(t.SnapshotName, &xattrType.SnapshotHashInfo{
				Method:            defaultHashMethod,
				Checksum:          checksum,
				ChangeTime:        changeTime,
				LastHashedAt:      lastHashedAt,
				SilentlyCorrupted: silentlyCorrupted,
			}); err != nil {
				logrus.WithError(err).Warnf("failed to set snapshot %s hash info to checksum file", t.SnapshotName)
			}

			var remain bool
			remain, err = t.isChangeTimeRemain(changeTime)
			if !remain {
				if err == nil {
					err = fmt.Errorf("snapshot %v modification time is changed", t.SnapshotName)
				}
				// Do the best to delete the useless checksum file.
				// The deletion failure is acceptable, because the mismatching timestamps
				// will trigger the rehash in the next hash request.
				if deleteErr := DeleteSnapshotHashInfoChecksumFile(t.SnapshotName); deleteErr != nil {
					logrus.WithError(deleteErr).Warnf("failed to delete snapshot %v hash info checksum file", t.SnapshotName)
				}
			}
		}

		if err != nil {
			logrus.WithError(err).Errorf("failed to hash snapshot %v", t.SnapshotName)
			t.State = ProgressStateError
			t.Error = err.Error()
		}
	}()

	// Each node can have only one snapshot hashing task at the same time per node.
	// When the snapshot hashing task is started, the task tries to fetch the file lock
	// (${fileLockDirectory}/hash). If the lock file is held by another task, it will stuck
	// here and wait for the lock. The file is unlocked after the task is completed.
	fileLock, err := t.LockFile()
	if err != nil {
		return err
	}
	defer t.UnlockFile(fileLock)

	changeTime, err = GetSnapshotChangeTime(t.SnapshotName)
	if err != nil {
		return err
	}

	// If the silent corruption is detected, don't need to recalculate the checksum.
	// Just set SilentlyCorrupted to true and return it.
	silentlyCorrupted, err = t.isSilentCorruptionAlreadyDetected(changeTime)
	if err != nil {
		return err
	}
	if silentlyCorrupted {
		return nil
	}

	if !t.Rehash {
		var requireRehash bool
		requireRehash, checksum, err = t.isRehashRequired(changeTime)
		if err != nil {
			return err
		}
		if !requireRehash {
			return nil
		}
	}

	logrus.Infof("Starting hashing snapshot %v", t.SnapshotName)

	lastHashedAt = time.Now().UTC().Format(time.RFC3339)
	checksum, err = hashSnapshot(t.Ctx, t.SnapshotName)
	if err != nil {
		return err
	}

	// If the silent corruption is detected, the checksum file will not be overrode.
	// The scene will be preserved and only set silentlyCorrupted to true.
	if t.isSnapshotSilentlyCorrupted(checksum) {
		silentlyCorrupted = true

		info, err := GetSnapshotHashInfoFromChecksumFile(t.SnapshotName)
		if err != nil {
			return err
		}

		checksum = info.Checksum
		lastHashedAt = info.LastHashedAt
		changeTime = info.ChangeTime
	}

	return nil
}

func (t *SnapshotHashJob) isSnapshotSilentlyCorrupted(checksum string) bool {
	// To detect the silent corruption, read the changeTime and checksum already recorded in the snapshot disk file first.
	// Then, rehash the file and compare the changeTimes and checksums.
	// If the changeTimes are identical but the checksums differ, the file is silently corrupted.

	info, err := GetSnapshotHashInfoFromChecksumFile(t.SnapshotName)
	if err != nil || info == nil {
		return false
	}

	existingChecksum := info.Checksum
	existingChangeTime := info.ChangeTime

	if existingChecksum == "" || existingChangeTime == "" {
		return false
	}

	remain, _ := t.isChangeTimeRemain(existingChangeTime)
	if !remain {
		return false
	}

	if checksum != existingChecksum {
		return true
	}

	return false
}

func GetSnapshotChangeTime(snapshotName string) (string, error) {
	fileInfo, err := os.Stat(diskutil.GenerateSnapshotDiskName(snapshotName))
	if err != nil {
		return "", err
	}

	stat := fileInfo.Sys().(*syscall.Stat_t)
	return time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)).String(), nil
}

func GetSnapshotHashInfoFromChecksumFile(snapshotName string) (*xattrType.SnapshotHashInfo, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get working directory when getting snapshot hash info")
	}

	path := filepath.Join(dir, diskutil.GenerateSnapshotDiskChecksumName(diskutil.GenerateSnapshotDiskName(snapshotName)))

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close checksum file %v", path)
		}
	}()

	var info xattrType.SnapshotHashInfo

	if err := json.NewDecoder(f).Decode(&info); err != nil {
		return nil, err
	}

	return &info, nil
}

func SetSnapshotHashInfoToChecksumFile(snapshotName string, info *xattrType.SnapshotHashInfo) error {
	dir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "failed to get working directory when setting snapshot hash info")
	}

	path := filepath.Join(dir, diskutil.GenerateSnapshotDiskChecksumName(diskutil.GenerateSnapshotDiskName(snapshotName)))

	return encodeToFile(xattrType.SnapshotHashInfo{
		Method:            defaultHashMethod,
		Checksum:          info.Checksum,
		ChangeTime:        info.ChangeTime,
		LastHashedAt:      info.LastHashedAt,
		SilentlyCorrupted: info.SilentlyCorrupted,
	}, path)
}

func encodeToFile(obj interface{}, path string) (err error) {
	tmpPath := fmt.Sprintf("%s.%s", path, tmpFileSuffix)

	defer func() {
		var rollbackErr error
		if err != nil {
			if _, err := os.Stat(tmpPath); err == nil {
				if err := os.Remove(tmpPath); err != nil {
					rollbackErr = err
				}
			}
		}
		err = types.GenerateFunctionErrorWithRollback(err, rollbackErr)
	}()

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close checksum file %v", tmpPath)
		}
	}()

	if err := json.NewEncoder(f).Encode(&obj); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

func DeleteSnapshotHashInfoChecksumFile(snapshotName string) error {
	dir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "failed to get working directory when deleting snapshot hash info")
	}

	path := filepath.Join(dir, diskutil.GenerateSnapshotDiskChecksumName(diskutil.GenerateSnapshotDiskName(snapshotName)))

	return os.RemoveAll(path)
}

func (t *SnapshotHashJob) isSilentCorruptionAlreadyDetected(currentChangeTime string) (bool, error) {
	info, err := GetSnapshotHashInfoFromChecksumFile(t.SnapshotName)
	if err != nil || info == nil {
		if !strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			return false, errors.Wrapf(err, "failed to get snapshot %v last hash info from checksum file", t.SnapshotName)
		}
		return false, nil
	}

	if currentChangeTime == info.ChangeTime {
		return info.SilentlyCorrupted, nil
	}

	return false, nil
}

func (t *SnapshotHashJob) isRehashRequired(currentChangeTime string) (bool, string, error) {
	info, err := GetSnapshotHashInfoFromChecksumFile(t.SnapshotName)
	if err != nil || info == nil {
		if !strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			return true, "", errors.Wrapf(err, "failed to get snapshot %v last hash info from checksum file", t.SnapshotName)
		}
		return true, "", nil
	}

	checksum := info.Checksum
	changeTime := info.ChangeTime

	if changeTime != currentChangeTime || checksum == "" {
		return true, "", nil
	}

	return false, checksum, nil
}

func (t *SnapshotHashJob) isChangeTimeRemain(oldChangeTime string) (bool, error) {
	newChangeTime, err := GetSnapshotChangeTime(t.SnapshotName)
	if err != nil {
		return false, err
	}

	return oldChangeTime == newChangeTime, nil
}

func hashSnapshot(ctx context.Context, snapshotName string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", errors.Wrapf(err, "failed to get working directory when hashing snapshot %v", snapshotName)
	}

	path := filepath.Join(dir, diskutil.GenerateSnapshotDiskName(snapshotName))

	f, err := sparse.NewDirectFileIoProcessor(path, os.O_RDONLY, 0)
	if err != nil {
		return "", errors.Wrapf(err, "failed to open %v", path)
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Warnf("Failed to close file %v", path)
		}
	}()

	h, err := newHashMethod(defaultHashMethod)
	if err != nil {
		return "", err
	}

	if err := dataCopyWithExistenceCheck(ctx, h, f, path); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

type readerFunc func(p []byte) (n int, err error)

func (rf readerFunc) Read(p []byte) (n int, err error) {
	return rf(p)
}

func dataCopyWithExistenceCheck(ctx context.Context, dst io.Writer, src io.Reader, path string) error {
	var readBytes int64

	_, err := io.Copy(dst, readerFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			n, err := src.Read(p)
			readBytes += int64(n)

			// Check file existence every checkInterval bytes read
			if readBytes >= checkInterval {
				readBytes = 0
				if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
					return 0, errors.Errorf("file %v no longer exists", path)
				} else if statErr != nil {
					return 0, errors.Wrapf(statErr, "failed to stat file %v", path)
				}
			}

			return n, err
		}
	}))

	return err
}

func newHashMethod(method string) (hash.Hash, error) {
	switch method {
	case "crc64":
		return crc64.New(crc64.MakeTable(crc64.ISO)), nil
	default:
		return nil, fmt.Errorf("invalid hash method %v", method)
	}
}
