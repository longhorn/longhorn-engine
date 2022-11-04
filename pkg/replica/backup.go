package replica

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type ProgressState string

const (
	snapBlockSize = 2 << 20 // 2MiB

	ProgressStateInProgress = ProgressState("in_progress")
	ProgressStateComplete   = ProgressState("complete")
	ProgressStateError      = ProgressState("error")
)

/*
type DeltaBlockBackupOperations interface {
	HasSnapshot(id, volumeID string) bool
	CompareSnapshot(id, compareID, volumeID string) (*metadata.Mappings, error)
	OpenSnapshot(id, volumeID string) error
	ReadSnapshot(id, volumeID string, start int64, data []byte) error
	CloseSnapshot(id, volumeID string) error
}
*/

type RestoreStatus struct {
	sync.RWMutex
	replicaAddress string
	Progress       int
	Error          string
	BackupURL      string
	State          ProgressState

	// The file that (temporarily) stores the data during the restoring.
	ToFileName string
	// The snapshot file that stores the restored data in the end.
	SnapshotDiskName string

	LastRestored           string
	CurrentRestoringBackup string
}

func NewRestore(snapshotName, replicaAddress, backupURL, currentRestoringBackup string) *RestoreStatus {
	return &RestoreStatus{
		replicaAddress:         replicaAddress,
		ToFileName:             snapshotName,
		SnapshotDiskName:       snapshotName,
		State:                  ProgressStateInProgress,
		Progress:               0,
		BackupURL:              backupURL,
		CurrentRestoringBackup: currentRestoringBackup,
	}
}

func (status *RestoreStatus) StartNewRestore(backupURL, currentRestoringBackup, toFileName, snapshotDiskName string, validLastRestoredBackup bool) {
	status.Lock()
	defer status.Unlock()
	status.ToFileName = toFileName

	status.Progress = 0
	status.Error = ""
	status.BackupURL = backupURL
	status.State = ProgressStateInProgress
	status.SnapshotDiskName = snapshotDiskName
	if !validLastRestoredBackup {
		status.LastRestored = ""
	}
	status.CurrentRestoringBackup = currentRestoringBackup
}

func (status *RestoreStatus) UpdateRestoreStatus(snapshot string, rp int, re error) {
	status.Lock()
	defer status.Unlock()

	status.ToFileName = snapshot
	status.Progress = rp
	if re != nil {
		if status.Error != "" {
			status.Error = fmt.Sprintf("%v: %v", re.Error(), status.Error)
		} else {
			status.Error = re.Error()
		}
		status.State = ProgressStateError
		status.CurrentRestoringBackup = ""
	}
}

func (status *RestoreStatus) FinishRestore() {
	status.Lock()
	defer status.Unlock()
	if status.State != ProgressStateError {
		status.State = ProgressStateComplete
		status.LastRestored = status.CurrentRestoringBackup
		status.CurrentRestoringBackup = ""
	}
}

// Revert is used for reverting the current restore status to the previous status.
// This function will be invoked when:
//     1. The new restore is failed before the actual restore is performed.
//     2. The existing files are not modified.
//     3. The current status has been updated/initialized for the new restore.
// If there is no modification applied on the existing replica disk files after the restore failure,
// it means the replica is still available. In order to make sure the replica work fine
// for the next restore and the status is not messed up, the revert is indispensable.
func (status *RestoreStatus) Revert(previousStatus *RestoreStatus) {
	status.Lock()
	defer status.Unlock()

	status.BackupURL = previousStatus.BackupURL
	status.Progress = previousStatus.Progress
	status.State = previousStatus.State
	status.Error = previousStatus.Error
	status.ToFileName = previousStatus.ToFileName
	status.SnapshotDiskName = previousStatus.SnapshotDiskName
	status.LastRestored = previousStatus.LastRestored
	status.CurrentRestoringBackup = previousStatus.CurrentRestoringBackup
}

func (status *RestoreStatus) DeepCopy() *RestoreStatus {
	status.RLock()
	defer status.RUnlock()
	return &RestoreStatus{
		ToFileName:             status.ToFileName,
		Progress:               status.Progress,
		Error:                  status.Error,
		LastRestored:           status.LastRestored,
		SnapshotDiskName:       status.SnapshotDiskName,
		BackupURL:              status.BackupURL,
		State:                  status.State,
		CurrentRestoringBackup: status.CurrentRestoringBackup,
	}
}

type BackupStatus struct {
	lock          sync.Mutex
	backingFile   *backingfile.BackingFile
	replica       *Replica
	volumeID      string
	SnapshotID    string
	Error         string
	Progress      int
	BackupURL     string
	State         ProgressState
	IsIncremental bool
}

func NewBackup(backingFile *backingfile.BackingFile) *BackupStatus {
	return &BackupStatus{
		backingFile: backingFile,
		State:       ProgressStateInProgress,
	}
}

func (rb *BackupStatus) UpdateBackupStatus(snapID, volumeID string, progress int, url string, errString string) error {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if err := rb.assertOpen(id, volumeID); err != nil {
		logrus.Errorf("Returning Error from UpdateBackupProgress")
		return err
	}

	rb.Progress = progress
	rb.BackupURL = url
	rb.Error = errString

	if rb.Progress == 100 {
		rb.State = ProgressStateComplete
	} else if rb.Error != "" {
		rb.State = ProgressStateError
	}
	return nil
}

func (rb *BackupStatus) HasSnapshot(snapID, volumeID string) bool {
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if rb.volumeID != volumeID {
		logrus.Warnf("Invalid state volume [%s] are open, not [%s]", rb.volumeID, volumeID)
		return false
	}
	id := diskutil.GenerateSnapshotDiskName(snapID)
	to := rb.findIndex(id)
	if to < 0 {
		return false
	}
	return true
}

func (rb *BackupStatus) OpenSnapshot(snapID, volumeID string) error {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if rb.volumeID == volumeID && rb.SnapshotID == id {
		return nil
	}

	if rb.volumeID != "" {
		return fmt.Errorf("volume %s and snapshot %s are already open, close first", rb.volumeID, rb.SnapshotID)
	}

	dir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "cannot get working directory")
	}
	r, err := NewReadOnly(dir, id, rb.backingFile)
	if err != nil {
		return err
	}

	rb.replica = r
	rb.volumeID = volumeID
	rb.SnapshotID = id

	return nil
}

func (rb *BackupStatus) assertOpen(id, volumeID string) error {
	if rb.volumeID != volumeID || rb.SnapshotID != id {
		return fmt.Errorf("invalid state volume [%s] and snapshot [%s] are open, not volume [%s], snapshot [%s]", rb.volumeID, rb.SnapshotID, volumeID, id)
	}
	return nil
}

func (rb *BackupStatus) ReadSnapshot(snapID, volumeID string, start int64, data []byte) error {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if err := rb.assertOpen(id, volumeID); err != nil {
		return err
	}

	_, err := rb.replica.ReadAt(data, start)
	return err
}

func (rb *BackupStatus) CloseSnapshot(snapID, volumeID string) error {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if err := rb.assertOpen(id, volumeID); err != nil {
		return err
	}

	if rb.volumeID == "" {
		return nil
	}

	err := rb.replica.Close()

	rb.replica = nil
	rb.volumeID = ""
	//Keeping the SnapshotID value populated as this will be used by the engine for displaying the progress
	//associated with this snapshot.
	//Also, this serves the purpose to ensure if the snapshot file is open or not as assertOpen function will check
	//for both volumeID and SnapshotID to be ""

	//rb.snapshotID = ""

	return err
}

func (rb *BackupStatus) CompareSnapshot(snapID, compareSnapID, volumeID string) (*backupstore.Mappings, error) {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	compareID := ""
	if compareSnapID != "" {
		compareID = diskutil.GenerateSnapshotDiskName(compareSnapID)
	}
	rb.lock.Lock()
	if err := rb.assertOpen(id, volumeID); err != nil {
		rb.lock.Unlock()
		return nil, err
	}
	rb.lock.Unlock()

	rb.replica.Lock()
	defer rb.replica.Unlock()

	from := rb.findIndex(id)
	if from < 0 {
		return nil, fmt.Errorf("failed to find snapshot %s in chain", id)
	}

	to := rb.findIndex(compareID)
	if to < 0 {
		return nil, fmt.Errorf("failed to find snapshot %s in chain", compareID)
	}

	mappings := &backupstore.Mappings{
		BlockSize: snapBlockSize,
	}
	mapping := backupstore.Mapping{
		Offset: -1,
	}

	if err := rb.replica.Preload(false); err != nil {
		return nil, err
	}

	for i, val := range rb.replica.volume.location {
		if val <= byte(from) && val > byte(to) {
			offset := int64(i) * rb.replica.volume.sectorSize
			// align
			offset -= (offset % snapBlockSize)
			if mapping.Offset != offset {
				mapping = backupstore.Mapping{
					Offset: offset,
					Size:   snapBlockSize,
				}
				mappings.Mappings = append(mappings.Mappings, mapping)
			}
		}
	}

	return mappings, nil
}

func (rb *BackupStatus) findIndex(id string) int {
	if id == "" {
		if rb.backingFile == nil {
			return 0
		}
		return 1
	}

	for i, disk := range rb.replica.activeDiskData {
		if i == 0 {
			continue
		}
		if disk.Name == id {
			return i
		}
	}
	logrus.Warnf("Cannot find snapshot %s in activeDiskData list of volume %s", id, rb.volumeID)
	return -1
}
