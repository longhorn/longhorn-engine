package replica

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

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

	stopOnce sync.Once
	stopChan chan struct{}
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
		stopChan:               make(chan struct{}),
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

func (status *RestoreStatus) OpenVolumeDev(volDevName string) (*os.File, string, error) {
	if _, err := os.Stat(volDevName); err == nil {
		logrus.WithError(err).Warnf("File %s for the restore exists, will remove and re-create it", volDevName)
		if err := os.RemoveAll(volDevName); err != nil {
			return nil, "", errors.Wrapf(err, "failed to clean up the existing file %v before restore", volDevName)
		}
	}

	file, err := os.Create(volDevName)
	return file, volDevName, err
}

func (status *RestoreStatus) CloseVolumeDev(volDev *os.File) error {
	if volDev == nil {
		return nil
	}
	return volDev.Close()
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
//  1. The new restore is failed before the actual restore is performed.
//  2. The existing files are not modified.
//  3. The current status has been updated/initialized for the new restore.
//
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

func (status *RestoreStatus) Stop() {
	status.stopOnce.Do(func() {
		close(status.stopChan)
	})
}

func (status *RestoreStatus) GetStopChan() chan struct{} {
	return status.stopChan
}
