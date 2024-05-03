package replica

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	btypes "github.com/longhorn/backupstore/types"
	butil "github.com/longhorn/backupstore/util"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type BackupStatus struct {
	lock          sync.Mutex
	Name          string
	backingFile   *backingfile.BackingFile
	replica       *Replica
	volumeID      string
	SnapshotID    string
	Error         string
	Progress      int
	BackupURL     string
	State         ProgressState
	IsIncremental bool
	IsOpened      bool
}

func NewBackup(name, volumeID, snapID string, backingFile *backingfile.BackingFile) *BackupStatus {
	backupName := name
	if backupName == "" {
		backupName = butil.GenerateName("backup")
	}
	return &BackupStatus{
		Name:        backupName,
		backingFile: backingFile,
		State:       ProgressStateInProgress,
		volumeID:    volumeID,
		SnapshotID:  diskutil.GenerateSnapshotDiskName(snapID),
	}
}

// UpdateBackupStatus updates the backup status. The state is first-respected, but if
// - The errString is not empty, the state will be set to error.
// - The progress is 100, the state will be set to complete.
func (rb *BackupStatus) UpdateBackupStatus(snapID, volumeID string, state string, progress int, url string, errString string) error {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	rb.lock.Lock()
	defer rb.lock.Unlock()

	if !rb.isVolumeSnapshotMatched(id, volumeID) {
		return fmt.Errorf("invalid volume [%s] and snapshot [%s], not volume [%s], snapshot [%s]", rb.volumeID, rb.SnapshotID, volumeID, id)
	}

	rb.State = ProgressState(state)
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
	return to >= 0
}

func (rb *BackupStatus) OpenSnapshot(snapID, volumeID string) error {
	id := diskutil.GenerateSnapshotDiskName(snapID)
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if rb.IsOpened {
		return nil
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
	rb.IsOpened = true

	return nil
}

func (rb *BackupStatus) assertOpen(id, volumeID string) error {
	if rb.volumeID != volumeID || rb.SnapshotID != id {
		return fmt.Errorf("invalid volume [%s] and snapshot [%s], not volume [%s], snapshot [%s]", rb.volumeID, rb.SnapshotID, volumeID, id)
	}
	if !rb.IsOpened {
		return fmt.Errorf("volume [%s] and snapshot [%s] are not opened", volumeID, id)
	}
	return nil
}

func (rb *BackupStatus) isVolumeSnapshotMatched(id, volumeID string) bool {
	if rb.volumeID != volumeID || rb.SnapshotID != id {
		return false
	}
	return true
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
	rb.IsOpened = false

	return err
}

func (rb *BackupStatus) CompareSnapshot(snapID, compareSnapID, volumeID string) (*btypes.Mappings, error) {
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

	mappings := &btypes.Mappings{
		BlockSize: backupBlockSize,
	}
	mapping := btypes.Mapping{
		Offset: -1,
	}

	if err := rb.replica.Preload(false); err != nil {
		return nil, err
	}

	for i, val := range rb.replica.volume.location {
		if val <= byte(from) && val > byte(to) {
			offset := int64(i) * rb.replica.volume.sectorSize
			// align
			offset -= (offset % backupBlockSize)
			if mapping.Offset != offset {
				mapping = btypes.Mapping{
					Offset: offset,
					Size:   backupBlockSize,
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
