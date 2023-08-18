package rpc

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica"
)

const (
	BackupListHighWaterMarkQuantum = 10

	MaxSnapshotHashJobSize = 10
)

// Count of "completed" records to keep in BackupList when pruning.
var retainBackupStateCounts = map[replica.ProgressState]int{
	replica.ProgressStateComplete: 5,
	replica.ProgressStateError:    10,
}

var backupListHighWaterMark = 0

type BackupList struct {
	sync.RWMutex
	infos []*BackupInfo
}

type BackupInfo struct {
	backupID     string
	backupStatus *replica.BackupStatus
}

type SnapshotHashList struct {
	sync.RWMutex
	infos []*SnapshotHashInfo
}

type SnapshotHashInfo struct {
	snapshotName string
	job          *replica.SnapshotHashJob
}

// The APIs BackupAdd, BackupGet, BackupDelete implement the CRUD interface for the backup object
// The slice Backup.backupList is implemented similar to a FIFO queue.

// BackupAdd creates a new backupList object and appends to the end of the list maintained by backup object
func (b *BackupList) BackupAdd(backupID string, backup *replica.BackupStatus) error {
	if backupID == "" {
		return fmt.Errorf("empty backupID")
	}

	// If a record already exists, remove it before adding the new one.
	// Ignore errors - we don't really expect to find it, normally.
	_ = b.BackupDelete(backupID)

	b.Lock()
	b.infos = append(b.infos, &BackupInfo{
		backupID:     backupID,
		backupStatus: backup,
	})
	b.Unlock()

	if backupListHighWaterMark < len(b.infos) {
		backupListHighWaterMark = len(b.infos)
		if 0 == backupListHighWaterMark%BackupListHighWaterMarkQuantum {
			logrus.Infof("New BackupList high water mark: %d", backupListHighWaterMark)
		}
	}

	if err := b.refresh(); err != nil {
		return err
	}

	return nil
}

// BackupGet takes backupID input and will return the backup object corresponding to that backupID or error if not found
func (b *BackupList) BackupGet(backupID string) (*replica.BackupStatus, error) {
	if backupID == "" {
		return nil, fmt.Errorf("empty backupID")
	}

	if err := b.refresh(); err != nil {
		return nil, err
	}

	b.RLock()
	defer b.RUnlock()

	for _, info := range b.infos {
		if info.backupID == backupID {
			return info.backupStatus, nil
		}
	}
	return nil, fmt.Errorf("backup not found %v", backupID)
}

// remove deletes the object present at slice[index] and returns the remaining elements of slice yet maintaining
// the original order of elements in the slice
func (*BackupList) remove(b []*BackupInfo, index int) ([]*BackupInfo, error) {
	if b == nil {
		return nil, fmt.Errorf("empty list")
	}
	if index >= len(b) || index < 0 {
		return nil, fmt.Errorf("BUG: attempting to delete an out of range index entry from backupList")
	}
	return append(b[:index], b[index+1:]...), nil
}

// Refresh deletes all the old completed backups from the front. Old backups are the completed backups
// (finished or error) that were created before the number we choose to retain.
func (b *BackupList) refresh() error {
	b.Lock()
	defer b.Unlock()

	for state, limit := range retainBackupStateCounts {
		var index, count int

		for index = len(b.infos) - 1; index >= 0; index-- {
			if b.infos[index].backupStatus.State == state {
				if count == limit {
					break
				}
				count++
			}
		}
		if count == limit {
			// Remove all the older completed backups in the range backupList[0:index]
			for ; index >= 0; index-- {
				if b.infos[index].backupStatus.State == state {
					updatedList, err := b.remove(b.infos, index)
					if err != nil {
						return err
					}
					b.infos = updatedList
					// As this backupList[index] is removed, will have to decrement the index by one
					index--
				}
			}
		}
	}
	return nil
}

// BackupDelete will delete the entry in the slice with the corresponding backupID
func (b *BackupList) BackupDelete(backupID string) error {
	b.Lock()
	defer b.Unlock()

	for index, backup := range b.infos {
		if backup.backupID == backupID {
			updatedList, err := b.remove(b.infos, index)
			if err != nil {
				return err
			}
			b.infos = updatedList
			return nil
		}
	}
	return fmt.Errorf("backup not found %v", backupID)
}

func (s *SnapshotHashList) Add(snapshotName string, job *replica.SnapshotHashJob) error {
	if snapshotName == "" {
		return fmt.Errorf("snapshot name is required")
	}

	if err := func() error {
		s.Lock()
		defer s.Unlock()
		for index, task := range s.infos {
			if task.snapshotName == snapshotName {
				if task.job == nil {
					return fmt.Errorf("BUG: snapshot %v job is nil", snapshotName)
				}

				if task.job.State == replica.ProgressStateComplete ||
					task.job.State == replica.ProgressStateError {
					updatedList, err := s.remove(s.infos, index)
					if err != nil {
						return err
					}
					s.infos = updatedList
					break
				} else {
					return fmt.Errorf("hashing snapshot %v is in progress", snapshotName)
				}
			}
		}

		s.infos = append(s.infos, &SnapshotHashInfo{
			snapshotName: snapshotName,
			job:          job,
		})
		return nil
	}(); err != nil {
		return err
	}

	if err := s.refresh(); err != nil {
		return err
	}

	return nil
}

func (s *SnapshotHashList) refresh() error {
	s.Lock()
	defer s.Unlock()

	purgeSnapshotHashInfos := []replica.ProgressState{
		replica.ProgressStateComplete,
		replica.ProgressStateError,
	}

	return s.purgePartialRetained(purgeSnapshotHashInfos, MaxSnapshotHashJobSize)
}

func (s *SnapshotHashList) purgePartialRetained(purgeSnapshotHashInfos []replica.ProgressState, limit int) error {
	// Note that looping over the purgeable states and resetting the count to zero means that
	// we will keep up to MaxSnapshotHashJobSize of each kind (completed or error).
	for _, state := range purgeSnapshotHashInfos {
		var index, completed int

		for index = len(s.infos) - 1; index >= 0; index-- {
			if s.infos[index].job.State == state {
				if completed == limit {
					break
				}
				completed++
			}
		}

		if completed == limit {
			// Remove all the older completed or error infos in the range snapshotHashList[0:index]
			for ; index >= 0; index-- {
				if s.infos[index].job.State == state {
					updatedList, err := s.remove(s.infos, index)
					if err != nil {
						return err
					}
					s.infos = updatedList
					// As this snapshotHashList[index] is removed, will have to decrement the index by one
					index--
				}
			}
		}
	}

	return nil
}

func (s *SnapshotHashList) remove(l []*SnapshotHashInfo, index int) ([]*SnapshotHashInfo, error) {
	if l == nil {
		return nil, fmt.Errorf("empty list")
	}
	if index >= len(l) || index < 0 {
		return nil, fmt.Errorf("BUG: attempting to delete an out of range index entry from snapshotHashList")
	}
	return append(l[:index], l[index+1:]...), nil
}

func (s *SnapshotHashList) Get(snapshotName string) (*replica.SnapshotHashJob, error) {
	if snapshotName == "" {
		return nil, fmt.Errorf("snapshot name is required")
	}

	if err := s.refresh(); err != nil {
		return nil, err
	}

	s.RLock()
	defer s.RUnlock()

	for _, job := range s.infos {
		if job.snapshotName == snapshotName {
			return job.job, nil
		}
	}
	return nil, fmt.Errorf("snapshot %v is not found", snapshotName)
}

// Delete will delete the entry in the slice with the corresponding snapshotName
func (s *SnapshotHashList) Delete(snapshotName string) error {
	s.Lock()
	defer s.Unlock()

	for index, job := range s.infos {
		if job.snapshotName == snapshotName {
			updatedList, err := s.remove(s.infos, index)
			if err != nil {
				return err
			}
			s.infos = updatedList
			return nil
		}
	}
	return nil
}

func (s *SnapshotHashList) GetSize() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.infos)
}
