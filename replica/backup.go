package replica

import (
	"fmt"
	"os"

	"github.com/rancher/convoy/metadata"
)

const (
	snapBlockSize = 2 << 20 // 2MiB
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

type Backup struct {
	backingFile *BackingFile
	replica     *Replica
	volumeID    string
	snapshotID  string
}

func NewBackup(backingFile *BackingFile) *Backup {
	return &Backup{
		backingFile: backingFile,
	}
}

func (rb *Backup) HasSnapshot(id, volumeID string) bool {
	//TODO Check current in the volume directory of volumeID
	if err := rb.assertOpen(id, volumeID); err != nil {
		return false
	}

	to := rb.findIndex(id)
	if to < 0 {
		return false
	}
	return true
}

func (rb *Backup) OpenSnapshot(id, volumeID string) error {
	if rb.volumeID == volumeID && rb.snapshotID == id {
		return nil
	}

	if rb.volumeID != "" {
		return fmt.Errorf("Volume %s and snapshot %s are already open, close first", rb.volumeID, rb.snapshotID)
	}

	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("Cannot get working directory: %v", err)
	}
	r, err := NewReadOnly(dir, id, rb.backingFile)
	if err != nil {
		return err
	}

	rb.replica = r
	rb.volumeID = volumeID
	rb.snapshotID = id

	return nil
}

func (rb *Backup) assertOpen(id, volumeID string) error {
	if rb.volumeID != volumeID || rb.snapshotID != id {
		return fmt.Errorf("Invalid state volume [%s] and snapshot [%s] are open, not [%s], [%s]", rb.volumeID, rb.snapshotID, id, volumeID)
	}
	return nil
}

func (rb *Backup) ReadSnapshot(id, volumeID string, start int64, data []byte) error {
	if err := rb.assertOpen(id, volumeID); err != nil {
		return err
	}

	if rb.snapshotID != id && rb.volumeID != volumeID {
		return fmt.Errorf("Snapshot %s and volume %s are not open", id, volumeID)
	}

	_, err := rb.replica.ReadAt(data, start)
	return err
}

func (rb *Backup) CloseSnapshot(id, volumeID string) error {
	if rb.volumeID == "" {
		return nil
	}

	if err := rb.assertOpen(id, volumeID); err != nil {
		return err
	}

	err := rb.replica.Close()

	rb.replica = nil
	rb.volumeID = ""
	rb.snapshotID = ""
	return err
}

func (rb *Backup) CompareSnapshot(id, compareID, volumeID string) (*metadata.Mappings, error) {
	if err := rb.assertOpen(id, volumeID); err != nil {
		return nil, err
	}

	rb.replica.Lock()
	defer rb.replica.Unlock()

	from := rb.findIndex(id)
	if from < 0 {
		return nil, fmt.Errorf("Failed to find snapshot %s in chain", id)
	}

	to := rb.findIndex(compareID)
	if to < 0 {
		return nil, fmt.Errorf("Failed to find snapshot %s in chain", compareID)
	}

	mappings := &metadata.Mappings{
		BlockSize: snapBlockSize,
	}
	mapping := metadata.Mapping{
		Offset: -1,
	}

	if err := preload(&rb.replica.volume); err != nil {
		return nil, err
	}

	for i, val := range rb.replica.volume.location {
		if val <= byte(from) && val > byte(to) {
			offset := int64(i) * rb.replica.volume.sectorSize
			// align
			offset -= (offset % snapBlockSize)
			if mapping.Offset != offset {
				mapping = metadata.Mapping{
					Offset: offset,
					Size:   snapBlockSize,
				}
				mappings.Mappings = append(mappings.Mappings, mapping)
			}
		}
	}

	return mappings, nil
}

func (rb *Backup) findIndex(id string) int {
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
		if disk.name == id {
			return i
		}
	}
	return -1
}

func preload(d *diffDisk) error {
	for i, f := range d.files {
		if i == 0 {
			continue
		}

		if i == 1 {
			// Reinitialize to zero so that we can detect holes in the base snapshot
			for j := 0; j < len(d.location); j++ {
				d.location[j] = 0
			}
		}

		generator := newGenerator(d, f)
		for offset := range generator.Generate() {
			d.location[offset] = byte(i)
		}

		if generator.Err() != nil {
			return generator.Err()
		}
	}

	return nil
}
