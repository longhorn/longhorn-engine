package backupstore

import (
	"fmt"

	"github.com/longhorn/backupstore/util"
)

type VolumeInfo struct {
	Name           string
	Size           int64 `json:",string"`
	Created        string
	LastBackupName string
	LastBackupAt   string
	DataStored     int64 `json:",string"`
	Deleting       bool

	Messages map[MessageType]string

	Backups map[string]*BackupInfo
}

type BackupInfo struct {
	Name            string
	URL             string
	SnapshotName    string
	SnapshotCreated string
	Created         string
	Size            int64 `json:",string"`
	Labels          map[string]string

	VolumeName    string `json:",omitempty"`
	VolumeSize    int64  `json:",string,omitempty"`
	VolumeCreated string `json:",omitempty"`
	Deleting      bool
}

func addListVolume(volumeName string, driver BackupStoreDriver, volumeOnly bool) (*VolumeInfo, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("Invalid empty volume Name")
	}

	if !util.ValidateName(volumeName) {
		return nil, fmt.Errorf("Invalid volume name %v", volumeName)
	}

	backupNames, err := getBackupNamesForVolume(volumeName, driver)
	if err != nil {
		return nil, err
	}

	volume, err := loadVolume(volumeName, driver)
	if err != nil {
		return &VolumeInfo{
			Name:     volumeName,
			Messages: map[MessageType]string{MessageTypeError: err.Error()},
			Backups:  make(map[string]*BackupInfo),
		}, nil
	}

	volumeInfo := fillVolumeInfo(volume)
	if volumeOnly {
		return volumeInfo, nil
	}

	for _, backupName := range backupNames {
		backup, err := loadBackup(backupName, volumeName, driver)
		if err != nil {
			return nil, err
		}
		r := fillBackupInfo(backup, driver.GetURL())
		volumeInfo.Backups[r.URL] = r
	}
	return volumeInfo, nil
}

func List(volumeName, destURL string, volumeOnly bool) (map[string]*VolumeInfo, error) {
	driver, err := GetBackupStoreDriver(destURL)
	if err != nil {
		return nil, err
	}
	resp := make(map[string]*VolumeInfo)
	if volumeName != "" {
		volumeInfo, err := addListVolume(volumeName, driver, volumeOnly)
		if err != nil {
			return nil, err
		}
		resp[volumeName] = volumeInfo
	} else {
		volumeNames, err := getVolumeNames(driver)
		if err != nil {
			return nil, err
		}
		for _, volumeName := range volumeNames {
			volumeInfo, err := addListVolume(volumeName, driver, volumeOnly)
			if err != nil {
				return nil, err
			}
			resp[volumeName] = volumeInfo
		}
	}
	return resp, nil
}

func fillVolumeInfo(volume *Volume) *VolumeInfo {
	return &VolumeInfo{
		Name:           volume.Name,
		Size:           volume.Size,
		Created:        volume.CreatedTime,
		LastBackupName: volume.LastBackupName,
		LastBackupAt:   volume.LastBackupAt,
		DataStored:     int64(volume.BlockCount * DEFAULT_BLOCK_SIZE),
		Messages:       make(map[MessageType]string),
		Backups:        make(map[string]*BackupInfo),
		Deleting:       volume.Deleting,
	}
}

func fillBackupInfo(backup *Backup, destURL string) *BackupInfo {
	return &BackupInfo{
		Name:            backup.Name,
		URL:             encodeBackupURL(backup.Name, backup.VolumeName, destURL),
		SnapshotName:    backup.SnapshotName,
		SnapshotCreated: backup.SnapshotCreatedAt,
		Created:         backup.CreatedTime,
		Size:            backup.Size,
		Labels:          backup.Labels,
		Deleting:        backup.Deleting,
	}
}

func fillFullBackupInfo(backup *Backup, volume *Volume, destURL string) *BackupInfo {
	info := fillBackupInfo(backup, destURL)
	info.VolumeName = volume.Name
	info.VolumeSize = volume.Size
	info.VolumeCreated = volume.CreatedTime
	return info
}

func InspectBackup(backupURL string) (*BackupInfo, error) {
	driver, err := GetBackupStoreDriver(backupURL)
	if err != nil {
		return nil, err
	}
	backupName, volumeName, err := decodeBackupURL(backupURL)
	if err != nil {
		return nil, err
	}

	volume, err := loadVolume(volumeName, driver)
	if err != nil {
		return nil, err
	}

	backup, err := loadBackup(backupName, volumeName, driver)
	if err != nil {
		return nil, err
	}
	return fillFullBackupInfo(backup, volume, driver.GetURL()), nil
}
