package sync

import (
	"fmt"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-engine/replica"
	replicaClient "github.com/longhorn/longhorn-engine/replica/client"
	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

type BackupStatusInfo struct {
	Progress     int    `json:"progress"`
	BackupURL    string `json:"backupURL,omitempty"`
	BackupError  string `json:"backupError,omitempty"`
	SnapshotName string `json:"snapshotName"`
}

func (t *Task) CreateBackup(snapshot, dest string, labels []string, credential map[string]string) (string, error) {
	var replica *types.ControllerReplicaInfo

	if snapshot == VolumeHeadName {
		return "", fmt.Errorf("Can not backup the head disk in the chain")
	}

	volume, err := t.client.VolumeGet()
	if err != nil {
		return "", err
	}

	replicas, err := t.client.ReplicaList()
	if err != nil {
		return "", err
	}

	for _, r := range replicas {
		if r.Mode == types.RW {
			replica = r
			break
		}
	}

	if replica == nil {
		return "", fmt.Errorf("Cannot find a suitable replica for backup")
	}

	backup, err := t.createBackup(replica, snapshot, dest, volume.Name, labels, credential)
	if err != nil {
		return "", err
	}
	return backup, nil
}

func (t *Task) createBackup(replicaInController *types.ControllerReplicaInfo, snapshot, dest, volumeName string, labels []string, credential map[string]string) (string, error) {
	if replicaInController.Mode != types.RW {
		return "", fmt.Errorf("Can only create backup from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return "", err
	}

	rep, err := repClient.GetReplica()
	if err != nil {
		return "", err
	}

	diskName := replica.GenerateSnapshotDiskName(snapshot)
	if _, ok := rep.Disks[diskName]; !ok {
		return "", fmt.Errorf("Snapshot disk %s not found on replica %s", diskName, replicaInController.Address)
	}

	logrus.Infof("Backing up %s on %s, to %s", snapshot, replicaInController.Address, dest)

	backupID, err := repClient.CreateBackup(snapshot, dest, volumeName, labels, credential)
	if err != nil {
		return "", err
	}
	//Store the backupID - Replica IP mapping in controller
	if err := t.client.BackupReplicaMappingCreate(backupID, replicaInController.Address); err != nil {
		return "", err
	}
	return backupID, nil
}

func (t *Task) FetchBackupStatus(backupID string, replicaIP string) (*BackupStatusInfo, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaIP)
	if err != nil {
		logrus.Errorf("Cannot create a replica client for IP[%v]: %v", replicaIP, err)
		return nil, err
	}

	progress, url, backupErr, snapshot, err := repClient.GetBackupStatus(backupID)
	if err != nil {
		return nil, err
	}

	info := &BackupStatusInfo{
		Progress:     progress,
		BackupURL:    url,
		BackupError:  backupErr,
		SnapshotName: snapshot,
	}

	return info, nil
}

func (t *Task) RestoreBackup(backup string) error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("Can not restore from backup because %s is rebuilding", r.Address)
		}
	}

	// generate new snapshot and metafile as base for new volume
	snapshotID := util.UUID()
	snapshotFile := replica.GenerateSnapshotDiskName(snapshotID)
	for _, replica := range replicas {
		if err := t.restoreBackup(replica, backup, snapshotFile); err != nil {
			return err
		}
	}

	// call to controller to revert to sfile
	if err := t.client.VolumeRevert(snapshotID); err != nil {
		return fmt.Errorf("Fail to revert to snapshot %v", snapshotID)
	}
	return nil
}

func (t *Task) restoreBackup(replicaInController *types.ControllerReplicaInfo, backup string, snapshotFile string) error {
	if replicaInController.Mode != types.RW {
		return fmt.Errorf("Can only restore backup from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	logrus.Infof("Restoring backup %s on %s", backup, replicaInController.Address)

	if err := repClient.RestoreBackup(backup, snapshotFile); err != nil {
		logrus.Errorf("Failed restoring backup %s on %s", backup, replicaInController.Address)
		return err
	}
	return nil
}

func (t *Task) RestoreBackupIncrementally(backup, backupName, lastRestored string) error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("can not incrementally restore from backup because %s is rebuilding", r.Address)
		}
		if ok, err := t.isDirty(r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("BUG: replica %s of standby volume cannot be dirty", r.Address)
		}
	}

	snapshots, err := GetSnapshotsInfo(replicas)
	if err != nil {
		return err
	}
	if len(snapshots) != 2 {
		return fmt.Errorf("BUG: replicas %s of standby volume should contains 2 snapshots only: volume head and the restore file", replicas)
	}
	var snapshotName string
	for _, s := range snapshots {
		if s.Name != VolumeHeadName {
			snapshotName = s.Name
		}
	}
	snapshotDiskName := replica.GenerateSnapshotDiskName(snapshotName)

	isValidLastRestored := true
	if _, err := backupstore.InspectBackup(strings.Replace(backup, backupName, lastRestored, 1)); err != nil {
		logrus.Warnf("Invalid argument last-restored, cannot find related backup %v, will do full restoration, err: %v", lastRestored, err)
		isValidLastRestored = false
	}

	errorMap := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(len(replicas))

	for _, r := range replicas {
		go func(replica *types.ControllerReplicaInfo) {
			defer wg.Done()
			err := t.restoreBackupIncrementally(replica, snapshotDiskName, backup, lastRestored, isValidLastRestored)
			if err != nil {
				errorMap.Store(replica.Address, err)
			}
		}(r)
	}

	wg.Wait()
	for _, r := range replicas {
		if v, ok := errorMap.Load(r.Address); ok {
			err = v.(error)
			logrus.Errorf("replica %v failed to incrementally restore: %v", r.Address, err)
		}
	}

	return err
}

func (t *Task) restoreBackupIncrementally(replicaInController *types.ControllerReplicaInfo, snapshotDiskName, backup, lastRestored string, isValidLastRestored bool) error {
	if replicaInController.Mode != types.RW {
		return fmt.Errorf("can only incrementally restore backup from replica in mode RW, got mode %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	logrus.Infof("Incrementally restoring backup %s on %s", backup, replicaInController.Address)

	if isValidLastRestored {
		// may need to generate a temporary delta file for incrementally restore.
		// we won't directly restore to the snapshot since a crashed restoring will mess up the snapshot
		deltaFileName := replica.GenerateDeltaFileName(lastRestored)

		// incrementally restore to delta file
		if err := repClient.RestoreBackupIncrementally(backup, deltaFileName, lastRestored); err != nil {
			logrus.Errorf("Failed to incrementally restore backup %s on %s", backup, replicaInController.Address)
			return err
		}

		// coalesce delta file to snapshot/disk file
		if err = repClient.CoalesceFile(deltaFileName, snapshotDiskName); err != nil {
			logrus.Errorf("Failed to coalesce %s on %s: %v", deltaFileName, snapshotDiskName, err)
			return err
		}

		// cleanup
		if err = repClient.RemoveFile(deltaFileName); err != nil {
			logrus.Warnf("Failed to cleanup delta file %s: %v", deltaFileName, err)
		}
	} else {
		// cannot restore backup incrementally, do full restoration instead
		snapshotDiskMetaName := replica.GenerateSnapshotDiskMetaName(snapshotDiskName)
		tmpSnapshotDiskName := replica.GenerateSnapTempFileName(snapshotDiskName)
		tmpSnapshotDiskMetaName := replica.GenerateSnapshotDiskMetaName(tmpSnapshotDiskName)

		defer func() {
			// try to cleanup tmp files
			repClient.RemoveFile(tmpSnapshotDiskName)
			repClient.RemoveFile(tmpSnapshotDiskMetaName)
		}()

		if err = t.restoreBackup(replicaInController, backup, tmpSnapshotDiskName); err != nil {
			return fmt.Errorf("failed to do full restoration in RestoreBackupIncrementally: %v", err)
		}
		logrus.Infof("Replica %v done restoreBackup in RestoreBackupIncrementally", replicaInController.Address)

		// replace old snapshot
		if err = repClient.RenameFile(tmpSnapshotDiskName, snapshotDiskName); err != nil {
			return fmt.Errorf("failed to replace old snapshot %v with the fully restored file %v: %v", snapshotDiskName, tmpSnapshotDiskName, err)
		}
		if err = repClient.RenameFile(tmpSnapshotDiskMetaName, snapshotDiskMetaName); err != nil {
			return fmt.Errorf("failed to replace old snapshot meta %v with the fully restored meta file %v: %v", snapshotDiskMetaName, tmpSnapshotDiskMetaName, err)
		}
	}

	// snapshot files get changed, need reload
	_, err = repClient.ReloadReplica()
	if err != nil {
		logrus.Warnf("Failed to reload replica %v: %v", replicaInController.Address, err)
	}

	return nil
}
