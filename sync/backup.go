package sync

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/controller/rest"
	"github.com/rancher/longhorn/replica"
	replicaClient "github.com/rancher/longhorn/replica/client"
	"github.com/rancher/longhorn/util"
)

func (t *Task) CreateBackup(snapshot, dest string) (string, error) {
	var replica *rest.Replica

	if snapshot == VolumeHeadName {
		return "", fmt.Errorf("Can not backup the head disk in the chain")
	}

	volume, err := t.client.GetVolume()
	if err != nil {
		return "", err
	}

	replicas, err := t.client.ListReplicas()
	if err != nil {
		return "", err
	}

	for _, r := range replicas {
		if r.Mode == "RW" {
			replica = &r
			break
		}
	}

	if replica == nil {
		return "", fmt.Errorf("Cannot find a suitable replica for backup")
	}

	backup, err := t.createBackup(replica, snapshot, dest, volume.Name)
	if err != nil {
		return "", err
	}
	return backup, nil
}

func (t *Task) createBackup(replicaInController *rest.Replica, snapshot, dest, volumeName string) (string, error) {
	if replicaInController.Mode != "RW" {
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

	backup, err := repClient.CreateBackup(snapshot, dest, volumeName)
	if err != nil {
		logrus.Errorf("Failed backing up %s on %s to %s", snapshot, replicaInController.Address, dest)
		return "", err
	}
	return backup, nil
}

func (t *Task) RestoreBackup(backup string) error {
	replicas, err := t.client.ListReplicas()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(&r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("Can not restore from backup because %s is rebuilding", r.Address)
		}
	}

	// generate new snapshot and metafile as base for new volume
	snapshotID := util.UUID()
	snapshotFile := replica.GenerateSnapshotDiskName(snapshotID)
	for _, replica := range replicas {
		if err := t.restoreBackup(&replica, backup, snapshotFile); err != nil {
			return err
		}
	}

	// call to controller to revert to sfile
	if err := t.client.RevertSnapshot(snapshotID); err != nil {
		return fmt.Errorf("Fail to revert to snapshot %v", snapshotID)
	}
	return nil
}

func (t *Task) restoreBackup(replicaInController *rest.Replica, backup string, snapshotFile string) error {
	if replicaInController.Mode != "RW" {
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
