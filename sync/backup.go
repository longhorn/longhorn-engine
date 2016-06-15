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

	replica, err := repClient.GetReplica()
	if err != nil {
		return "", err
	}

	snapshotName, index := getNameAndIndex(replica.Chain, snapshot)
	switch {
	case index < 0:
		return "", fmt.Errorf("Snapshot %s not found on replica %s", snapshot, replicaInController.Address)
	case index == 0:
		return "", fmt.Errorf("Can not backup the head disk in the chain")
	}

	logrus.Infof("Backing up %s on %s, to %s", snapshotName, replicaInController.Address, dest)

	backup, err := repClient.CreateBackup(snapshotName, dest, volumeName)
	if err != nil {
		logrus.Errorf("Failed backing up %s on %s to %s", snapshotName, replicaInController.Address, dest)
		return "", err
	}
	return backup, nil
}

func (t *Task) RmBackup(backup string) error {
	var replica *rest.Replica

	replicas, err := t.client.ListReplicas()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if r.Mode == "RW" {
			replica = &r
			break
		}
	}

	if replica == nil {
		return fmt.Errorf("Cannot find a suitable replica for remove backup")
	}

	if err := t.rmBackup(replica, backup); err != nil {
		return err
	}
	return nil
}

func (t *Task) rmBackup(replicaInController *rest.Replica, backup string) error {
	if replicaInController.Mode != "RW" {
		return fmt.Errorf("Can only remove backup from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	logrus.Infof("Removing backup %s on %s", backup, replicaInController.Address)

	if err := repClient.RmBackup(backup); err != nil {
		logrus.Errorf("Failed removing backup %s on %s", backup, replicaInController.Address)
		return err
	}
	return nil
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

func (t *Task) InspectBackup(backup string) (string, error) {
	var replica *rest.Replica

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
		return "", fmt.Errorf("Cannot find a suitable replica for inspect backup")
	}

	result, err := t.inspectBackup(replica, backup)
	if err != nil {
		return "", err
	}
	return result, nil
}

func (t *Task) inspectBackup(replicaInController *rest.Replica, backup string) (string, error) {
	if replicaInController.Mode != "RW" {
		return "", fmt.Errorf("Can only inspect backup from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return "", err
	}

	result, err := repClient.InspectBackup(backup)
	if err != nil {
		logrus.Errorf("Failed inspect backup %s on %s", backup, replicaInController.Address)
		return "", err
	}
	return result, nil
}
