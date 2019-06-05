package sync

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/controller/client"
	"github.com/longhorn/longhorn-engine/controller/rest"
	"github.com/longhorn/longhorn-engine/replica"
	replicaClient "github.com/longhorn/longhorn-engine/replica/client"
	replicarpc "github.com/longhorn/longhorn-engine/replica/rpc"
	"github.com/longhorn/longhorn-engine/types"
)

var (
	RetryCounts = 3
)

const VolumeHeadName = "volume-head"

type Task struct {
	client *client.ControllerClient
}

func NewTask(controller string) *Task {
	return &Task{
		client: client.NewControllerClient(controller),
	}
}

func (t *Task) DeleteSnapshot(snapshot string) error {
	var err error

	replicas, err := t.client.ListReplicas()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(&r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("Can not remove a snapshot because %s is rebuilding", r.Address)
		}
	}

	for _, replica := range replicas {
		if err = t.markSnapshotAsRemoved(&replica, snapshot); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) PurgeSnapshots() error {
	var err error

	leaves := []string{}

	replicas, err := t.client.ListReplicas()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(&r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("Can not purge snapshots because %s is rebuilding", r.Address)
		}
	}

	snapshotsInfo, err := GetSnapshotsInfo(replicas)
	if err != nil {
		return err
	}
	for snapshot, info := range snapshotsInfo {
		if len(info.Children) == 0 {
			leaves = append(leaves, snapshot)
		}
		if info.Name == VolumeHeadName {
			continue
		}
		// Mark system generated snapshots as removed
		if !info.UserCreated && !info.Removed {
			for _, replica := range replicas {
				if err = t.markSnapshotAsRemoved(&replica, snapshot); err != nil {
					return err
				}
			}
		}
	}

	snapshotsInfo, err = GetSnapshotsInfo(replicas)
	if err != nil {
		return err
	}
	// We're tracing up from each leaf to the root
	for _, leaf := range leaves {
		// Somehow the leaf was removed during the process
		if _, ok := snapshotsInfo[leaf]; !ok {
			continue
		}
		snapshot := leaf
		for snapshot != "" {
			// Snapshot already removed? Skip to the next leaf
			info, ok := snapshotsInfo[snapshot]
			if !ok {
				break
			}
			if info.Removed {
				if info.Name == VolumeHeadName {
					return fmt.Errorf("BUG: Volume head was marked as removed")
				}

				for _, replica := range replicas {
					ops, err := t.prepareRemoveSnapshot(&replica, snapshot)
					if err != nil {
						return err
					}
					if err := t.processRemoveSnapshot(&replica, snapshot, ops); err != nil {
						return err
					}
				}
			}
			snapshot = info.Parent
		}
		// Update snapshotInfo in case some nodes have been removed
		snapshotsInfo, err = GetSnapshotsInfo(replicas)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) rmDisk(replicaInController *rest.Replica, disk string, force bool) error {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	return repClient.RemoveDisk(disk, force)
}

func (t *Task) replaceDisk(replicaInController *rest.Replica, target, source string) error {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	return repClient.ReplaceDisk(target, source)
}

func getNameAndIndex(chain []string, snapshot string) (string, int) {
	index := find(chain, snapshot)
	if index < 0 {
		snapshot = fmt.Sprintf("volume-snap-%s.img", snapshot)
		index = find(chain, snapshot)
	}

	if index < 0 {
		return "", index
	}

	return snapshot, index
}

func (t *Task) isRebuilding(replicaInController *rest.Replica) (bool, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return false, err
	}

	replica, err := repClient.GetReplica()
	if err != nil {
		return false, err
	}

	return replica.Rebuilding, nil
}

func (t *Task) isDirty(replicaInController *rest.Replica) (bool, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return false, err
	}

	replica, err := repClient.GetReplica()
	if err != nil {
		return false, err
	}

	return replica.Dirty, nil
}

func (t *Task) prepareRemoveSnapshot(replicaInController *rest.Replica, snapshot string) ([]*replicarpc.PrepareRemoveAction, error) {
	if replicaInController.Mode != "RW" {
		return nil, fmt.Errorf("Can only removed snapshot from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return nil, err
	}

	operations, err := repClient.PrepareRemoveDisk(snapshot)
	if err != nil {
		return nil, err
	}

	return operations, nil
}

func (t *Task) markSnapshotAsRemoved(replicaInController *rest.Replica, snapshot string) error {
	if replicaInController.Mode != "RW" {
		return fmt.Errorf("Can only mark snapshot as removed from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	if err := repClient.MarkDiskAsRemoved(snapshot); err != nil {
		return err
	}

	return nil
}

func (t *Task) processRemoveSnapshot(replicaInController *rest.Replica, snapshot string, ops []*replicarpc.PrepareRemoveAction) error {
	if len(ops) == 0 {
		return nil
	}

	if replicaInController.Mode != "RW" {
		return fmt.Errorf("Can only removed snapshot from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}

	for _, op := range ops {
		switch op.Action {
		case replica.OpRemove:
			logrus.Infof("Removing %s on %s", op.Source, replicaInController.Address)
			if err := t.rmDisk(replicaInController, op.Source, false); err != nil {
				return err
			}
		case replica.OpCoalesce:
			logrus.Infof("Coalescing %v to %v on %v", op.Target, op.Source, replicaInController.Address)
			if err = repClient.CoalesceFile(op.Target, op.Source); err != nil {
				logrus.Errorf("Failed to coalesce %s on %s: %v", snapshot, replicaInController.Address, err)
				return err
			}
		case replica.OpReplace:
			logrus.Infof("Replace %v with %v on %v", op.Target, op.Source, replicaInController.Address)
			if err = t.replaceDisk(replicaInController, op.Target, op.Source); err != nil {
				logrus.Errorf("Failed to replace %v with %v on %v", op.Target, op.Source, replicaInController.Address)
				return err
			}
		}
	}

	return nil
}

func find(list []string, item string) int {
	for i, val := range list {
		if val == item {
			return i
		}
	}
	return -1
}

func (t *Task) AddReplica(replica string) error {
	volume, err := t.client.GetVolume()
	if err != nil {
		return err
	}

	if volume.ReplicaCount == 0 {
		return t.client.Start(replica)
	}

	if err := t.checkAndResetFailedRebuild(replica); err != nil {
		return err
	}

	logrus.Infof("Adding replica %s in WO mode", replica)
	_, err = t.client.CreateReplica(replica)
	if err != nil {
		return err
	}

	fromClient, toClient, err := t.getTransferClients(replica)
	if err != nil {
		return err
	}

	if err := toClient.SetRebuilding(true); err != nil {
		return err
	}

	output, err := t.client.PrepareRebuild(rest.EncodeID(replica))
	if err != nil {
		return err
	}

	if err = t.syncFiles(fromClient, toClient, output.Disks); err != nil {
		return err
	}

	if err := t.reloadAndVerify(replica, toClient); err != nil {
		return err
	}

	return nil
}

func (t *Task) checkAndResetFailedRebuild(address string) error {
	client, err := replicaClient.NewReplicaClient(address)
	if err != nil {
		return err
	}

	replica, err := client.GetReplica()
	if err != nil {
		return err
	}

	if replica.State == "closed" && replica.Rebuilding {
		if err := client.OpenReplica(); err != nil {
			return err
		}

		if err := client.SetRebuilding(false); err != nil {
			return err
		}

		return client.Close()
	}

	return nil
}

func (t *Task) reloadAndVerify(address string, repClient *replicaClient.ReplicaClient) error {
	_, err := repClient.ReloadReplica()
	if err != nil {
		return err
	}

	if err := t.client.VerifyRebuildReplica(rest.EncodeID(address)); err != nil {
		return err
	}

	if err := repClient.SetRebuilding(false); err != nil {
		return err
	}
	return nil
}

func (t *Task) syncFiles(fromClient *replicaClient.ReplicaClient, toClient *replicaClient.ReplicaClient, disks []string) error {
	// volume head has been synced by PrepareRebuild()
	for _, disk := range disks {
		if strings.Contains(disk, VolumeHeadName) {
			return fmt.Errorf("Disk list shouldn't contain %s", VolumeHeadName)
		}
		if err := t.syncFile(disk, "", fromClient, toClient); err != nil {
			return err
		}

		if err := t.syncFile(disk+".meta", "", fromClient, toClient); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) syncFile(from, to string, fromClient *replicaClient.ReplicaClient, toClient *replicaClient.ReplicaClient) error {
	if to == "" {
		to = from
	}

	host, port, err := toClient.LaunchReceiver(to)
	if err != nil {
		return err
	}

	logrus.Infof("Synchronizing %s to %s@%s:%d", from, to, host, port)
	err = fromClient.SendFile(from, host, port)
	if err != nil {
		logrus.Infof("Failed synchronizing %s to %s@%s:%d: %v", from, to, host, port, err)
	} else {
		logrus.Infof("Done synchronizing %s to %s@%s:%d", from, to, host, port)
	}

	return err
}

func (t *Task) getTransferClients(address string) (*replicaClient.ReplicaClient, *replicaClient.ReplicaClient, error) {
	from, err := t.getFromReplica()
	if err != nil {
		return nil, nil, err
	}
	logrus.Infof("Using replica %s as the source for rebuild ", from.Address)

	fromClient, err := replicaClient.NewReplicaClient(from.Address)
	if err != nil {
		return nil, nil, err
	}

	to, err := t.getToReplica(address)
	if err != nil {
		return nil, nil, err
	}
	logrus.Infof("Using replica %s as the target for rebuild ", to.Address)

	toClient, err := replicaClient.NewReplicaClient(to.Address)
	if err != nil {
		return nil, nil, err
	}

	return fromClient, toClient, nil
}

func (t *Task) getFromReplica() (rest.Replica, error) {
	replicas, err := t.client.ListReplicas()
	if err != nil {
		return rest.Replica{}, err
	}

	for _, r := range replicas {
		if r.Mode == "RW" {
			return r, nil
		}
	}

	return rest.Replica{}, fmt.Errorf("Failed to find good replica to copy from")
}

func (t *Task) getToReplica(address string) (rest.Replica, error) {
	replicas, err := t.client.ListReplicas()
	if err != nil {
		return rest.Replica{}, err
	}

	for _, r := range replicas {
		if r.Address == address {
			if r.Mode != "WO" {
				return rest.Replica{}, fmt.Errorf("Replica %s is not in mode WO got: %s", address, r.Mode)
			}
			return r, nil
		}
	}

	return rest.Replica{}, fmt.Errorf("Failed to find target replica to copy to")
}

func getNonBackingDisks(address string) (map[string]types.DiskInfo, error) {
	repClient, err := replicaClient.NewReplicaClient(address)
	if err != nil {
		return nil, err
	}

	r, err := repClient.GetReplica()
	if err != nil {
		return nil, err
	}

	disks := make(map[string]types.DiskInfo)
	for name, disk := range r.Disks {
		if name == r.BackingFile {
			continue
		}
		disks[name] = disk
	}

	return disks, err
}

func GetSnapshotsInfo(replicas []rest.Replica) (outputDisks map[string]types.DiskInfo, err error) {
	defer func() {
		err = errors.Wrapf(err, "BUG: cannot get snapshot info")
	}()
	for _, r := range replicas {
		if r.Mode != "RW" {
			continue
		}

		disks, err := getNonBackingDisks(r.Address)
		if err != nil {
			return nil, err
		}

		newDisks := make(map[string]types.DiskInfo)
		for name, disk := range disks {
			snapshot := ""

			if !replica.IsHeadDisk(name) {
				snapshot, err = replica.GetSnapshotNameFromDiskName(name)
				if err != nil {
					return nil, err
				}
			} else {
				snapshot = VolumeHeadName
			}
			children := map[string]bool{}
			for childDisk := range disk.Children {
				child := ""
				if !replica.IsHeadDisk(childDisk) {
					child, err = replica.GetSnapshotNameFromDiskName(childDisk)
					if err != nil {
						return nil, err
					}
				} else {
					child = VolumeHeadName
				}
				children[child] = true
			}
			parent := ""
			if disk.Parent != "" {
				parent, err = replica.GetSnapshotNameFromDiskName(disk.Parent)
				if err != nil {
					return nil, err
				}
			}
			info := types.DiskInfo{
				Name:        snapshot,
				Parent:      parent,
				Removed:     disk.Removed,
				UserCreated: disk.UserCreated,
				Children:    children,
				Created:     disk.Created,
				Size:        disk.Size,
				Labels:      disk.Labels,
			}
			newDisks[snapshot] = info
		}
		// we treat the healthy replica with the most snapshots as the
		// source of the truth, since that means something are still in
		// progress and haven't completed yet.
		if len(newDisks) > len(outputDisks) {
			outputDisks = newDisks
		}
	}
	return outputDisks, nil
}
