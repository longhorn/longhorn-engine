package controller

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

func GetReplicaDisksAndChain(address, volumeName, instanceName string) (map[string]types.DiskInfo, []string, error) {
	// We may not know the replica instance name. Validation is best effort, so it's fine to pass an empty string.
	repClient, err := client.NewReplicaClient(address, volumeName, instanceName)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "cannot get replica client for %v", address)
	}
	defer func() {
		if errClose := repClient.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close replica client for %v", address)
		}
	}()

	rep, err := repClient.GetReplica()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "cannot get replica for %v", address)
	}

	if len(rep.Chain) == 0 {
		return nil, nil, fmt.Errorf("replica on %v does not have any non-removed disks", address)
	}

	disks := map[string]types.DiskInfo{}
	head := rep.Chain[0]
	for diskName, info := range rep.Disks {
		// skip volume head
		if diskName == head {
			continue
		}
		// skip backing file
		if diskName == rep.BackingFile {
			continue
		}
		disks[diskName] = info
	}
	return disks, rep.Chain, nil
}

// GetReplicaDisksAndHead returns the disks of the replica at address, without the
// volume head and the backing file, and the name of the volume head.
func GetReplicaDisksAndHead(address, volumeName, instanceName string) (map[string]types.DiskInfo, string, error) {
	disks, chain, err := GetReplicaDisksAndChain(address, volumeName, instanceName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get replica disks and/or head for %s: %v", address, err)
	}
	return disks, chain[0], nil
}

// markSnapshotRemovedOnReplica marks the snapshot as removed on the replica at
// address and cancels any running hash job for it. It does not purge the
// snapshot.
func markSnapshotRemovedOnReplica(address, volumeName, instanceName, snapshotName string) error {
	repClient, err := client.NewReplicaClient(address, volumeName, instanceName)
	if err != nil {
		return fmt.Errorf("cannot get replica client for %v: %w", address, err)
	}
	defer func() {
		if errClose := repClient.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close replica client for %v", address)
		}
	}()

	if err := repClient.MarkDiskAsRemoved(snapshotName); err != nil {
		return fmt.Errorf("failed to mark snapshot %v as removed on %v: %w", snapshotName, address, err)
	}
	if err := repClient.SnapshotHashCancel(snapshotName); err != nil {
		return fmt.Errorf("failed to cancel hash of snapshot %v on %v: %w", snapshotName, address, err)
	}
	return nil
}
