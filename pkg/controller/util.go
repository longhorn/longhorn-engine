package controller

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

func GetReplicaDisksAndHead(address, volumeName, instanceName string) (map[string]types.DiskInfo, string, error) {
	// We may not know the replica instance name. Validation is best effort, so it's fine to pass an empty string.
	repClient, err := client.NewReplicaClient(address, volumeName, instanceName)
	if err != nil {
		return nil, "", errors.Wrapf(err, "cannot get replica client for %v", address)
	}
	defer func() {
		if errClose := repClient.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close replica client for %v", address)
		}
	}()

	rep, err := repClient.GetReplica()
	if err != nil {
		return nil, "", errors.Wrapf(err, "cannot get replica for %v", address)
	}

	if len(rep.Chain) == 0 {
		return nil, "", fmt.Errorf("replica on %v does not have any non-removed disks", address)
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
	return disks, head, nil
}

// FindOldestSnapshot returns the name of the oldest snapshot disk in disks
// that is not marked as removed. Disks with the same creation time are
// ordered by name so the result is deterministic. It returns an empty string
// if there is no such disk found.
func FindOldestSnapshot(disks map[string]types.DiskInfo) (string, error) {
	var oldestSnapshot string
	var oldestCreated time.Time
	for name, disk := range disks {
		if disk.Removed {
			continue
		}
		created, err := time.Parse(time.RFC3339, disk.Created)
		if err != nil {
			return "", fmt.Errorf("cannot parse creation time for snapshot disk %v: %w", name, err)
		}

		cmp := created.Compare(oldestCreated)
		// if the creationTimeStamp of both is same, prefer the name that sorts first alphabetically
		if oldestSnapshot == "" || cmp < 0 || (cmp == 0 && name < oldestSnapshot) {
			oldestCreated = created
			oldestSnapshot = name
		}
	}
	return oldestSnapshot, nil
}
