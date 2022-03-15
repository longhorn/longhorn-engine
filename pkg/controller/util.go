package controller

import (
	"fmt"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

// These const are from "github.com/longhorn/longhorn-engine/pkg/replica/replica"
const (
	metadataSuffix   = ".meta"
	diskPrefix       = "volume-snap-"
	diskSuffix       = ".img"
	diskNameTemplate = diskPrefix + "%s" + diskSuffix
)

func GenerateSnapshotDiskName(snapshotName string) string {
	return fmt.Sprintf(diskNameTemplate, snapshotName)
}

func GenerateSnapshotDiskMetaName(diskName string) string {
	return diskName + metadataSuffix
}

func GetReplicaDisksAndHead(address string) (map[string]types.DiskInfo, string, error) {
	repClient, err := client.NewReplicaClient(address)
	if err != nil {
		return nil, "", fmt.Errorf("cannot get replica client for %v: %v",
			address, err)
	}
	defer repClient.Close()

	rep, err := repClient.GetReplica()
	if err != nil {
		return nil, "", fmt.Errorf("cannot get replica for %v: %v",
			address, err)
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
