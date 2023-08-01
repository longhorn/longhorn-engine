package controller

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

func (c *Controller) Revert(name string) error {
	rw := false
	wo := false
	for _, rep := range c.replicas {
		// BUG: Need to deal with replica in rebuilding(WO) mode.
		if rep.Mode == types.RW {
			rw = true
		} else if rep.Mode == types.WO {
			wo = true
		}
	}
	doable := rw && !wo
	if !doable {
		return fmt.Errorf("not valid state to revert, rebuilding in process or all replica are in ERR state")
	}

	snapshotDiskName := diskutil.GenerateSnapshotDiskName(name)
	found := false
	for _, r := range c.replicas {
		if r.Mode != types.RW {
			continue
		}

		disks, _, err := GetReplicaDisksAndHead(r.Address, c.VolumeName, "")
		if err != nil {
			return err
		}
		for _, disk := range disks {
			if disk.Name != snapshotDiskName {
				continue
			}
			if disk.Removed {
				return fmt.Errorf("cannot revert to removed snapshot %v", name)
			}
			found = true
			break
		}
		if found {
			break
		}
	}
	if !found {
		return fmt.Errorf("cannot do revert since the target snapshot %v is not found", name)
	}

	clients, name, err := c.clientsAndSnapshot(name)
	if err != nil {
		return err
	}
	defer func() {
		for _, repClient := range clients {
			_ = repClient.Close()
		}
	}()

	if c.FrontendState() == "up" {
		return fmt.Errorf("volume frontend enabled, aborting snapshot revert")
	}

	c.Lock()
	defer c.Unlock()

	minimalSuccess := false
	now := util.Now()
	for address, rClient := range clients {
		logrus.Infof("Reverting to snapshot %s on %s at %s", name, address, now)
		if err := rClient.Revert(name, now); err != nil {
			logrus.WithError(err).Errorf("Error on reverting to %s on %s", name, address)
			c.setReplicaModeNoLock(address, types.ERR)
		} else {
			minimalSuccess = true
			logrus.Infof("Reverting to snapshot %s on %s succeeded", name, address)
		}
	}

	if !minimalSuccess {
		return fmt.Errorf("failed to revert to %v on all replicas", name)
	}

	return nil
}

func (c *Controller) clientsAndSnapshot(name string) (map[string]*client.ReplicaClient, string, error) {
	clients := map[string]*client.ReplicaClient{}
	var repClient *client.ReplicaClient
	var err error

	// close all clients in case of error
	defer func() {
		if err != nil {
			for _, repClient := range clients {
				_ = repClient.Close()
			}
		}
	}()

	for _, replica := range c.replicas {
		if replica.Mode == types.WO {
			return nil, "", fmt.Errorf("cannot revert %s during rebuilding process", replica.Address)
		}
		if replica.Mode != types.RW {
			continue
		}

		if !strings.HasPrefix(replica.Address, "tcp://") {
			return nil, "", fmt.Errorf("backend %s does not support revert", replica.Address)
		}

		// We don't know the replica's instanceName, so create a client without it.
		repClient, err = client.NewReplicaClient(replica.Address, c.VolumeName, "")
		if err != nil {
			return nil, "", err
		}
		clients[replica.Address] = repClient

		_, err = repClient.GetReplica()
		if err != nil {
			return nil, "", err
		}
	}
	name = diskutil.GenerateSnapshotDiskName(name)

	return clients, name, nil
}
