package controller

import (
	"fmt"
	"net"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

func (c *Controller) getCurrentAndRWReplica(address string) (*types.Replica, *types.Replica, error) {
	var (
		current, rwReplica *types.Replica
	)

	for i := range c.replicas {
		if c.replicas[i].Address == address {
			current = &c.replicas[i]
		} else if c.replicas[i].Mode == types.RW {
			rwReplica = &c.replicas[i]
		}
	}
	if current == nil {
		return nil, nil, fmt.Errorf("cannot find replica %v", address)
	}
	if rwReplica == nil {
		return nil, nil, fmt.Errorf("cannot find any healthy replica")
	}

	return current, rwReplica, nil
}

func (c *Controller) VerifyRebuildReplica(address string) error {
	// Prevent snapshot happenes at the same time, as well as prevent
	// writing from happening since we're updating revision counter
	c.Lock()
	defer c.Unlock()

	replica, rwReplica, err := c.getCurrentAndRWReplica(address)
	if err != nil {
		return err
	}

	if replica.Mode == types.RW {
		return nil
	}
	if replica.Mode != types.WO {
		return fmt.Errorf("invalid mode %v for replica %v to check", replica.Mode, address)
	}

	fromDisks, _, err := GetReplicaDisksAndHead(rwReplica.Address)
	if err != nil {
		return err
	}

	toDisks, _, err := GetReplicaDisksAndHead(address)
	if err != nil {
		return err
	}

	// The `Children` field of disk info may contain volume head. And the head file index of rebuilt replica
	// is different from that of health replicas. Hence we cannot directly compare the disk info map here.
	if len(fromDisks) != len(toDisks) {
		return fmt.Errorf("replica %v's disk number is not equal to that of RW replica %v: %+v vs %+v",
			address, rwReplica.Address, toDisks, fromDisks)
	}
	for diskName := range fromDisks {
		if _, exist := toDisks[diskName]; !exist {
			return fmt.Errorf("replica %v's chain not equal to RW replica %v's chain: %+v vs %+v",
				address, rwReplica.Address, toDisks, fromDisks)
		}
	}

	if !c.revisionCounterDisabled {
		counter, err := c.backend.GetRevisionCounter(rwReplica.Address)
		if err != nil || counter == -1 {
			return fmt.Errorf("failed to get revision counter of RW Replica %v: counter %v, err %v",
				rwReplica.Address, counter, err)

		}
		if err := c.backend.SetRevisionCounter(address, counter); err != nil {
			return fmt.Errorf("failed to set revision counter for %v: %v", address, err)
		}
	}

	logrus.Infof("WO replica %v's chain verified, update mode to RW", address)
	c.setReplicaModeNoLock(address, types.RW)
	return nil
}

func syncFile(from, to string, fromReplica, toReplica *types.Replica) error {
	if to == "" {
		to = from
	}

	fromClient, err := client.NewReplicaClient(fromReplica.Address)
	if err != nil {
		return fmt.Errorf("cannot get replica client for %v: %v",
			fromReplica.Address, err)
	}
	defer fromClient.Close()

	toClient, err := client.NewReplicaClient(toReplica.Address)
	if err != nil {
		return fmt.Errorf("cannot get replica client for %v: %v",
			toReplica.Address, err)
	}
	defer toClient.Close()

	host, port, err := toClient.LaunchReceiver(to)
	if err != nil {
		return err
	}

	strHostPort := net.JoinHostPort(host, strconv.Itoa(int(port)))

	logrus.Infof("Synchronizing %s to %s@%s", from, to, strHostPort)
	err = fromClient.SendFile(from, host, port)
	if err != nil {
		logrus.Infof("Failed synchronizing %s to %s@%s: %v", from, to, strHostPort, err)
	} else {
		logrus.Infof("Done synchronizing %s to %s@%s", from, to, strHostPort)
	}

	return err
}

func (c *Controller) PrepareRebuildReplica(address string) ([]types.SyncFileInfo, error) {
	c.Lock()
	defer c.Unlock()

	if !c.revisionCounterDisabled {
		if err := c.backend.SetRevisionCounter(address, 0); err != nil {
			return nil, fmt.Errorf("failed to set revision counter for %v: %v", address, err)
		}
	}

	replica, rwReplica, err := c.getCurrentAndRWReplica(address)
	if err != nil {
		return nil, err
	}
	if replica.Mode != types.WO {
		return nil, fmt.Errorf("invalid mode %v for replica %v to prepare rebuild", replica.Mode, address)
	}

	fromDisks, fromHead, err := GetReplicaDisksAndHead(rwReplica.Address)
	if err != nil {
		return nil, err
	}

	toDisks, toHead, err := GetReplicaDisksAndHead(address)
	if err != nil {
		return nil, err
	}

	syncFileInfoList := []types.SyncFileInfo{}
	extraDisks := toDisks
	for diskName, info := range fromDisks {
		diskMeta := GenerateSnapshotDiskMetaName(diskName)
		diskSize, err := strconv.ParseInt(info.Size, 10, 64)
		if err != nil {
			return nil, err
		}
		syncFileInfoList = append(syncFileInfoList, types.SyncFileInfo{
			FromFileName: diskName,
			ToFileName:   diskName,
			ActualSize:   diskSize,
		})
		// Consider the meta file size as 0
		syncFileInfoList = append(syncFileInfoList, types.SyncFileInfo{
			FromFileName: diskMeta,
			ToFileName:   diskMeta,
			ActualSize:   0,
		})
		delete(extraDisks, diskName)
	}

	if err := removeExtraDisks(extraDisks, address); err != nil {
		return nil, err
	}

	// The lock will block the read/write for this head file sync
	if err := syncFile(fromHead+".meta", toHead+".meta", rwReplica, replica); err != nil {
		return nil, err
	}

	return syncFileInfoList, nil
}

func removeExtraDisks(extraDisks map[string]types.DiskInfo, address string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to remove extra disks %v in replica %v", extraDisks, address)
	}()

	if len(extraDisks) == 0 {
		return nil
	}

	repClient, err := client.NewReplicaClient(address)
	if err != nil {
		return errors.Wrapf(err, "cannot replica client")
	}
	defer repClient.Close()

	for disk := range extraDisks {
		if err = repClient.RemoveDisk(disk, true); err != nil {
			return errors.Wrapf(err, "cannot remove disk")
		}
	}
	logrus.Warnf("Removed extra disks %v in replica %v", extraDisks, address)

	return nil
}
