package controller

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/engine/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/engine/types"
)

func getReplicaDisksAndHead(address string) (map[string]types.DiskInfo, string, error) {
	repClient, err := client.NewReplicaClient(address)
	if err != nil {
		return nil, "", fmt.Errorf("Cannot get replica client for %v: %v",
			address, err)
	}

	rep, err := repClient.GetReplica()
	if err != nil {
		return nil, "", fmt.Errorf("Cannot get replica for %v: %v",
			address, err)
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

func getDiskMetaFileName(disk string) string {
	return fmt.Sprintf("%s.meta", disk)
}

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
		return nil, nil, fmt.Errorf("Cannot find replica %v", address)
	}
	if rwReplica == nil {
		return nil, nil, fmt.Errorf("Cannot find any healthy replica")
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
		return fmt.Errorf("Invalid mode %v for replica %v to check", replica.Mode, address)
	}

	fromDisks, _, err := getReplicaDisksAndHead(rwReplica.Address)
	if err != nil {
		return err
	}

	toDisks, _, err := getReplicaDisksAndHead(address)
	if err != nil {
		return err
	}

	// The `Children` field of disk info may contain volume head. And the head file index of rebuilt replica
	// is different from that of health replicas. Hence we cannot directly compare the disk info map here.
	if len(fromDisks) != len(toDisks) {
		return fmt.Errorf("Replica %v's disk number is not equal to that of RW replica %v: %+v vs %+v",
			address, rwReplica.Address, toDisks, fromDisks)
	}
	for diskName := range fromDisks {
		if _, exist := toDisks[diskName]; !exist {
			return fmt.Errorf("Replica %v's chain not equal to RW replica %v's chain: %+v vs %+v",
				address, rwReplica.Address, toDisks, fromDisks)
		}
	}

	counter, err := c.backend.GetRevisionCounter(rwReplica.Address)
	if err != nil || counter == -1 {
		return fmt.Errorf("Failed to get revision counter of RW Replica %v: counter %v, err %v",
			rwReplica.Address, counter, err)

	}
	if err := c.backend.SetRevisionCounter(address, counter); err != nil {
		return fmt.Errorf("Fail to set revision counter for %v: %v", address, err)
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
		return fmt.Errorf("Cannot get replica client for %v: %v",
			fromReplica.Address, err)
	}

	toClient, err := client.NewReplicaClient(toReplica.Address)
	if err != nil {
		return fmt.Errorf("Cannot get replica client for %v: %v",
			toReplica.Address, err)
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

func (c *Controller) PrepareRebuildReplica(address string) ([]types.SyncFileInfo, error) {
	c.Lock()
	defer c.Unlock()

	if err := c.backend.SetRevisionCounter(address, 0); err != nil {
		return nil, fmt.Errorf("Fail to set revision counter for %v: %v", address, err)
	}

	replica, rwReplica, err := c.getCurrentAndRWReplica(address)
	if err != nil {
		return nil, err
	}
	if replica.Mode != types.WO {
		return nil, fmt.Errorf("Invalid mode %v for replica %v to prepare rebuild", replica.Mode, address)
	}

	fromDisks, fromHead, err := getReplicaDisksAndHead(rwReplica.Address)
	if err != nil {
		return nil, err
	}

	toDisks, toHead, err := getReplicaDisksAndHead(address)
	if err != nil {
		return nil, err
	}

	syncFileInfoList := []types.SyncFileInfo{}
	extraDisks := toDisks
	for diskName, info := range fromDisks {
		diskMeta := getDiskMetaFileName(diskName)
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

	for disk := range extraDisks {
		if err = repClient.RemoveDisk(disk, true); err != nil {
			return errors.Wrapf(err, "cannot remove disk")
		}
	}
	logrus.Warnf("Removed extra disks %v in replica %v", extraDisks, address)

	return nil
}
