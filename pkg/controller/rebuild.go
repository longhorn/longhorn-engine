package controller

import (
	"fmt"
	"net"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
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

func (c *Controller) VerifyRebuildReplica(address, instanceName string) error {
	// Prevent snapshot happens at the same time, as well as prevent
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

	fromDisks, _, err := GetReplicaDisksAndHead(rwReplica.Address, c.Name, "")
	if err != nil {
		return err
	}

	toDisks, _, err := GetReplicaDisksAndHead(address, c.Name, instanceName)
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
			return errors.Wrapf(err, "failed to get revision counter of RW Replica %v: counter %v",
				rwReplica.Address, counter)

		}
		if err := c.backend.SetRevisionCounter(address, counter); err != nil {
			return errors.Wrapf(err, "failed to set revision counter for %v", address)
		}
	}

	logrus.Infof("WO replica %v's chain verified, update mode to RW", address)
	c.setReplicaModeNoLock(address, types.RW)
	return nil
}

func syncFile(from, to, fromAddress, toAddress, volumeName, toInstanceName string, fileSyncHTTPClientTimeout int, fastSync bool) error {
	if to == "" {
		to = from
	}

	fromClient, err := client.NewReplicaClient(fromAddress, volumeName, "")
	if err != nil {
		return errors.Wrapf(err, "cannot get replica client for %v", fromAddress)
	}
	defer fromClient.Close()

	toClient, err := client.NewReplicaClient(toAddress, volumeName, toInstanceName)
	if err != nil {
		return errors.Wrapf(err, "cannot get replica client for %v", toAddress)
	}
	defer toClient.Close()

	host, port, err := toClient.LaunchReceiver(to)
	if err != nil {
		return err
	}

	strHostPort := net.JoinHostPort(host, strconv.Itoa(int(port)))

	logrus.Infof("Synchronizing %s to %s:%s", from, to, strHostPort)
	err = fromClient.SendFile(from, host, port, fileSyncHTTPClientTimeout, fastSync)
	if err != nil {
		logrus.WithError(err).Errorf("failed to synchronize %s to %s:%s", from, to, strHostPort)
	} else {
		logrus.Infof("Done synchronizing %s to %s:%s", from, to, strHostPort)
	}

	return err
}

func (c *Controller) PrepareRebuildReplica(address, instanceName string) ([]types.SyncFileInfo, error) {
	c.Lock()
	defer c.Unlock()

	if !c.revisionCounterDisabled {
		if err := c.backend.SetRevisionCounter(address, 0); err != nil {
			return nil, errors.Wrapf(err, "failed to set revision counter for %v", address)
		}
	}

	replica, rwReplica, err := c.getCurrentAndRWReplica(address)
	if err != nil {
		return nil, err
	}
	if replica.Mode != types.WO {
		return nil, fmt.Errorf("invalid mode %v for replica %v to prepare rebuild", replica.Mode, address)
	}

	fromDisks, fromHead, err := GetReplicaDisksAndHead(rwReplica.Address, c.Name, "")
	if err != nil {
		return nil, err
	}

	toDisks, toHead, err := GetReplicaDisksAndHead(address, c.Name, instanceName)
	if err != nil {
		return nil, err
	}

	syncFileInfoList := []types.SyncFileInfo{}
	extraDisks := toDisks
	for diskName, info := range fromDisks {
		diskMeta := diskutil.GenerateSnapshotDiskMetaName(diskName)
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

	if err := removeExtraDisks(extraDisks, address, c.Name, instanceName); err != nil {
		return nil, err
	}

	// The lock will block the read/write for this head file sync
	if err := syncFile(fromHead+".meta", toHead+".meta", rwReplica.Address, address, c.Name, instanceName,
		c.fileSyncHTTPClientTimeout, false); err != nil {
		return nil, err
	}

	return syncFileInfoList, nil
}

func removeExtraDisks(extraDisks map[string]types.DiskInfo, address, volumeName, instanceName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to remove extra disks %v in replica %v", extraDisks, address)
	}()

	if len(extraDisks) == 0 {
		return nil
	}

	repClient, err := client.NewReplicaClient(address, volumeName, instanceName)
	if err != nil {
		return errors.Wrapf(err, "cannot create replica client for address %v", address)
	}
	defer repClient.Close()

	for disk := range extraDisks {
		if err = repClient.RemoveDisk(disk, true); err != nil {
			return errors.Wrapf(err, "cannot remove disk %v", disk)
		}
	}
	logrus.Warnf("Removed extra disks %v in replica %v", extraDisks, address)

	return nil
}
