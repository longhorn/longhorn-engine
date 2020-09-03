package sync

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

type BackupCreateInfo struct {
	BackupID      string `json:"backupID"`
	IsIncremental bool   `json:"isIncremental"`
}

type BackupStatusInfo struct {
	Progress       int    `json:"progress"`
	BackupURL      string `json:"backupURL,omitempty"`
	Error          string `json:"error,omitempty"`
	SnapshotName   string `json:"snapshotName"`
	State          string `json:"state"`
	ReplicaAddress string `json:"replicaAddress"`
}

type RestoreStatus struct {
	IsRestoring            bool   `json:"isRestoring"`
	Progress               int    `json:"progress,omitempty"`
	Error                  string `json:"error,omitempty"`
	Filename               string `json:"filename,omitempty"`
	State                  string `json:"state"`
	BackupURL              string `json:"backupURL"`
	LastRestored           string `json:"lastRestored"`
	CurrentRestoringBackup string `json:"currentRestoringBackup"`
}

func (t *Task) CreateBackup(snapshot, dest string, labels []string, credential map[string]string) (*BackupCreateInfo, error) {
	var replica *types.ControllerReplicaInfo

	if snapshot == VolumeHeadName {
		return nil, fmt.Errorf("can not backup the head disk in the chain")
	}

	volume, err := t.client.VolumeGet()
	if err != nil {
		return nil, err
	}

	replicas, err := t.client.ReplicaList()
	if err != nil {
		return nil, err
	}

	for _, r := range replicas {
		if r.Mode == types.RW {
			replica = r
			break
		}
	}

	if replica == nil {
		return nil, fmt.Errorf("cannot find a suitable replica for backup")
	}

	backup, err := t.createBackup(replica, snapshot, dest, volume.Name, labels, credential)
	if err != nil {
		return nil, err
	}
	return backup, nil
}

func (t *Task) createBackup(replicaInController *types.ControllerReplicaInfo, snapshot, dest, volumeName string, labels []string,
	credential map[string]string) (*BackupCreateInfo, error) {
	if replicaInController.Mode != types.RW {
		return nil, fmt.Errorf("can only create backup from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return nil, err
	}

	rep, err := repClient.GetReplica()
	if err != nil {
		return nil, err
	}

	diskName := replica.GenerateSnapshotDiskName(snapshot)
	if _, ok := rep.Disks[diskName]; !ok {
		return nil, fmt.Errorf("snapshot disk %s not found on replica %s", diskName, replicaInController.Address)
	}

	logrus.Infof("Backing up %s on %s, to %s", snapshot, replicaInController.Address, dest)

	reply, err := repClient.CreateBackup(snapshot, dest, volumeName, labels, credential)
	if err != nil {
		return nil, err
	}

	info := &BackupCreateInfo{
		BackupID:      reply.Backup,
		IsIncremental: reply.IsIncremental,
	}
	//Store the backupID - Replica IP mapping in controller
	if err := t.client.BackupReplicaMappingCreate(info.BackupID, replicaInController.Address); err != nil {
		return nil, err
	}
	return info, nil
}

func (t *Task) FetchBackupStatus(backupID string, replicaAddr string) (*BackupStatusInfo, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaAddr)
	if err != nil {
		logrus.Errorf("Cannot create a replica client for IP[%v]: %v", replicaAddr, err)
		return nil, err
	}

	bs, err := repClient.BackupStatus(backupID)
	if err != nil {
		return &BackupStatusInfo{
			Error: fmt.Sprintf("Failed to get backup status on %s for %v: %v", replicaAddr, backupID, err),
		}, nil
	}

	info := &BackupStatusInfo{
		Progress:       int(bs.Progress),
		BackupURL:      bs.BackupUrl,
		Error:          bs.Error,
		SnapshotName:   bs.SnapshotName,
		State:          bs.State,
		ReplicaAddress: replicaAddr,
	}

	return info, nil
}

func (t *Task) RestoreBackup(backup string, credential map[string]string) error {
	volume, err := t.client.VolumeGet()
	if err != nil {
		return errors.Wrapf(err, "failed to get volume")
	}
	if volume.FrontendState == "up" {
		return fmt.Errorf("volume frontend enabled, cannot perform restore")
	}

	replicas, err := t.client.ReplicaList()
	if err != nil {
		return errors.Wrapf(err, "failed to list replicas before the restore")
	}

	taskErr := NewTaskError()
	for _, r := range replicas {
		if isRebuilding, err := t.isRebuilding(r); err != nil {
			taskErr.Append(NewReplicaError(r.Address, err))
		} else if isRebuilding {
			taskErr.Append(NewReplicaError(r.Address, fmt.Errorf("can not do restore for normal rebuilding replica")))
		}
	}
	if taskErr.HasError() {
		return taskErr
	}

	purgeStatusMap, err := t.PurgeSnapshotStatus()
	if err != nil {
		return err
	}
	for _, ps := range purgeStatusMap {
		if ps.IsPurging {
			return fmt.Errorf("cannot do restore now since replicas are purging snapshots")
		}
	}

	// For the volumes have not restored backups, there is no snapshot.
	// Hence a random snapshot name will be generated here.
	// Otherwise, the existing snapshot will be used to store the restored data.
	isIncremental := false
	snapshotDiskName := ""

	restoreStatusMap, err := t.RestoreStatus()
	if err != nil {
		return err
	}
	for _, rs := range restoreStatusMap {
		if rs.LastRestored != "" {
			isIncremental = true
		}
		if rs.IsRestoring && rs.Filename != "" {
			snapshotDiskName = rs.Filename
		}
	}

	snapshots, err := GetSnapshotsInfo(replicas)
	if err != nil {
		return errors.Wrapf(err, "failed to get snapshot info before the incremental restore")
	}
	if isIncremental {
		if len(snapshots) < 2 {
			return fmt.Errorf("BUG: replicas %+v of DR volume should contains at least 2 disk files: the volume head and a snapshot storing restore data", replicas)
		}
		// Need to do snapshot purge before incremental restore.
		// Otherwise, the snapshot chains may be different among all replicas after expansion and rebuild.
		if len(snapshots) > 2 {
			if err := t.PurgeSnapshots(true); err != nil {
				return err
			}
			return fmt.Errorf("found more than 1 snapshot in the replicas, hence started to purge snapshots before the restore")
		}
		for _, s := range snapshots {
			if s.Name == VolumeHeadName {
				snapshotDiskName = replica.GenerateSnapshotDiskName(s.Parent)
				break
			}
		}
	} else {
		if snapshotDiskName == "" {
			snapshotDiskName = replica.GenerateSnapshotDiskName(util.UUID())
		}
	}

	backupInfo, err := backupstore.InspectBackup(backup)
	if err != nil {
		return errors.Wrapf(err, "failed to get the current restoring backup info")
	}
	if backupInfo.VolumeSize < volume.Size {
		return fmt.Errorf("BUG: The backup volume %v size %v cannot be smaller than the DR volume %v size %v", backupInfo.VolumeName, backupInfo.VolumeSize, volume.Name, volume.Size)
	} else if backupInfo.VolumeSize > volume.Size {
		if !isIncremental {
			return fmt.Errorf("BUG: The backup volume %v size %v cannot be larger than normal restore volume %v size %v", backupInfo.VolumeName, backupInfo.VolumeSize, volume.Name, volume.Size)
		}
		return fmt.Errorf("need to expand the DR volume %v to size %v before incremental restoration", volume.Name, backupInfo.VolumeSize)
	}

	syncErrorMap := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for _, r := range replicas {
		go func(replica *types.ControllerReplicaInfo) {
			defer wg.Done()
			err := t.restoreBackup(replica, backup, snapshotDiskName, credential)
			if err != nil {
				syncErrorMap.Store(replica.Address, err)
			}
		}(r)
	}
	wg.Wait()

	for _, r := range replicas {
		if v, ok := syncErrorMap.Load(r.Address); ok {
			err = v.(error)
			taskErr.Append(NewReplicaError(r.Address, err))
		}
	}
	if len(taskErr.ReplicaErrors) != 0 {
		return taskErr
	}

	return nil
}

func (t *Task) restoreBackup(replicaInController *types.ControllerReplicaInfo, backup string, snapshotFile string, credential map[string]string) error {
	if replicaInController.Mode == types.ERR {
		return fmt.Errorf("can not restore backup from replica in mode ERR")
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}
	if err := repClient.RestoreBackup(backup, snapshotFile, credential); err != nil {
		return err
	}

	return nil
}

func (t *Task) Reset() error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		logrus.Errorf("Failed to get the replica list: %v", err)
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(r); err != nil {
			logrus.Errorf("can't check if replica's are rebuilding: %v", err)
			return err
		} else if ok {
			logrus.Errorf("Replicas are rebuilding. Can't reset: %v", err)
			return fmt.Errorf("can not reset Restore info as replica(%s) is rebuilding", r.Address)
		}
	}

	for _, replica := range replicas {
		repClient, err := replicaClient.NewReplicaClient(replica.Address)
		if err != nil {
			logrus.Errorf("Failed to get a replica client: %v for %v", err, replica.Address)
			return err
		}

		logrus.Infof("Performing sync-agent-server-reset for replica %s", replica.Address)

		if err := repClient.Reset(); err != nil {
			logrus.Errorf("Failed Resetting restore status for replica %s", replica.Address)
			return err
		}
	}

	return nil
}

func (t *Task) RestoreStatus() (map[string]*RestoreStatus, error) {
	replicaStatusMap := make(map[string]*RestoreStatus)

	replicas, err := t.client.ReplicaList()
	if err != nil {
		return nil, err
	}

	for _, replica := range replicas {
		if replica.Mode == types.ERR {
			continue
		}
		repClient, err := replicaClient.NewReplicaClient(replica.Address)
		if err != nil {
			return nil, err
		}

		rs, err := repClient.RestoreStatus()
		if err != nil {
			replicaStatusMap[replica.Address] = &RestoreStatus{
				Error: fmt.Sprintf("Failed to get restoring status on %s: %v", replica.Address, err),
			}
			continue
		}
		replicaStatusMap[replica.Address] = &RestoreStatus{
			IsRestoring:            rs.IsRestoring,
			Progress:               int(rs.Progress),
			Error:                  rs.Error,
			Filename:               rs.DestFileName,
			State:                  rs.State,
			BackupURL:              rs.BackupUrl,
			LastRestored:           rs.LastRestored,
			CurrentRestoringBackup: rs.CurrentRestoringBackup,
		}
	}

	return replicaStatusMap, nil
}
