package sync

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	lhutils "github.com/longhorn/go-common-libs/utils"

	"github.com/longhorn/longhorn-engine/pkg/types"

	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type BackupCreateInfo struct {
	BackupID       string `json:"backupID"`
	IsIncremental  bool   `json:"isIncremental"`
	ReplicaAddress string `json:"replicaAddress"`
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

func (t *Task) CreateBackup(backupName, snapshot, dest, backingImageName, backingImageChecksum string,
	compressionMethod string, concurrentLimit int, storageClassName string, labels []string, credential, parameters map[string]string) (*BackupCreateInfo, error) {
	if snapshot == types.VolumeHeadName {
		return nil, fmt.Errorf("cannot backup the head disk in the chain")
	}

	volume, err := t.client.VolumeGet()
	if err != nil {
		return nil, err
	}

	replica, err := t.findRWReplica()
	if err != nil {
		return nil, err
	}

	return t.createBackup(replica, backupName, snapshot, dest, volume.Name, backingImageName, backingImageChecksum,
		compressionMethod, concurrentLimit, storageClassName, labels, credential, parameters)
}

func (t *Task) findRWReplica() (*types.ControllerReplicaInfo, error) {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list replicas for backup")
	}

	for _, r := range replicas {
		if r.Mode == types.RW {
			return r, nil
		}
	}

	return nil, fmt.Errorf("cannot find an available replica for backup")
}

func (t *Task) createBackup(replicaInController *types.ControllerReplicaInfo, backupName, snapshot, dest, volumeName,
	backingImageName, backingImageChecksum, compression string, concurrentLimit int, storageClassName string,
	labels []string, credential, parameters map[string]string) (*BackupCreateInfo, error) {
	if replicaInController.Mode != types.RW {
		return nil, fmt.Errorf("can only create backup from replica in mode RW, got %s", replicaInController.Mode)
	}

	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return nil, err
	}
	defer repClient.Close()

	rep, err := repClient.GetReplica()
	if err != nil {
		return nil, err
	}

	diskName := diskutil.GenerateSnapshotDiskName(snapshot)
	if _, ok := rep.Disks[diskName]; !ok {
		return nil, fmt.Errorf("snapshot disk %s not found on replica %s", diskName, replicaInController.Address)
	}

	logrus.Infof("Backing up %s on %s, to %s", snapshot, replicaInController.Address, dest)

	reply, err := repClient.CreateBackup(backupName, snapshot, dest, volumeName, backingImageName, backingImageChecksum,
		compression, concurrentLimit, storageClassName, labels, credential, parameters)
	if err != nil {
		return nil, err
	}
	return &BackupCreateInfo{
		BackupID:       reply.Backup,
		IsIncremental:  reply.IsIncremental,
		ReplicaAddress: replicaInController.Address,
	}, nil
}

func FetchBackupStatus(client *replicaClient.ReplicaClient, backupID string, replicaAddr string) (*BackupStatusInfo, error) {
	bs, err := client.BackupStatus(backupID)
	if err != nil {
		return &BackupStatusInfo{
			Error: fmt.Sprintf("Failed to get backup status on %s for %v: %v", replicaAddr, backupID, err),
		}, nil
	}

	return &BackupStatusInfo{
		Progress:       int(bs.Progress),
		BackupURL:      bs.BackupUrl,
		Error:          bs.Error,
		SnapshotName:   bs.SnapshotName,
		State:          bs.State,
		ReplicaAddress: replicaAddr,
	}, nil
}

func (t *Task) RestoreBackup(backup string, credential map[string]string, concurrentLimit int) error {
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
			taskErr.Append(NewReplicaError(r.Address, fmt.Errorf("cannot do restore for normal rebuilding replica")))
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

	snapshots, err := GetSnapshotsInfo(replicas, t.client.VolumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get snapshot info before the incremental restore")
	}

	switch len(snapshots) {
	case 1: // the volume head.
		if isIncremental {
			return fmt.Errorf("BUG: replicas %+v of DR volume should contains at least 2 disk files: the volume head and a snapshot storing restore data", replicas)
		}
		if snapshotDiskName == "" {
			snapshotDiskName = diskutil.GenerateSnapshotDiskName(lhutils.UUID())
		}
	case 2: // the volume head and the only system snapshot.
		for _, s := range snapshots {
			if s.Name == types.VolumeHeadName {
				snapshotDiskName = diskutil.GenerateSnapshotDiskName(s.Parent)
				break
			}
		}
	case 0:
		return fmt.Errorf("BUG: replicas %+v do not contain any snapshots and volume head", replicas)
	default: // more than 1 snapshots beside the volume head
		// Need to do snapshot purge before restore if there are more than 1 (system) snapshot in one of the replicas.
		// Otherwise, the snapshot chains may be different among all replicas after expansion or rebuild.
		if err := t.PurgeSnapshots(true); err != nil {
			return err
		}
		return fmt.Errorf("found more than 1 snapshot in the replicas, hence started to purge snapshots before the restore")
	}

	backupInfo, err := backupstore.InspectBackup(backup)
	if err != nil {
		for _, r := range replicas {
			taskErr.Append(NewReplicaError(r.Address, errors.Wrapf(err, "failed to get the current restoring backup info")))
		}
		return taskErr
	}

	if backupInfo.VolumeSize < volume.Size {
		return fmt.Errorf("BUG: The backup volume %v size %v cannot be smaller than the DR volume %v size %v", backupInfo.VolumeName, backupInfo.VolumeSize, volume.Name, volume.Size)
	}
	if backupInfo.VolumeSize > volume.Size {
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
			err := t.restoreBackup(replica, backup, snapshotDiskName, credential, concurrentLimit)
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

func (t *Task) restoreBackup(replicaInController *types.ControllerReplicaInfo, backup string, snapshotFile string,
	credential map[string]string, concurrentLimit int) error {
	if replicaInController.Mode == types.ERR {
		return fmt.Errorf("cannot restore backup from replica in mode ERR")
	}

	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return err
	}
	defer repClient.Close()

	if err := repClient.RestoreBackup(backup, snapshotFile, credential, concurrentLimit); err != nil {
		return err
	}

	return nil
}

func (t *Task) Reset() error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		logrus.WithError(err).Error("Failed to get the replica list")
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(r); err != nil {
			logrus.WithError(err).Error("Cannot check if replica's are rebuilding")
			return err
		} else if ok {
			logrus.WithError(err).Error("Replicas are rebuilding. Can't reset")
			return fmt.Errorf("cannot reset Restore info as replica(%s) is rebuilding", r.Address)
		}
	}

	var clients []*replicaClient.ReplicaClient
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	for _, replica := range replicas {
		// We don't know the replica's instanceName, so create a client without it.
		repClient, err := replicaClient.NewReplicaClient(replica.Address, t.client.VolumeName, "")
		if err != nil {
			logrus.WithError(err).Errorf("Failed to get a replica client for %v", replica.Address)
			return err
		}
		clients = append(clients, repClient)

		logrus.Infof("Performing sync-agent-server-reset for replica %s", replica.Address)
		if err := repClient.Reset(); err != nil {
			logrus.WithError(err).Errorf("Failed Resetting restore status for replica %s", replica.Address)
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

	// clean up clients after processing
	var clients []*replicaClient.ReplicaClient
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	for _, replica := range replicas {
		if replica.Mode == types.ERR {
			continue
		}

		// We don't know the replica's instanceName, so create a client without it.
		repClient, err := replicaClient.NewReplicaClient(replica.Address, t.client.VolumeName, "")
		if err != nil {
			return nil, err
		}
		clients = append(clients, repClient)

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
