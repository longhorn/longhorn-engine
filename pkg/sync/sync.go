package sync

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/controller/client"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

type Task struct {
	client *client.ControllerClient
}

type TaskError struct {
	ReplicaErrors []ReplicaError
}

type ReplicaError struct {
	Address string
	Message string
}

type SnapshotPurgeStatus struct {
	Error     string `json:"error"`
	IsPurging bool   `json:"isPurging"`
	Progress  int    `json:"progress"`
	State     string `json:"state"`
}

type ReplicaRebuildStatus struct {
	Error              string `json:"error"`
	IsRebuilding       bool   `json:"isRebuilding"`
	Progress           int    `json:"progress"`
	State              string `json:"state"`
	FromReplicaAddress string `json:"fromReplicaAddress"`
}

type SnapshotCloneStatus struct {
	IsCloning          bool   `json:"isCloning"`
	Error              string `json:"error"`
	Progress           int    `json:"progress"`
	State              string `json:"state"`
	FromReplicaAddress string `json:"fromReplicaAddress"`
	SnapshotName       string `json:"snapshotName"`
}

type SnapshotHashStatus struct {
	State             string `json:"state"`
	Checksum          string `json:"checksum"`
	Error             string `json:"error"`
	SilentlyCorrupted bool   `json:"silentlyCorrupted"`
}

func NewTaskError(res ...ReplicaError) *TaskError {
	return &TaskError{
		ReplicaErrors: append([]ReplicaError{}, res...),
	}
}

func (e *TaskError) Error() string {
	var errs []string
	for _, re := range e.ReplicaErrors {
		errs = append(errs, re.Error())
	}

	if errs == nil {
		return "Unknown"
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return strings.Join(errs, "; ")
}

func (e *TaskError) Append(re ReplicaError) {
	e.ReplicaErrors = append(e.ReplicaErrors, re)
}

func (e *TaskError) HasError() bool {
	return len(e.ReplicaErrors) != 0
}

func NewReplicaError(address string, err error) ReplicaError {
	return ReplicaError{
		Address: address,
		Message: err.Error(),
	}
}

func (e ReplicaError) Error() string {
	return fmt.Sprintf("%v: %v", e.Address, e.Message)
}

// NewTask creates new task with an initialized ControllerClient
// The lifetime of the Task::client is bound to the context lifetime
// client calls have their own contexts with timeouts for the call
func NewTask(ctx context.Context, controllerAddress, volumeName, controllerInstanceName string) (*Task, error) {
	controllerClient, err := client.NewControllerClient(controllerAddress, volumeName, controllerInstanceName)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = controllerClient.Close()
	}()

	return &Task{
		client: controllerClient,
	}, nil
}

func (t *Task) DeleteSnapshot(snapshot string) error {
	var err error

	replicas, err := t.client.ReplicaList()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if ok, err := t.isRebuilding(r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("cannot remove a snapshot because %s is rebuilding", r.Address)
		}
	}

	for _, replica := range replicas {
		if err = t.markSnapshotAsRemoved(replica, snapshot); err != nil {
			return err
		}

		if err = t.cancelSnapshotHashJob(replica, snapshot); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) PurgeSnapshots(skip bool) error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return errors.Wrap(err, "failed to list replicas before purging")
	}

	taskErr := NewTaskError()
	for _, r := range replicas {
		if ok, err := t.isRebuilding(r); err != nil {
			taskErr.Append(NewReplicaError(r.Address, errors.Wrapf(err, "failed to check if replica %v is rebuilding before purging", r.Address)))
		} else if ok {
			taskErr.Append(NewReplicaError(r.Address, fmt.Errorf("cannot purge snapshots because %s is rebuilding", r.Address)))
		}

		if ok, err := t.isPurging(r); err != nil {
			taskErr.Append(NewReplicaError(r.Address, errors.Wrapf(err, "failed to check if replica %v is purging before purging", r.Address)))
		} else if ok {
			if skip {
				return nil
			}
			taskErr.Append(NewReplicaError(r.Address, fmt.Errorf("cannot purge snapshots because %s is already purging snapshots", r.Address)))
		}
	}

	if taskErr.HasError() {
		return taskErr
	}

	errorMap := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(len(replicas))

	for _, r := range replicas {
		go func(rep *types.ControllerReplicaInfo) {
			defer wg.Done()

			// We don't know the replica's instanceName, so create a client without it.
			repClient, err := replicaClient.NewReplicaClient(rep.Address, t.client.VolumeName, "")
			if err != nil {
				errorMap.Store(rep.Address, errors.Wrapf(err, "failed to get replica client %v before purging", rep.Address))
				return
			}
			defer repClient.Close()

			if err := repClient.SnapshotPurge(); err != nil {
				errorMap.Store(rep.Address, errors.Wrapf(err, "replica %v failed to execute snapshot purge", rep.Address))
				return
			}
		}(r)
	}

	wg.Wait()

	for _, r := range replicas {
		if v, ok := errorMap.Load(r.Address); ok {
			err = v.(error)
			if skip && types.IsAlreadyPurgingError(err) {
				continue
			}
			taskErr.Append(NewReplicaError(r.Address, err))
		}
	}

	if taskErr.HasError() {
		return taskErr
	}
	return nil
}

func (t *Task) PurgeSnapshotStatus() (map[string]*SnapshotPurgeStatus, error) {
	replicaStatusMap := make(map[string]*SnapshotPurgeStatus)

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

	for _, r := range replicas {
		if r.Mode == types.ERR {
			continue
		}

		// We don't know the replica's instanceName, so create a client without it.
		repClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, "")
		if err != nil {
			return nil, err
		}
		clients = append(clients, repClient)

		status, err := repClient.SnapshotPurgeStatus()
		if err != nil {
			replicaStatusMap[r.Address] = &SnapshotPurgeStatus{
				Error: fmt.Sprintf("failed to get snapshot purge status of %v: %v", r.Address, err),
			}
			continue
		}
		replicaStatusMap[r.Address] = &SnapshotPurgeStatus{
			Error:     status.Error,
			IsPurging: status.IsPurging,
			Progress:  int(status.Progress),
			State:     status.State,
		}
	}

	return replicaStatusMap, nil
}

func (t *Task) isRebuilding(replicaInController *types.ControllerReplicaInfo) (bool, error) {
	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return false, err
	}
	defer repClient.Close()

	replica, err := repClient.GetReplica()
	if err != nil {
		return false, err
	}

	return replica.Rebuilding, nil
}

func (t *Task) isHashingSnapshot(replicaInController *types.ControllerReplicaInfo) (bool, error) {
	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return false, err
	}
	defer repClient.Close()

	isLocked, err := repClient.SnapshotHashLockState()
	if err != nil {
		return false, err
	}

	return isLocked, nil
}

func (t *Task) isPurging(replicaInController *types.ControllerReplicaInfo) (bool, error) {
	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return false, err
	}
	defer repClient.Close()

	status, err := repClient.SnapshotPurgeStatus()
	if err != nil {
		return false, err
	}

	return status.IsPurging, nil
}

func (t *Task) markSnapshotAsRemoved(replicaInController *types.ControllerReplicaInfo, snapshot string) error {
	if replicaInController.Mode != types.RW {
		return fmt.Errorf("can only mark snapshot as removed from replica in mode RW, got %s", replicaInController.Mode)
	}

	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return err
	}
	defer repClient.Close()

	if err := repClient.MarkDiskAsRemoved(snapshot); err != nil {
		return err
	}

	return nil
}

func (t *Task) cancelSnapshotHashJob(replicaInController *types.ControllerReplicaInfo, snapshot string) error {
	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address, t.client.VolumeName, "")
	if err != nil {
		return err
	}
	defer repClient.Close()

	if err := repClient.SnapshotHashCancel(snapshot); err != nil {
		return err
	}

	return nil
}

func (t *Task) AddRestoreReplica(volumeSize, volumeCurrentSize int64, address, instanceName string) error {
	volume, err := t.client.VolumeGet()
	if err != nil {
		return err
	}

	if volume.ReplicaCount == 0 {
		return t.client.VolumeStart(volumeSize, volumeCurrentSize, address)
	}

	if err := t.checkRestoreReplicaSize(address, instanceName, volume.Size); err != nil {
		return err
	}

	logrus.Infof("Adding restore replica %s in WO mode", address)

	// The replica mode will become RW after the first restoration complete.
	// And the rebuilding flag in the replica server won't be set since this is not normal rebuilding.
	if _, err = t.client.ReplicaCreate(address, false, types.WO); err != nil {
		return err
	}

	return nil
}

func (t *Task) checkRestoreReplicaSize(address, instanceName string, volumeSize int64) (err error) {
	var (
		replicaCli  *replicaClient.ReplicaClient
		replicaInfo *types.ReplicaInfo
	)
	replicaCli, err = replicaClient.NewReplicaClient(address, t.client.VolumeName, instanceName)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := replicaCli.CloseReplica(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("original error: %w, close error: %v", err, closeErr)
			}
		}
	}()

	replicaInfo, err = replicaCli.GetReplica()
	if err != nil {
		return
	}
	replicaSize, err := strconv.ParseInt(replicaInfo.Size, 10, 64)
	if err != nil {
		return
	}
	if replicaSize != volumeSize {
		err = fmt.Errorf("rebuilding replica size %v is not the same as volume size %v", replicaSize, volumeSize)
	}
	return
}

func (t *Task) VerifyRebuildReplica(address, instanceName string) error {
	if err := t.client.ReplicaVerifyRebuild(address, instanceName); err != nil {
		return err
	}
	return nil
}

func (t *Task) AddReplica(volumeSize, volumeCurrentSize int64, address, instanceName string, fileSyncHTTPClientTimeout int, fastSync bool) error {
	volume, err := t.client.VolumeGet()
	if err != nil {
		return err
	}

	if volume.ReplicaCount == 0 {
		return t.client.VolumeStart(volumeSize, volumeCurrentSize, address)
	}

	if err := t.checkAndExpandReplica(address, instanceName, volume.Size); err != nil {
		return err
	}

	if err := t.checkAndResetFailedRebuild(address, instanceName); err != nil {
		return err
	}

	logrus.Infof("Adding replica %s in WO mode", address)
	_, err = t.client.ReplicaCreate(address, true, types.WO)
	if err != nil {
		return err
	}

	fromClient, toClient, fromAddress, _, err := t.getTransferClients(address, instanceName)
	if err != nil {
		return err
	}

	defer func() {
		_ = fromClient.Close()
		_ = toClient.Close()
	}()

	if err := toClient.SetRebuilding(true); err != nil {
		return err
	}

	resp, err := t.client.ReplicaPrepareRebuild(address, instanceName)
	if err != nil {
		return err
	}

	if checkIfVolumeHeadExists(resp) {
		return fmt.Errorf("sync file list shouldn't contain volume head")
	}

	if err = toClient.SyncFiles(fromAddress, resp, fileSyncHTTPClientTimeout, fastSync); err != nil {
		return err
	}

	if err := t.reloadAndVerify(address, instanceName, toClient); err != nil {
		return err
	}

	return nil
}

func (t *Task) checkAndResetFailedRebuild(address, instanceName string) error {
	client, err := replicaClient.NewReplicaClient(address, t.client.VolumeName, instanceName)
	if err != nil {
		return err
	}
	defer client.Close()

	replica, err := client.GetReplica()
	if err != nil {
		return err
	}

	if replica.State == string(types.ReplicaStateClosed) && replica.Rebuilding {
		if err := client.OpenReplica(); err != nil {
			return err
		}

		if err := client.SetRebuilding(false); err != nil {
			return err
		}

		return client.CloseReplica()
	}

	return nil
}

func (t *Task) checkAndExpandReplica(address, instanceName string, size int64) error {
	client, err := replicaClient.NewReplicaClient(address, t.client.VolumeName, instanceName)
	if err != nil {
		return err
	}
	defer client.Close()

	replica, err := client.GetReplica()
	if err != nil {
		return err
	}

	replicaSize, err := strconv.ParseInt(replica.Size, 10, 64)
	if err != nil {
		return err
	}
	if replicaSize > size {
		return fmt.Errorf("cannot add new replica larger than size %v", size)
	} else if replicaSize < size {
		logrus.Infof("Prepare to expand new replica to size %v", size)
		needClose := false
		if replica.State == "closed" {
			if err := client.OpenReplica(); err != nil {
				return err
			}
			needClose = true
		}
		if _, err := client.ExpandReplica(size); err != nil {
			return err
		}
		if needClose {
			return client.CloseReplica()
		}
	}

	return nil
}

func (t *Task) reloadAndVerify(address, instanceName string, repClient *replicaClient.ReplicaClient) error {
	_, err := repClient.ReloadReplica()
	if err != nil {
		return err
	}

	if err := t.client.ReplicaVerifyRebuild(address, instanceName); err != nil {
		return err
	}

	if err := repClient.SetRebuilding(false); err != nil {
		return err
	}
	return nil
}

func checkIfVolumeHeadExists(infoList []types.SyncFileInfo) bool {
	// volume head has been synced by PrepareRebuild()
	for _, info := range infoList {
		if strings.Contains(info.FromFileName, types.VolumeHeadName) {
			return true
		}
	}
	return false
}

func (t *Task) getTransferClients(address, instanceName string) (*replicaClient.ReplicaClient,
	*replicaClient.ReplicaClient, string, string, error) {
	var err error
	var fromClient, toClient *replicaClient.ReplicaClient
	var fromAddress, toAddress string

	// clean up replica clients on failure
	defer func() {
		if err != nil {
			if fromClient != nil {
				_ = fromClient.Close()
			}
			if toClient != nil {
				_ = toClient.Close()
			}
		}
	}()

	if fromClient, fromAddress, err = t.getFromReplicaClientForTransfer(); err != nil {
		return nil, nil, "", "", err
	}
	logrus.Infof("Using replica %s as the source for rebuild", fromAddress)

	if toClient, toAddress, err = t.getToReplicaClientForTransfer(address, instanceName); err != nil {
		return nil, nil, "", "", err
	}
	logrus.Infof("Using replica %s as the target for rebuild", toAddress)

	return fromClient, toClient, fromAddress, toAddress, nil
}

func (t *Task) getFromReplicaClientForTransfer() (*replicaClient.ReplicaClient, string, error) {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return nil, "", err
	}

	for _, r := range replicas {
		if r.Mode != types.RW {
			continue
		}
		// We don't know the replica's instanceName, so create a client without it.
		fromClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, "")
		if err != nil {
			logrus.WithError(err).Warnf("Failed to get the client for replica %v when picking up a transfer-from replica", r.Address)
			continue
		}
		fromReplicaPurgeStatus, err := fromClient.SnapshotPurgeStatus()
		if err != nil {
			logrus.WithError(err).Warnf("Failed to check the purge status for replica %v when picking up a transfer-from replica", r.Address)
			continue
		}
		if fromReplicaPurgeStatus.IsPurging {
			logrus.Warnf("Replica %v is purging snapshots, cannot be used as a transfer-from replica", r.Address)
			continue
		}
		return fromClient, r.Address, nil
	}

	return nil, "", fmt.Errorf("failed to find good replica to copy from")
}

func (t *Task) getToReplicaClientForTransfer(address, instanceName string) (*replicaClient.ReplicaClient, string, error) {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return nil, "", err
	}

	for _, r := range replicas {
		if r.Address != address {
			continue
		}
		if r.Mode != types.WO {
			return nil, "", fmt.Errorf("replica %s is not in mode WO: %s", address, r.Mode)
		}
		toClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, instanceName)
		if err != nil {
			return nil, "", err
		}
		return toClient, r.Address, nil
	}

	return nil, "", fmt.Errorf("failed to find target replica to copy to")
}

func getNonBackingDisks(address string, volumeName string) (map[string]types.DiskInfo, error) {
	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(address, volumeName, "")
	if err != nil {
		return nil, err
	}
	defer repClient.Close()

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

func GetSnapshotsInfo(replicas []*types.ControllerReplicaInfo, volumeName string) (outputDisks map[string]types.DiskInfo, err error) {
	defer func() {
		err = errors.Wrapf(err, "BUG: cannot get snapshot info")
	}()
	for _, r := range replicas {
		if r.Mode != types.RW {
			continue
		}

		disks, err := getNonBackingDisks(r.Address, volumeName)
		if err != nil {
			return nil, err
		}

		newDisks := make(map[string]types.DiskInfo)
		for name, disk := range disks {
			snapshot := ""

			if !diskutil.IsHeadDisk(name) {
				snapshot, err = diskutil.GetSnapshotNameFromDiskName(name)
				if err != nil {
					return nil, err
				}
			} else {
				snapshot = types.VolumeHeadName
			}
			children := map[string]bool{}
			for childDisk := range disk.Children {
				child := ""
				if !diskutil.IsHeadDisk(childDisk) {
					child, err = diskutil.GetSnapshotNameFromDiskName(childDisk)
					if err != nil {
						return nil, err
					}
				} else {
					child = types.VolumeHeadName
				}
				children[child] = true
			}
			parent := ""
			if disk.Parent != "" {
				parent, err = diskutil.GetSnapshotNameFromDiskName(disk.Parent)
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

func (t *Task) StartWithReplicas(volumeSize, volumeCurrentSize int64, replicas []string) error {
	volume, err := t.client.VolumeGet()
	if err != nil {
		return err
	}

	if volume.ReplicaCount != 0 {
		return fmt.Errorf("cannot add multiple replicas if volume is already up")
	}

	return t.client.VolumeStart(volumeSize, volumeCurrentSize, replicas...)
}

func (t *Task) RebuildStatus() (map[string]*ReplicaRebuildStatus, error) {
	replicaStatusMap := make(map[string]*ReplicaRebuildStatus)

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

	for _, r := range replicas {
		if r.Mode != types.WO {
			continue
		}

		// We don't know the replica's instanceName, so create a client without it.
		repClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, "")
		if err != nil {
			return nil, err
		}
		clients = append(clients, repClient)

		restoreStatus, err := repClient.RestoreStatus()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to check the restore status before fetching the rebuild status")
		}
		if restoreStatus.DestFileName != "" {
			logrus.Debug("Skip checking rebuild status since the volume is a restore/DR volume")
			return replicaStatusMap, nil
		}

		status, err := repClient.ReplicaRebuildStatus()
		if err != nil {
			replicaStatusMap[r.Address] = &ReplicaRebuildStatus{
				Error: fmt.Sprintf("failed to get replica rebuild status of %v: %v", r.Address, err),
			}
			continue
		}
		replicaStatusMap[r.Address] = &ReplicaRebuildStatus{
			Error:              status.Error,
			IsRebuilding:       status.IsRebuilding,
			Progress:           int(status.Progress),
			State:              status.State,
			FromReplicaAddress: status.FromReplicaAddress,
		}
	}

	return replicaStatusMap, nil
}

func CloneSnapshot(engineControllerClient, fromControllerClient *client.ControllerClient, volumeName, fromVolumeName,
	snapshotFileName string, exportBackingImageIfExist bool, fileSyncHTTPClientTimeout int) error {
	replicas, err := fromControllerClient.ReplicaList()
	if err != nil {
		return err
	}
	var sourceReplica *types.ControllerReplicaInfo
	for _, r := range replicas {
		if r.Mode == types.RW {
			sourceReplica = r
			break
		}
	}
	if sourceReplica == nil {
		return fmt.Errorf("cannot find a RW replica in the source volume for cloning")
	}

	replicas, err = engineControllerClient.ReplicaList()
	if err != nil {
		return err
	}
	for _, r := range replicas {
		if r.Mode != types.RW {
			return fmt.Errorf("cannot do snapshot clone because replica %v in %v mode. "+
				"All replicas in the target volume must be in RW mode", r.Address, r.Mode)
		}
	}

	taskErr := NewTaskError()
	syncErrorMap := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(len(replicas))

	for _, r := range replicas {
		go func(r *types.ControllerReplicaInfo) {
			defer wg.Done()
			// We don't know the replica's instanceName, so create a client without it.
			repClient, err := replicaClient.NewReplicaClient(r.Address, volumeName, "")
			if err != nil {
				syncErrorMap.Store(r.Address, err)
				return
			}
			defer repClient.Close()
			if err := repClient.CloneSnapshot(sourceReplica.Address, fromVolumeName, snapshotFileName,
				exportBackingImageIfExist, fileSyncHTTPClientTimeout); err != nil {
				syncErrorMap.Store(r.Address, err)
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
	if taskErr.HasError() {
		return taskErr
	}

	return nil
}

func CloneStatus(engineControllerClient *client.ControllerClient, volumeName string) (map[string]*SnapshotCloneStatus,
	error) {
	cloneStatusMap := make(map[string]*SnapshotCloneStatus)

	replicas, err := engineControllerClient.ReplicaList()
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

	for _, r := range replicas {
		// We don't know the replica's instanceName, so create a client without it.
		repClient, err := replicaClient.NewReplicaClient(r.Address, volumeName, "")
		if err != nil {
			return nil, err
		}
		clients = append(clients, repClient)
		status, err := repClient.SnapshotCloneStatus()
		if err != nil {
			cloneStatusMap[r.Address] = &SnapshotCloneStatus{
				Error: fmt.Sprintf("failed to get snapshot clone status of %v: %v", r.Address, err),
			}
			continue
		}
		cloneStatusMap[r.Address] = &SnapshotCloneStatus{
			IsCloning:          status.IsCloning,
			Error:              status.Error,
			Progress:           int(status.Progress),
			State:              status.State,
			FromReplicaAddress: status.FromReplicaAddress,
			SnapshotName:       status.SnapshotName,
		}
	}
	return cloneStatusMap, nil
}

func (t *Task) HashSnapshot(snapshotName string, rehash bool) error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if r.Mode != types.RW {
			return fmt.Errorf(types.CannotRequestHashingSnapshotPrefix+" because %v is in %v mode", r.Address, r.Mode)
		}

		if ok, err := t.isRebuilding(r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf(types.CannotRequestHashingSnapshotPrefix+" because %s is rebuilding", r.Address)
		}

		// Do the best to reduce the queued tasks in sync-agent
		if ok, err := t.isHashingSnapshot(r); err != nil {
			return err
		} else if ok {
			return fmt.Errorf(types.CannotRequestHashingSnapshotPrefix+" because %s is hashing snapshot", r.Address)
		}
	}

	taskErr := NewTaskError()
	syncErrorMap := sync.Map{}

	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for _, r := range replicas {
		go func(r *types.ControllerReplicaInfo) {
			defer wg.Done()
			// We don't know the replica's instanceName, so create a client without it.
			repClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, "")
			if err != nil {
				syncErrorMap.Store(r.Address, err)
				return
			}
			defer repClient.Close()

			if err := repClient.SnapshotHash(snapshotName, rehash); err != nil {
				syncErrorMap.Store(r.Address, err)
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
	if taskErr.HasError() {
		return taskErr
	}

	return nil
}

func (t *Task) HashSnapshotStatus(snapshotName string) (map[string]*SnapshotHashStatus, error) {
	hashStatusMap := make(map[string]*SnapshotHashStatus)
	var lock sync.Mutex

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

	wg := sync.WaitGroup{}

	for _, r := range replicas {
		wg.Add(1)

		go func(r *types.ControllerReplicaInfo) {
			defer wg.Done()

			var err error
			var status *ptypes.SnapshotHashStatusResponse

			defer func() {
				lock.Lock()
				defer lock.Unlock()

				if err == nil && status != nil {
					hashStatusMap[r.Address] = &SnapshotHashStatus{
						State:    status.State,
						Checksum: status.Checksum,
						Error:    status.Error,
					}
					return
				}
				hashStatusMap[r.Address] = &SnapshotHashStatus{
					State: string(replica.ProgressStateError),
					Error: err.Error(),
				}
			}()
			// We don't know the replica's instanceName, so create a client without it.
			repClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, "")
			if err != nil {
				err = errors.Wrapf(err, "failed to create replica client to %v", r.Address)
				return
			}

			lock.Lock()
			clients = append(clients, repClient)
			lock.Unlock()

			if r.Mode != types.RW {
				err = fmt.Errorf("replica %v since it is in %v mode", r.Address, r.Mode)
				return
			}

			if ok, err := t.isRebuilding(r); err != nil {
				err = errors.Wrapf(err, "cannot get snapshot hashing status of %v", r.Address) // nolint: ineffassign,staticcheck
				return
			} else if ok {
				err = fmt.Errorf("replica %v is rebuilding", r.Address) // nolint: ineffassign,staticcheck
				return
			}

			status, err = repClient.SnapshotHashStatus(snapshotName)
			if err != nil {
				err = errors.Wrapf(err, "failed to get snapshot hashing status of %v", r.Address)
				return
			}
			if status == nil {
				err = fmt.Errorf("BUG: nil snapshot hashing status from %v", r.Address)
			}
		}(r)
	}

	wg.Wait()

	return hashStatusMap, nil
}

func (t *Task) HashSnapshotCancel(snapshotName string) error {
	replicas, err := t.client.ReplicaList()
	if err != nil {
		return err
	}

	taskErr := NewTaskError()
	syncErrorMap := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(len(replicas))

	for _, r := range replicas {
		go func(r *types.ControllerReplicaInfo) {
			defer wg.Done()
			// We don't know the replica's instanceName, so create a client without it.
			repClient, err := replicaClient.NewReplicaClient(r.Address, t.client.VolumeName, "")
			if err != nil {
				syncErrorMap.Store(r.Address, err)
				return
			}
			defer repClient.Close()
			if err := repClient.SnapshotHashCancel(snapshotName); err != nil {
				syncErrorMap.Store(r.Address, err)
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
	if taskErr.HasError() {
		return taskErr
	}

	return nil
}
