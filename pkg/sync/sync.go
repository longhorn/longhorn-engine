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
)

const (
	VolumeHeadName = "volume-head"
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
func NewTask(ctx context.Context, controllerAddress string) (*Task, error) {
	controllerClient, err := client.NewControllerClient(controllerAddress)
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
			return fmt.Errorf("Can not remove a snapshot because %s is rebuilding", r.Address)
		}
	}

	for _, replica := range replicas {
		if err = t.markSnapshotAsRemoved(replica, snapshot); err != nil {
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

			repClient, err := replicaClient.NewReplicaClient(rep.Address)
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

		repClient, err := replicaClient.NewReplicaClient(r.Address)
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

func getNameAndIndex(chain []string, snapshot string) (string, int) {
	index := find(chain, snapshot)
	if index < 0 {
		snapshot = fmt.Sprintf("volume-snap-%s.img", snapshot)
		index = find(chain, snapshot)
	}

	if index < 0 {
		return "", index
	}

	return snapshot, index
}

func (t *Task) isRebuilding(replicaInController *types.ControllerReplicaInfo) (bool, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
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

func (t *Task) isPurging(replicaInController *types.ControllerReplicaInfo) (bool, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
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

func (t *Task) isDirty(replicaInController *types.ControllerReplicaInfo) (bool, error) {
	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return false, err
	}
	defer repClient.Close()

	replica, err := repClient.GetReplica()
	if err != nil {
		return false, err
	}

	return replica.Dirty, nil
}

func (t *Task) markSnapshotAsRemoved(replicaInController *types.ControllerReplicaInfo, snapshot string) error {
	if replicaInController.Mode != types.RW {
		return fmt.Errorf("Can only mark snapshot as removed from replica in mode RW, got %s", replicaInController.Mode)
	}

	repClient, err := replicaClient.NewReplicaClient(replicaInController.Address)
	if err != nil {
		return err
	}
	defer repClient.Close()

	if err := repClient.MarkDiskAsRemoved(snapshot); err != nil {
		return err
	}

	return nil
}

func find(list []string, item string) int {
	for i, val := range list {
		if val == item {
			return i
		}
	}
	return -1
}

func (t *Task) AddRestoreReplica(volumeSize, volumeCurrentSize int64, replica string) error {
	volume, err := t.client.VolumeGet()
	if err != nil {
		return err
	}

	if volume.ReplicaCount == 0 {
		return t.client.VolumeStart(volumeSize, volumeCurrentSize, replica)
	}

	if err := t.checkRestoreReplicaSize(replica, volume.Size); err != nil {
		return err
	}

	logrus.Infof("Adding restore replica %s in WO mode", replica)

	// The replica mode will become RW after the first restoration complete.
	// And the rebuilding flag in the replica server won't be set since this is not normal rebuilding.
	if _, err = t.client.ReplicaCreate(replica, false, types.WO); err != nil {
		return err
	}

	return nil
}

func (t *Task) checkRestoreReplicaSize(address string, volumeSize int64) error {
	replicaCli, err := replicaClient.NewReplicaClient(address)
	if err != nil {
		return err
	}
	defer replicaCli.CloseReplica()

	replicaInfo, err := replicaCli.GetReplica()
	if err != nil {
		return err
	}
	replicaSize, err := strconv.ParseInt(replicaInfo.Size, 10, 64)
	if err != nil {
		return err
	}
	if replicaSize != volumeSize {
		return fmt.Errorf("rebuilding replica size %v is not the same as volume size %v", replicaSize, volumeSize)
	}

	return nil
}

func (t *Task) VerifyRebuildReplica(address string) error {
	if err := t.client.ReplicaVerifyRebuild(address); err != nil {
		return err
	}
	return nil
}

func (t *Task) AddReplica(volumeSize, volumeCurrentSize int64, replica string) error {
	volume, err := t.client.VolumeGet()
	if err != nil {
		return err
	}

	if volume.ReplicaCount == 0 {
		return t.client.VolumeStart(volumeSize, volumeCurrentSize, replica)
	}

	if err := t.checkAndExpandReplica(replica, volume.Size); err != nil {
		return err
	}

	if err := t.checkAndResetFailedRebuild(replica); err != nil {
		return err
	}

	logrus.Infof("Adding replica %s in WO mode", replica)
	_, err = t.client.ReplicaCreate(replica, true, types.WO)
	if err != nil {
		return err
	}

	fromClient, toClient, fromAddress, _, err := t.getTransferClients(replica)
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

	resp, err := t.client.ReplicaPrepareRebuild(replica)
	if err != nil {
		return err
	}

	if checkIfVolumeHeadExists(resp) {
		return fmt.Errorf("sync file list shouldn't contain volume head")
	}

	if err = toClient.SyncFiles(fromAddress, resp); err != nil {
		return err
	}

	if err := t.reloadAndVerify(replica, toClient); err != nil {
		return err
	}

	return nil
}

func (t *Task) checkAndResetFailedRebuild(address string) error {
	client, err := replicaClient.NewReplicaClient(address)
	if err != nil {
		return err
	}
	defer client.Close()

	replica, err := client.GetReplica()
	if err != nil {
		return err
	}

	if replica.State == "closed" && replica.Rebuilding {
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

func (t *Task) checkAndExpandReplica(address string, size int64) error {
	client, err := replicaClient.NewReplicaClient(address)
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

func (t *Task) reloadAndVerify(address string, repClient *replicaClient.ReplicaClient) error {
	_, err := repClient.ReloadReplica()
	if err != nil {
		return err
	}

	if err := t.client.ReplicaVerifyRebuild(address); err != nil {
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
		if strings.Contains(info.FromFileName, VolumeHeadName) {
			return true
		}
	}
	return false
}

func (t *Task) getTransferClients(address string) (*replicaClient.ReplicaClient, *replicaClient.ReplicaClient, string, string, error) {
	var err error
	var fromClient, toClient *replicaClient.ReplicaClient
	var fromAddress, toAddress string

	// cleanup replica clients on failure
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

	if toClient, toAddress, err = t.getToReplicaClientForTransfer(address); err != nil {
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
		fromClient, err := replicaClient.NewReplicaClient(r.Address)
		if err != nil {
			logrus.Warnf("Failed to get the client for replica %v when picking up a transfer-from replica: %v", r.Address, err)
			continue
		}
		fromReplicaPurgeStatus, err := fromClient.SnapshotPurgeStatus()
		if err != nil {
			logrus.Warnf("Failed to check the purge status for replica %v when picking up a transfer-from replica: %v", r.Address, err)
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

func (t *Task) getToReplicaClientForTransfer(address string) (*replicaClient.ReplicaClient, string, error) {
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
		toClient, err := replicaClient.NewReplicaClient(r.Address)
		if err != nil {
			return nil, "", err
		}
		return toClient, r.Address, nil
	}

	return nil, "", fmt.Errorf("failed to find target replica to copy to")
}

func getNonBackingDisks(address string) (map[string]types.DiskInfo, error) {
	repClient, err := replicaClient.NewReplicaClient(address)
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

func GetSnapshotsInfo(replicas []*types.ControllerReplicaInfo) (outputDisks map[string]types.DiskInfo, err error) {
	defer func() {
		err = errors.Wrapf(err, "BUG: cannot get snapshot info")
	}()
	for _, r := range replicas {
		if r.Mode != types.RW {
			continue
		}

		disks, err := getNonBackingDisks(r.Address)
		if err != nil {
			return nil, err
		}

		newDisks := make(map[string]types.DiskInfo)
		for name, disk := range disks {
			snapshot := ""

			if !replica.IsHeadDisk(name) {
				snapshot, err = replica.GetSnapshotNameFromDiskName(name)
				if err != nil {
					return nil, err
				}
			} else {
				snapshot = VolumeHeadName
			}
			children := map[string]bool{}
			for childDisk := range disk.Children {
				child := ""
				if !replica.IsHeadDisk(childDisk) {
					child, err = replica.GetSnapshotNameFromDiskName(childDisk)
					if err != nil {
						return nil, err
					}
				} else {
					child = VolumeHeadName
				}
				children[child] = true
			}
			parent := ""
			if disk.Parent != "" {
				parent, err = replica.GetSnapshotNameFromDiskName(disk.Parent)
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

		repClient, err := replicaClient.NewReplicaClient(r.Address)
		if err != nil {
			return nil, err
		}
		clients = append(clients, repClient)

		restoreStatus, err := repClient.RestoreStatus()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to check the restore status before fetching the rebuild status")
		}
		if restoreStatus.DestFileName != "" {
			logrus.Debugf("Skip checking rebuild status since the volume is a restore/DR volume")
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

func CloneSnapshot(engineControllerClient, fromControllerClient *client.ControllerClient, snapshotFileName string, exportBackingImageIfExist bool) error {
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
		return fmt.Errorf("cannot find a RW replica in the source volume for clonning")
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
			repClient, err := replicaClient.NewReplicaClient(r.Address)
			if err != nil {
				syncErrorMap.Store(r.Address, err)
				return
			}
			defer repClient.Close()
			if err := repClient.CloneSnapshot(sourceReplica.Address, snapshotFileName, exportBackingImageIfExist); err != nil {
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

func CloneStatus(engineControllerClient *client.ControllerClient) (map[string]*SnapshotCloneStatus, error) {
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
		repClient, err := replicaClient.NewReplicaClient(r.Address)
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
