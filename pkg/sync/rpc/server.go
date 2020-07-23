package rpc

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/moby/moby/pkg/reexec"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-engine/pkg/backup"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	replicaclient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
	"github.com/longhorn/sparse-tools/sparse"
	sparserest "github.com/longhorn/sparse-tools/sparse/rest"
)

/*
 * Lock sequence
 * 1. SyncAgentServer
 * 2. BackupList, RestoreInfo or PurgeStatus (cannot be hold at the same time)
 */

const (
	MaxBackupSize = 5

	PeriodicRefreshIntervalInSeconds = 2

	GRPCServiceCommonTimeout = 1 * time.Minute

	FileSyncTimeout = 120

	VolumeHeadName = "volume-head"
)

type SyncAgentServer struct {
	sync.RWMutex

	currentPort     int
	startPort       int
	endPort         int
	processesByPort map[int]string
	isPurging       bool
	isRestoring     bool
	isRebuilding    bool
	lastRestored    string
	replicaAddress  string

	BackupList    *BackupList
	RestoreInfo   *replica.RestoreStatus
	PurgeStatus   *PurgeStatus
	RebuildStatus *RebuildStatus
}

type BackupList struct {
	sync.RWMutex
	backups []*BackupInfo
}

type BackupInfo struct {
	backupID     string
	backupStatus *replica.BackupStatus
}

type PurgeStatus struct {
	sync.RWMutex
	Error    string
	Progress int
	State    types.ProcessState

	processed int
	total     int
}

func (ps *PurgeStatus) UpdateFoldFileProgress(progress int, done bool, err error) {
	ps.Lock()
	// Avoid possible division by zero, also total 0 means nothing to be done
	if ps.total == 0 {
		ps.Progress = 100
	} else {
		ps.Progress = int((float32(ps.processed)/float32(ps.total) + float32(progress)/(float32(ps.total)*100)) * 100)
	}
	ps.Unlock()
}

type RebuildStatus struct {
	sync.RWMutex
	Error              string
	Progress           int
	State              types.ProcessState
	FromReplicaAddress string

	processedSize int64
	totalSize     int64
}

func (rs *RebuildStatus) UpdateSyncFileProgress(size int64) {
	rs.Lock()
	rs.processedSize = rs.processedSize + size
	rs.Progress = int((float32(rs.processedSize) / float32(rs.totalSize)) * 100)
	rs.Unlock()
}

func GetDiskInfo(info *ptypes.DiskInfo) *types.DiskInfo {
	diskInfo := &types.DiskInfo{
		Name:        info.Name,
		Parent:      info.Parent,
		Children:    info.Children,
		Removed:     info.Removed,
		UserCreated: info.UserCreated,
		Created:     info.Created,
		Size:        info.Size,
		Labels:      info.Labels,
	}

	if diskInfo.Labels == nil {
		diskInfo.Labels = map[string]string{}
	}

	return diskInfo
}

func NewSyncAgentServer(startPort, endPort int, replicaAddress string) *SyncAgentServer {
	return &SyncAgentServer{
		currentPort:     startPort,
		startPort:       startPort,
		endPort:         endPort,
		processesByPort: map[int]string{},
		replicaAddress:  replicaAddress,

		BackupList:    &BackupList{},
		PurgeStatus:   &PurgeStatus{},
		RebuildStatus: &RebuildStatus{},
	}
}

func (s *SyncAgentServer) nextPort(processName string) (int, error) {
	s.Lock()
	defer s.Unlock()

	// Must be called with s.Lock() obtained
	for i := 0; i < (s.endPort - s.startPort + 1); i++ {
		port := s.currentPort
		s.currentPort++
		if s.currentPort > s.endPort {
			s.currentPort = s.startPort
		}

		if _, ok := s.processesByPort[port]; ok {
			continue
		}

		s.processesByPort[port] = processName

		return port, nil
	}

	return 0, errors.New("Out of ports")
}

func (s *SyncAgentServer) IsRestoring() bool {
	s.RLock()
	defer s.RUnlock()
	return s.isRestoring
}

func (s *SyncAgentServer) GetLastRestored() string {
	s.RLock()
	defer s.RUnlock()
	return s.lastRestored
}

func (s *SyncAgentServer) PrepareRestore(backupURL, toFile, snapshotDiskName, lastRestored string) (*replica.RestoreStatus, error) {
	s.Lock()
	defer s.Unlock()
	if s.isRestoring {
		return nil, fmt.Errorf("cannot initiate backup restore as there is one already in progress")
	}
	if s.lastRestored != "" && lastRestored != "" && s.lastRestored != lastRestored {
		return nil, fmt.Errorf("field lastRestored %v in the request doesn't match field LastRestored %v in the server", lastRestored, s.lastRestored)
	}

	requestedBackupName, err := backupstore.GetBackupFromBackupURL(util.UnescapeURL(backupURL))
	if err != nil {
		return nil, err
	}
	restoreInfo := replica.NewRestore(toFile, s.replicaAddress, backupURL, requestedBackupName)
	restoreInfo.BackupURL = backupURL
	restoreInfo.SnapshotDiskName = snapshotDiskName
	s.RestoreInfo = restoreInfo
	s.isRestoring = true
	return s.RestoreInfo, nil
}

func (s *SyncAgentServer) FinishRestore(currentRestored string, err error) error {
	s.Lock()
	defer s.Unlock()
	return s.finishRestoreNoLock(currentRestored, err)
}

func (s *SyncAgentServer) finishRestoreNoLock(currentRestored string, restoreErr error) (err error) {
	defer func() {
		if s.RestoreInfo != nil {
			if restoreErr != nil {
				s.RestoreInfo.UpdateRestoreStatus(s.RestoreInfo.ToFileName, 0, restoreErr)
			} else {
				s.RestoreInfo.FinishRestore()
			}
		}
	}()

	if !s.isRestoring {
		err = fmt.Errorf("BUG: volume is not restoring")
		if restoreErr != nil {
			restoreErr = types.CombineErrors(err, restoreErr)
		} else {
			restoreErr = err
		}
		return err
	}

	s.isRestoring = false
	if currentRestored != "" && restoreErr == nil {
		s.lastRestored = currentRestored
	}

	return nil
}

func (s *SyncAgentServer) Reset(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	s.Lock()
	defer s.Unlock()
	if s.isRestoring {
		logrus.Errorf("replica is currently restoring, cannot reset")
		return nil, fmt.Errorf("replica is currently restoring, cannot reset")
	}
	s.lastRestored = ""
	s.isRestoring = false
	s.BackupList = &BackupList{}
	return &empty.Empty{}, nil
}

func (*SyncAgentServer) FileRemove(ctx context.Context, req *ptypes.FileRemoveRequest) (*empty.Empty, error) {
	logrus.Infof("Running rm %v", req.FileName)

	if err := os.Remove(req.FileName); err != nil {
		logrus.Infof("Error running %s %v: %v", "rm", req.FileName, err)
		return nil, err
	}

	logrus.Infof("Done running %s %v", "rm", req.FileName)
	return &empty.Empty{}, nil
}

func (*SyncAgentServer) FileRename(ctx context.Context, req *ptypes.FileRenameRequest) (*empty.Empty, error) {
	logrus.Infof("Running rename file from %v to %v", req.OldFileName, req.NewFileName)

	if err := os.Rename(req.OldFileName, req.NewFileName); err != nil {
		logrus.Infof("Error running %s from %v to %v: %v", "rename", req.OldFileName, req.NewFileName, err)
		return nil, err
	}

	logrus.Infof("Done running %s from %v to %v", "rename", req.OldFileName, req.NewFileName)
	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) FileSend(ctx context.Context, req *ptypes.FileSendRequest) (*empty.Empty, error) {
	address := fmt.Sprintf("%s:%d", req.Host, req.Port)
	logrus.Infof("Sending file %v to %v", req.FromFileName, address)
	if err := sparse.SyncFile(req.FromFileName, address, FileSyncTimeout); err != nil {
		return nil, err
	}
	logrus.Infof("Done sending file %v to %v", req.FromFileName, address)

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) ReceiverLaunch(ctx context.Context, req *ptypes.ReceiverLaunchRequest) (*ptypes.ReceiverLaunchResponse, error) {
	port, err := s.launchReceiver("ReceiverLaunch", req.ToFileName, &sparserest.SyncFileStub{})
	if err != nil {
		return nil, err
	}
	logrus.Infof("Launching receiver for file %v", req.ToFileName)

	return &ptypes.ReceiverLaunchResponse{Port: int32(port)}, nil
}

func (s *SyncAgentServer) launchReceiver(processName, toFileName string, ops sparserest.SyncFileOperations) (int, error) {
	port, err := s.nextPort(processName)
	if err != nil {
		return 0, err
	}

	go func() {
		defer func() {
			s.Lock()
			delete(s.processesByPort, port)
			s.Unlock()
		}()

		logrus.Infof("Running ssync server for file %v at port %v", toFileName, port)
		if err = sparserest.Server(strconv.Itoa(port), toFileName, ops); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("Error running ssync server: %v", err)
			return
		}
		logrus.Infof("Done running ssync server for file %v at port %v", toFileName, port)
	}()

	return port, nil
}

func (s *SyncAgentServer) FilesSync(ctx context.Context, req *ptypes.FilesSyncRequest) (res *empty.Empty, err error) {
	if err := s.PrepareRebuild(req.SyncFileInfoList, req.FromAddress); err != nil {
		return nil, err
	}

	defer func() {
		s.RebuildStatus.Lock()
		if err != nil {
			s.RebuildStatus.Error = err.Error()
			s.RebuildStatus.State = types.ProcessStateError
			logrus.Errorf("Sync agent gRPC server failed to rebuild replica/sync files: %v", err)
		} else {
			s.RebuildStatus.State = types.ProcessStateComplete
			logrus.Infof("Sync agent gRPC server finished rebuilding replica/sync files for replica %v", req.ToHost)
		}
		s.RebuildStatus.Unlock()

		if err = s.FinishRebuild(); err != nil {
			logrus.Errorf("could not finish rebuilding: %v", err)
		}
	}()

	fromClient, err := replicaclient.NewReplicaClient(req.FromAddress)
	if err != nil {
		return nil, err
	}

	var ops sparserest.SyncFileOperations
	fileStub := &sparserest.SyncFileStub{}
	for _, info := range req.SyncFileInfoList {
		// Do not count size for disk meta file or empty disk file.
		if info.ActualSize == 0 {
			ops = fileStub
		} else {
			ops = s.RebuildStatus
		}

		port, err := s.launchReceiver("FilesSync", info.ToFileName, ops)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to launch receiver for file %v", info.ToFileName)
		}
		if err := fromClient.SendFile(info.FromFileName, req.ToHost, int32(port)); err != nil {
			return nil, errors.Wrapf(err, "replica %v failed to send file %v to %v:%v", req.FromAddress, info.ToFileName, req.ToHost, port)
		}
	}

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) PrepareRebuild(list []*ptypes.SyncFileInfo, fromReplicaAddress string) error {
	s.Lock()
	defer s.Unlock()

	if s.isRebuilding {
		return fmt.Errorf("replica is already rebuilding")
	}

	s.isRebuilding = true

	s.RebuildStatus.Lock()
	s.RebuildStatus.FromReplicaAddress = fromReplicaAddress
	s.RebuildStatus.Error = ""
	s.RebuildStatus.State = types.ProcessStateInProgress
	// avoid possible division by zero
	s.RebuildStatus.processedSize = 1
	s.RebuildStatus.totalSize = 1
	for _, info := range list {
		s.RebuildStatus.totalSize += info.ActualSize
	}
	s.RebuildStatus.Progress = int((float32(s.RebuildStatus.processedSize) / float32(s.RebuildStatus.totalSize)) * 100)
	s.RebuildStatus.Unlock()

	return nil
}

func (s *SyncAgentServer) FinishRebuild() error {
	s.Lock()
	defer s.Unlock()

	if !s.isRebuilding {
		return fmt.Errorf("BUG: replica is not rebuilding")
	}

	s.isRebuilding = false
	return nil
}

func (s *SyncAgentServer) IsRebuilding() bool {
	s.RLock()
	defer s.RUnlock()

	return s.isRebuilding
}

func (s *SyncAgentServer) ReplicaRebuildStatus(ctx context.Context, req *empty.Empty) (*ptypes.ReplicaRebuildStatusResponse, error) {
	isRebuilding := s.IsRebuilding()

	s.RebuildStatus.RLock()
	defer s.RebuildStatus.RUnlock()
	return &ptypes.ReplicaRebuildStatusResponse{
		IsRebuilding:       isRebuilding,
		Error:              s.RebuildStatus.Error,
		Progress:           int32(s.RebuildStatus.Progress),
		State:              string(s.RebuildStatus.State),
		FromReplicaAddress: s.RebuildStatus.FromReplicaAddress,
	}, nil
}

func (s *SyncAgentServer) BackupCreate(ctx context.Context, req *ptypes.BackupCreateRequest) (*ptypes.BackupCreateResponse, error) {
	backupType, err := util.CheckBackupType(req.BackupTarget)
	if err != nil {
		return nil, err
	}
	// set aws credential
	if backupType == "s3" {
		credential := req.Credential
		// validate environment variable first, since CronJob has set credential to environment variable.
		if credential != nil && credential[types.AWSAccessKey] != "" && credential[types.AWSSecretKey] != "" {
			os.Setenv(types.AWSAccessKey, credential[types.AWSAccessKey])
			os.Setenv(types.AWSSecretKey, credential[types.AWSSecretKey])
			os.Setenv(types.AWSEndPoint, credential[types.AWSEndPoint])
			os.Setenv(types.HTTPSProxy, credential[types.HTTPSProxy])
			os.Setenv(types.HTTPProxy, credential[types.HTTPProxy])
			os.Setenv(types.NOProxy, credential[types.NOProxy])
			os.Setenv(types.VirtualHostedStyle, credential[types.VirtualHostedStyle])

			// set a custom ca cert if available
			if credential[types.AWSCert] != "" {
				os.Setenv(types.AWSCert, credential[types.AWSCert])
			}
		} else if os.Getenv(types.AWSAccessKey) == "" || os.Getenv(types.AWSSecretKey) == "" {
			return nil, errors.New("Could not backup to s3 without setting credential secret")
		}
	}

	backupID, replicaObj, err := backup.DoBackupCreate(req.VolumeName, req.SnapshotFileName, req.BackupTarget, req.Labels)
	if err != nil {
		logrus.Errorf("Error creating backup: %v", err)
		return nil, err
	}

	resp := &ptypes.BackupCreateResponse{
		Backup:        backupID,
		IsIncremental: replicaObj.IsIncremental,
	}

	if err := s.BackupList.BackupAdd(backupID, replicaObj); err != nil {
		return nil, fmt.Errorf("failed to add the backup object: %v", err)
	}

	logrus.Infof("Done initiating backup creation, received backupID: %v", resp.Backup)
	return resp, nil
}

func (s *SyncAgentServer) BackupStatus(ctx context.Context, req *ptypes.BackupStatusRequest) (*ptypes.BackupStatusResponse, error) {
	if req.Backup == "" {
		return nil, fmt.Errorf("bad request: empty backup name")
	}

	replicaObj, err := s.BackupList.BackupGet(req.Backup)
	if err != nil {
		return nil, err
	}

	snapshotName, err := replica.GetSnapshotNameFromDiskName(replicaObj.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("couldn't get snapshot name: %v", err)
	}

	resp := &ptypes.BackupStatusResponse{
		Progress:     int32(replicaObj.Progress),
		BackupUrl:    replicaObj.BackupURL,
		Error:        replicaObj.Error,
		SnapshotName: snapshotName,
		State:        string(replicaObj.State),
	}
	return resp, nil
}

func (*SyncAgentServer) BackupRemove(ctx context.Context, req *ptypes.BackupRemoveRequest) (*empty.Empty, error) {
	cmd := reexec.Command("sbackup", "delete", req.Backup)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	logrus.Infof("Running %s %v", cmd.Path, cmd.Args)
	if err := cmd.Wait(); err != nil {
		logrus.Infof("Error running %s %v: %v", "sbackup", cmd.Args, err)
		return nil, err
	}

	logrus.Infof("Done running %s %v", "sbackup", cmd.Args)
	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) waitForRestoreComplete() error {
	var (
		restoreProgress int
		restoreError    string
	)
	periodicChecker := time.NewTicker(PeriodicRefreshIntervalInSeconds * time.Second)

	for range periodicChecker.C {
		s.RestoreInfo.RLock()
		restoreProgress = s.RestoreInfo.Progress
		restoreError = s.RestoreInfo.Error
		s.RestoreInfo.RUnlock()

		if restoreProgress == 100 {
			logrus.Infof("Backup data restore completed successfully in Server")
			periodicChecker.Stop()
			return nil
		}
		if restoreError != "" {
			logrus.Errorf("Backup data restore Error Found in Server[%v]", restoreError)
			periodicChecker.Stop()
			return fmt.Errorf("%v", restoreError)
		}
	}
	return nil
}

func (s *SyncAgentServer) BackupRestore(ctx context.Context, req *ptypes.BackupRestoreRequest) (e *empty.Empty, err error) {
	// Check request
	if req.SnapshotFileName == "" {
		return nil, fmt.Errorf("empty snapshot file name for the restore")
	}
	if req.Backup == "" {
		return nil, fmt.Errorf("empty backup URL for the restore")
	}
	backupType, err := util.CheckBackupType(req.Backup)
	if err != nil {
		return nil, fmt.Errorf("failed to check the type for backup %v: %v", req.Backup, err)
	}
	// Check/Set AWS credential
	if backupType == "s3" {
		credential := req.Credential
		// validate environment variable first, since CronJob has set credential to environment variable.
		if credential != nil && credential[types.AWSAccessKey] != "" && credential[types.AWSSecretKey] != "" {
			os.Setenv(types.AWSAccessKey, credential[types.AWSAccessKey])
			os.Setenv(types.AWSSecretKey, credential[types.AWSSecretKey])
			os.Setenv(types.AWSEndPoint, credential[types.AWSEndPoint])
			os.Setenv(types.HTTPSProxy, credential[types.HTTPSProxy])
			os.Setenv(types.HTTPProxy, credential[types.HTTPProxy])
			os.Setenv(types.NOProxy, credential[types.NOProxy])
			os.Setenv(types.VirtualHostedStyle, credential[types.VirtualHostedStyle])

			// set a custom ca cert if available
			if credential[types.AWSCert] != "" {
				os.Setenv(types.AWSCert, credential[types.AWSCert])
			}
		} else if os.Getenv(types.AWSAccessKey) == "" || os.Getenv(types.AWSSecretKey) == "" {
			return nil, fmt.Errorf("could not do backup restore from s3 without setting credential secret")
		}
	}

	restoreInfo, err := s.PrepareRestore(req.Backup, req.SnapshotFileName, "", "")
	if err != nil {
		return nil, errors.Wrapf(err, "error preparing backup restore")
	}

	if err := backup.DoBackupRestore(req.Backup, req.SnapshotFileName, restoreInfo); err != nil {
		if extraErr := s.FinishRestore("", err); extraErr != nil {
			return nil, fmt.Errorf("error finishing restore after backup restore initialization failure: [%v]", err)
		}
		return nil, errors.Wrapf(err, "error initiating backup restore")
	}
	logrus.Infof("Successfully initiated restore for %v to [%v]", req.Backup, req.SnapshotFileName)

	go s.completeBackupRestore()

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) completeBackupRestore() (err error) {
	currentRestoredBackupName := ""
	defer func() {
		if extraErr := s.FinishRestore(currentRestoredBackupName, err); extraErr != nil {
			logrus.Errorf("failed to finish backup restore: %v", extraErr)
			return
		}
	}()

	if err := s.waitForRestoreComplete(); err != nil {
		return errors.Wrapf(err, "failed to wait for restore complete")
	}

	s.RLock()
	restoreStatus := s.RestoreInfo.DeepCopy()
	s.RUnlock()

	if err := backup.CreateNewSnapshotMetafile(restoreStatus.ToFileName + ".meta"); err != nil {
		logrus.Errorf("failed creating meta snapshot file: %v", err)
		return err
	}

	// Check if this full restore is the fallback of the incremental restore
	if strings.HasSuffix(restoreStatus.ToFileName, ".snap_tmp") {
		if err := s.postIncrementalFullRestoreOperations(restoreStatus); err != nil {
			logrus.Errorf("failed to complete incremental fallback full restore: %v", err)
			return err
		}
		logrus.Infof("Done running full restore %v to %v as the fallback of the incremental restore",
			restoreStatus.BackupURL, restoreStatus.ToFileName)
	} else {
		if err := s.replicaRevert(restoreStatus.ToFileName, time.Now().UTC().Format(time.RFC3339)); err != nil {
			logrus.Errorf("Error on reverting to %s on %s: %v", restoreStatus.ToFileName, s.replicaAddress, err)
			return err
		}
		logrus.Infof("Reverting to snapshot %s on %s successful", restoreStatus.ToFileName, s.replicaAddress)
	}

	if currentRestoredBackupName, err = backupstore.GetBackupFromBackupURL(util.UnescapeURL(restoreStatus.BackupURL)); err != nil {
		return err
	}

	logrus.Infof("Done running restore %v to %v", restoreStatus.BackupURL, restoreStatus.ToFileName)
	return nil
}

func (s *SyncAgentServer) postIncrementalFullRestoreOperations(restoreStatus *replica.RestoreStatus) error {
	tmpSnapshotDiskName := restoreStatus.ToFileName
	snapshotDiskName, err := replica.GetSnapshotNameFromTempFileName(tmpSnapshotDiskName)
	if err != nil {
		logrus.Errorf("failed to get snapshotName from tempFileName: %v", err)
		return err
	}
	snapshotDiskMetaName := replica.GenerateSnapshotDiskMetaName(snapshotDiskName)
	tmpSnapshotDiskMetaName := replica.GenerateSnapshotDiskMetaName(tmpSnapshotDiskName)

	defer func() {
		// try to cleanup tmp files
		if _, err := s.FileRemove(nil, &ptypes.FileRemoveRequest{
			FileName: tmpSnapshotDiskName,
		}); err != nil {
			logrus.Warnf("Failed to cleanup delta file %s: %v", tmpSnapshotDiskName, err)
		}

		if _, err := s.FileRemove(nil, &ptypes.FileRemoveRequest{
			FileName: tmpSnapshotDiskMetaName,
		}); err != nil {
			logrus.Warnf("Failed to cleanup delta file %s: %v", tmpSnapshotDiskMetaName, err)
		}
	}()

	// replace old snapshot
	fileRenameReq := &ptypes.FileRenameRequest{
		OldFileName: tmpSnapshotDiskName,
		NewFileName: snapshotDiskName,
	}
	if _, err = s.FileRename(nil, fileRenameReq); err != nil {
		logrus.Errorf("failed to replace old snapshot %v with the fully restored file %v: %v",
			snapshotDiskName, tmpSnapshotDiskName, err)
		return err
	}
	fileRenameReq.OldFileName = tmpSnapshotDiskMetaName
	fileRenameReq.NewFileName = snapshotDiskMetaName
	if _, err = s.FileRename(nil, fileRenameReq); err != nil {
		logrus.Errorf("failed to replace old snapshot meta %v with the fully restored meta file %v: %v",
			snapshotDiskMetaName, tmpSnapshotDiskMetaName, err)
		return err
	}

	// Reload the replica as snapshot files got changed
	if err := s.reloadReplica(); err != nil {
		logrus.Errorf("failed to reload replica: %v", err)
		return err
	}

	return nil
}

func (s *SyncAgentServer) replicaRevert(name, created string) error {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaRevert(ctx, &ptypes.ReplicaRevertRequest{
		Name:    name,
		Created: created,
	}); err != nil {
		return fmt.Errorf("failed to revert replica %v: %v", s.replicaAddress, err)
	}

	return nil
}

func (s *SyncAgentServer) BackupRestoreIncrementally(ctx context.Context,
	req *ptypes.BackupRestoreIncrementallyRequest) (e *empty.Empty, err error) {
	// Check request
	if req.DeltaFileName == "" {
		return nil, fmt.Errorf("empty DeltaFileName for the incremental restore")
	}
	if req.Backup == "" {
		return nil, fmt.Errorf("empty Backup URL for the incremental restore")
	}
	backupType, err := util.CheckBackupType(req.Backup)
	if err != nil {
		return nil, fmt.Errorf("failed to check the type for backup %v: %v", req.Backup, err)
	}
	if backupType == "s3" {
		credential := req.Credential
		// validate environment variable first, since CronJob has set credential to environment variable.
		if credential != nil && credential[types.AWSAccessKey] != "" && credential[types.AWSSecretKey] != "" {
			os.Setenv(types.AWSAccessKey, credential[types.AWSAccessKey])
			os.Setenv(types.AWSSecretKey, credential[types.AWSSecretKey])
			os.Setenv(types.AWSEndPoint, credential[types.AWSEndPoint])
			os.Setenv(types.HTTPSProxy, credential[types.HTTPSProxy])
			os.Setenv(types.HTTPProxy, credential[types.HTTPProxy])
			os.Setenv(types.NOProxy, credential[types.NOProxy])
			os.Setenv(types.VirtualHostedStyle, credential[types.VirtualHostedStyle])

			// set a custom ca cert if available
			if credential[types.AWSCert] != "" {
				os.Setenv(types.AWSCert, credential[types.AWSCert])
			}
		} else if os.Getenv(types.AWSAccessKey) == "" || os.Getenv(types.AWSSecretKey) == "" {
			return nil, fmt.Errorf("could not do incremental backup restore from s3 without setting credential secret")
		}
	}

	restoreInfo, err := s.PrepareRestore(req.Backup, req.DeltaFileName, req.SnapshotDiskName, req.LastRestoredBackupName)
	if err != nil {
		return nil, errors.Wrapf(err, "error preparing incremental backup restore")
	}

	if err := backup.DoBackupRestoreIncrementally(req.Backup, req.DeltaFileName, req.LastRestoredBackupName, restoreInfo); err != nil {
		if extraErr := s.FinishRestore("", err); extraErr != nil {
			return nil, fmt.Errorf("error finishing incremental restore after restore initialization failure: [%v]", extraErr)
		}
		return nil, errors.Wrapf(err, "error initiating incremental backup restore")
	}

	go s.completeIncrementalBackupRestore()

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) completeIncrementalBackupRestore() (err error) {
	currentRestoredBackupName := ""
	defer func() {
		if extraErr := s.FinishRestore(currentRestoredBackupName, err); extraErr != nil {
			logrus.Errorf("failed to finish incremental restore: %v", extraErr)
			return
		}
	}()

	if err := s.waitForRestoreComplete(); err != nil {
		return errors.Wrapf(err, "failed to wait for incremental restore complete")
	}

	s.RLock()
	restoreStatus := s.RestoreInfo.DeepCopy()
	s.RUnlock()

	if err := s.postIncrementalRestoreOperations(restoreStatus); err != nil {
		logrus.Errorf("failed to complete incremental restore: %v", err)
		return err
	}

	if currentRestoredBackupName, err = backupstore.GetBackupFromBackupURL(util.UnescapeURL(restoreStatus.BackupURL)); err != nil {
		return err
	}

	logrus.Infof("Done running incremental restore %v to %v", restoreStatus.BackupURL, restoreStatus.ToFileName)
	return nil
}

func (s *SyncAgentServer) postIncrementalRestoreOperations(restoreStatus *replica.RestoreStatus) error {
	deltaFileName := restoreStatus.ToFileName
	logrus.Infof("Cleaning up incremental restore by Coalescing and removing the delta file")
	defer func() {
		if _, err := s.FileRemove(nil, &ptypes.FileRemoveRequest{
			FileName: deltaFileName,
		}); err != nil {
			logrus.Warnf("Failed to cleanup delta file %s: %v", deltaFileName, err)
		}
	}()

	// coalesce delta file to snapshot/disk file
	if err := sparse.FoldFile(deltaFileName, restoreStatus.SnapshotDiskName, &PurgeStatus{}); err != nil {
		logrus.Errorf("Failed to coalesce %s on %s: %v", deltaFileName, restoreStatus.SnapshotDiskName, err)
		return err
	}

	// Reload the replica as snapshot files got changed
	if err := s.reloadReplica(); err != nil {
		logrus.Errorf("failed to reload replica: %v", err)
		return err
	}

	return nil
}

func (s *SyncAgentServer) reloadReplica() error {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaReload(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to reload replica %v: %v", s.replicaAddress, err)
	}

	return nil
}

func (s *SyncAgentServer) RestoreStatus(ctx context.Context, req *empty.Empty) (*ptypes.RestoreStatusResponse, error) {
	resp := ptypes.RestoreStatusResponse{
		IsRestoring:  s.IsRestoring(),
		LastRestored: s.GetLastRestored(),
	}

	if s.RestoreInfo == nil {
		return &resp, nil
	}

	restoreStatus := s.RestoreInfo.DeepCopy()
	resp.Progress = int32(restoreStatus.Progress)
	resp.DestFileName = restoreStatus.SnapshotDiskName
	resp.State = string(restoreStatus.State)
	resp.Error = restoreStatus.Error
	resp.BackupUrl = restoreStatus.BackupURL
	resp.CurrentRestoringBackup = restoreStatus.CurrentRestoringBackup
	return &resp, nil
}

func (s *SyncAgentServer) SnapshotPurge(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	if err := s.PreparePurge(); err != nil {
		return nil, err
	}

	go s.purgeSnapshots()

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) purgeSnapshots() (err error) {
	defer func() {
		s.PurgeStatus.Lock()
		if err != nil {
			s.PurgeStatus.Error = err.Error()
			s.PurgeStatus.State = types.ProcessStateError
		} else {
			s.PurgeStatus.State = types.ProcessStateComplete
		}
		s.PurgeStatus.Unlock()

		if err := s.FinishPurge(); err != nil {
			logrus.Errorf("could not mark finish purge: %v", err)
		}
	}()

	var leaves []string

	snapshotsInfo, _, err := s.getSnapshotsInfo()
	if err != nil {
		return err
	}

	for snapshot, info := range snapshotsInfo {
		if len(info.Children) == 0 {
			leaves = append(leaves, snapshot)
		}
		if info.Name == VolumeHeadName {
			continue
		}
		// Mark system generated snapshots as removed
		if !info.UserCreated && !info.Removed {
			if err := s.markSnapshotAsRemoved(snapshot); err != nil {
				return err
			}
		}
	}

	snapshotsInfo, markedRemoved, err := s.getSnapshotsInfo()
	if err != nil {
		return err
	}

	s.PurgeStatus.Lock()
	s.PurgeStatus.total = markedRemoved
	s.PurgeStatus.Unlock()

	// We're tracing up from each leaf to the root
	var removed int
	for _, leaf := range leaves {
		// Somehow the leaf was removed during the process
		if _, ok := snapshotsInfo[leaf]; !ok {
			continue
		}
		snapshot := leaf
		for snapshot != "" {
			// Snapshot already removed? Skip to the next leaf
			info, ok := snapshotsInfo[snapshot]
			if !ok {
				break
			}
			if info.Removed {
				if info.Name == VolumeHeadName {
					return fmt.Errorf("BUG: Volume head was marked as removed")
				}
				if err := s.processRemoveSnapshot(snapshot); err != nil {
					return err
				}
				removed++
				s.PurgeStatus.Lock()
				s.PurgeStatus.processed = removed
				s.PurgeStatus.Unlock()
			}
			snapshot = info.Parent
		}
		// Update snapshotInfo in case some nodes have been removed
		snapshotsInfo, _, err = s.getSnapshotsInfo()
		if err != nil {
			return err
		}
		s.PurgeStatus.Lock()
		s.PurgeStatus.Progress = int(float32(removed) / float32(markedRemoved) * 100)
		s.PurgeStatus.Unlock()
	}

	s.PurgeStatus.Lock()
	s.PurgeStatus.Progress = 100
	s.PurgeStatus.Unlock()

	return nil
}

func (s *SyncAgentServer) SnapshotPurgeStatus(ctx context.Context, req *empty.Empty) (*ptypes.SnapshotPurgeStatusResponse, error) {
	isPurging := s.IsPurging()

	s.PurgeStatus.RLock()
	defer s.PurgeStatus.RUnlock()
	return &ptypes.SnapshotPurgeStatusResponse{
		IsPurging: isPurging,
		Error:     s.PurgeStatus.Error,
		Progress:  int32(s.PurgeStatus.Progress),
		State:     string(s.PurgeStatus.State),
	}, nil
}

func (s *SyncAgentServer) PreparePurge() error {
	s.Lock()
	defer s.Unlock()

	if s.isPurging {
		return fmt.Errorf("replica is already purging snapshots")
	}

	s.isPurging = true

	s.PurgeStatus.Lock()
	s.PurgeStatus.Error = ""
	s.PurgeStatus.Progress = 0
	s.PurgeStatus.State = types.ProcessStateInProgress
	s.PurgeStatus.total = 0
	s.PurgeStatus.processed = 0
	s.PurgeStatus.Unlock()

	return nil
}

func (s *SyncAgentServer) FinishPurge() error {
	s.Lock()
	defer s.Unlock()

	if !s.isPurging {
		return fmt.Errorf("BUG: replica is not purging snapshots")
	}

	s.isPurging = false
	return nil
}

func (s *SyncAgentServer) IsPurging() bool {
	s.RLock()
	defer s.RUnlock()

	return s.isPurging
}

func (s *SyncAgentServer) getSnapshotsInfo() (map[string]types.DiskInfo, int, error) {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return nil, 0, fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()

	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	resp, err := replicaServiceClient.ReplicaGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, 0, err
	}

	disks := make(map[string]types.DiskInfo)
	for name, disk := range resp.Replica.Disks {
		if name == resp.Replica.BackingFile {
			continue
		}
		disks[name] = *GetDiskInfo(disk)
	}

	newDisks := make(map[string]types.DiskInfo)
	removedCount := 0
	for name, disk := range disks {
		snapshot := ""

		if !replica.IsHeadDisk(name) {
			snapshot, err = replica.GetSnapshotNameFromDiskName(name)
			if err != nil {
				return nil, 0, err
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
					return nil, 0, err
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
				return nil, 0, err
			}
		}

		if disk.Removed {
			removedCount++
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

	return newDisks, removedCount, nil
}

func (s *SyncAgentServer) markSnapshotAsRemoved(snapshot string) error {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()

	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.DiskMarkAsRemoved(ctx, &ptypes.DiskMarkAsRemovedRequest{
		Name: snapshot,
	}); err != nil {
		return err
	}

	return nil
}

func (s *SyncAgentServer) processRemoveSnapshot(snapshot string) error {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()

	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	ops, err := replicaServiceClient.DiskPrepareRemove(ctx, &ptypes.DiskPrepareRemoveRequest{
		Name: snapshot,
	})
	if err != nil {
		return err
	}

	for _, op := range ops.Operations {
		switch op.Action {
		case replica.OpCoalesce:
			logrus.Infof("Coalescing %v to %v", op.Target, op.Source)
			if err := sparse.FoldFile(op.Target, op.Source, s.PurgeStatus); err != nil {
				logrus.Errorf("failed to coalesce %s on %s: %v", op.Target, op.Source, err)
				return err
			}
		case replica.OpRemove:
			logrus.Infof("Removing %v", op.Source)
			if err := s.rmDisk(op.Source); err != nil {
				return err
			}
		case replica.OpReplace:
			logrus.Infof("Replace %v with %v", op.Target, op.Source)
			if err = s.replaceDisk(op.Source, op.Target); err != nil {
				logrus.Errorf("Failed to replace %v with %v", op.Target, op.Source)
				return err
			}
		}
	}

	return nil
}

func (s *SyncAgentServer) replaceDisk(source, target string) error {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()

	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.DiskReplace(ctx, &ptypes.DiskReplaceRequest{
		Source: source,
		Target: target,
	}); err != nil {
		return err
	}

	return nil
}

func (s *SyncAgentServer) rmDisk(disk string) error {
	conn, err := grpc.Dial(s.replicaAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", s.replicaAddress, err)
	}
	defer conn.Close()

	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.DiskRemove(ctx, &ptypes.DiskRemoveRequest{
		Force: false,
		Name:  disk,
	}); err != nil {
		return err
	}

	return nil
}

// The APIs BackupAdd, BackupGet, Refresh, BackupDelete implement the CRUD interface for the backup object
// The slice Backup.backupList is implemented similar to a FIFO queue.

// BackupAdd creates a new backupList object and appends to the end of the list maintained by backup object
func (b *BackupList) BackupAdd(backupID string, backup *replica.BackupStatus) error {
	if backupID == "" {
		return fmt.Errorf("empty backupID")
	}

	b.Lock()
	b.backups = append(b.backups, &BackupInfo{
		backupID:     backupID,
		backupStatus: backup,
	})
	b.Unlock()

	if err := b.Refresh(); err != nil {
		return err
	}

	return nil
}

// BackupGet takes backupID input and will return the backup object corresponding to that backupID or error if not found
func (b *BackupList) BackupGet(backupID string) (*replica.BackupStatus, error) {
	if backupID == "" {
		return nil, fmt.Errorf("empty backupID")
	}

	if err := b.Refresh(); err != nil {
		return nil, err
	}

	b.RLock()
	defer b.RUnlock()

	for _, info := range b.backups {
		if info.backupID == backupID {
			return info.backupStatus, nil
		}
	}
	return nil, fmt.Errorf("backup not found %v", backupID)
}

// remove deletes the object present at slice[index] and returns the remaining elements of slice yet maintaining
// the original order of elements in the slice
func (*BackupList) remove(b []*BackupInfo, index int) ([]*BackupInfo, error) {
	if b == nil {
		return nil, fmt.Errorf("empty list")
	}
	if index >= len(b) || index < 0 {
		return nil, fmt.Errorf("BUG: attempting to delete an out of range index entry from backupList")
	}
	return append(b[:index], b[index+1:]...), nil
}

// Refresh deletes all the old completed backups from the front. Old backups are the completed backups
// that are created before MaxBackupSize completed backups
func (b *BackupList) Refresh() error {
	b.Lock()
	defer b.Unlock()

	var index, completed int

	for index = len(b.backups) - 1; index >= 0; index-- {
		if b.backups[index].backupStatus.Progress == 100 {
			if completed == MaxBackupSize {
				break
			}
			completed++
		}
	}
	if completed == MaxBackupSize {
		//Remove all the older completed backups in the range backupList[0:index]
		for ; index >= 0; index-- {
			if b.backups[index].backupStatus.Progress == 100 {
				updatedList, err := b.remove(b.backups, index)
				if err != nil {
					return err
				}
				b.backups = updatedList
				//As this backupList[index] is removed, will have to decrement the index by one
				index--
			}
		}
	}
	return nil
}

// BackupDelete will delete the entry in the slice with the corresponding backupID
func (b *BackupList) BackupDelete(backupID string) error {
	b.Lock()
	defer b.Unlock()

	for index, backup := range b.backups {
		if backup.backupID == backupID {
			updatedList, err := b.remove(b.backups, index)
			if err != nil {
				return err
			}
			b.backups = updatedList
			return nil
		}
	}
	return fmt.Errorf("backup not found %v", backupID)
}
