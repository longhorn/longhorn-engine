package rpc

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/pkg/reexec"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-engine/backup"
	"github.com/longhorn/longhorn-engine/replica"
	replicarpc "github.com/longhorn/longhorn-engine/replica/rpc"
	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

const (
	MaxBackupSize = 5

	PeriodicRefreshIntervalInSeconds = 2

	GRPCServiceCommonTimeout = 1 * time.Minute
)

type SyncAgentServer struct {
	sync.Mutex

	currentPort     int
	startPort       int
	endPort         int
	processesByPort map[int]string
	isRestoring     bool
	lastRestored    string
	replicaAddress  string

	BackupList  *BackupList
	RestoreInfo *replica.RestoreStatus
}

type BackupList struct {
	sync.RWMutex
	backups []*BackupInfo
}

type BackupInfo struct {
	backupID     string
	backupStatus *replica.BackupStatus
}

func NewSyncAgentServer(startPort, endPort int, replicaAddress string) *SyncAgentServer {
	return &SyncAgentServer{
		currentPort:     startPort,
		startPort:       startPort,
		endPort:         endPort,
		processesByPort: map[int]string{},
		replicaAddress:  replicaAddress,

		BackupList: &BackupList{
			RWMutex: sync.RWMutex{},
		},
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
	s.Lock()
	defer s.Unlock()
	return s.isRestoring
}

func (s *SyncAgentServer) GetLastRestored() string {
	s.Lock()
	defer s.Unlock()
	return s.lastRestored
}

func (s *SyncAgentServer) PrepareRestore(lastRestored string) error {
	s.Lock()
	defer s.Unlock()
	if s.isRestoring {
		return fmt.Errorf("cannot initate backup restore as there is one already in progress")
	}

	if s.lastRestored != "" && lastRestored != "" && s.lastRestored != lastRestored {
		return fmt.Errorf("flag lastRestored %v in command doesn't match field LastRestored %v in engine",
			lastRestored, s.lastRestored)
	}
	s.isRestoring = true
	return nil
}

func (s *SyncAgentServer) FinishRestore(currentRestored string) error {
	s.Lock()
	defer s.Unlock()
	if !s.isRestoring {
		return fmt.Errorf("BUG: volume is not restoring")
	}
	if currentRestored != "" {
		s.lastRestored = currentRestored
	}
	s.isRestoring = false
	s.RestoreInfo.FinishRestore()
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
	s.BackupList = &BackupList{
		RWMutex: sync.RWMutex{},
	}
	return &empty.Empty{}, nil
}

func (*SyncAgentServer) FileRemove(ctx context.Context, req *FileRemoveRequest) (*empty.Empty, error) {
	logrus.Infof("Running rm %v", req.FileName)

	if err := os.Remove(req.FileName); err != nil {
		logrus.Infof("Error running %s %v: %v", "rm", req.FileName, err)
		return nil, err
	}

	logrus.Infof("Done running %s %v", "rm", req.FileName)
	return &empty.Empty{}, nil
}

func (*SyncAgentServer) FileRename(ctx context.Context, req *FileRenameRequest) (*empty.Empty, error) {
	logrus.Infof("Running rename file from %v to %v", req.OldFileName, req.NewFileName)

	if err := os.Rename(req.OldFileName, req.NewFileName); err != nil {
		logrus.Infof("Error running %s from %v to %v: %v", "rename", req.OldFileName, req.NewFileName, err)
		return nil, err
	}

	logrus.Infof("Done running %s from %v to %v", "rename", req.OldFileName, req.NewFileName)
	return &empty.Empty{}, nil
}

func (*SyncAgentServer) FileCoalesce(ctx context.Context, req *FileCoalesceRequest) (*empty.Empty, error) {
	cmd := reexec.Command("sfold", req.FromFileName, req.ToFileName)
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
		logrus.Infof("Error running %s %v: %v", "sfold", cmd.Args, err)
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) FileSend(ctx context.Context, req *FileSendRequest) (*empty.Empty, error) {
	args := []string{"ssync"}
	if req.Host != "" {
		args = append(args, "-host", req.Host)
	}
	if req.Port != 0 {
		args = append(args, "-port", strconv.FormatInt(int64(req.Port), 10))
	}
	if req.FromFileName != "" {
		args = append(args, req.FromFileName)
	}

	cmd := reexec.Command(args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	logrus.Infof("Running %s %v", "ssync", args)
	if err := cmd.Wait(); err != nil {
		logrus.Infof("Error running %s %v: %v", "ssync", args, err)
		return nil, err
	}

	logrus.Infof("Done running %s %v", "ssync", args)
	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) ReceiverLaunch(ctx context.Context, req *ReceiverLaunchRequest) (*ReceiverLaunchReply, error) {
	port, err := s.nextPort("LaunchReceiver")
	if err != nil {
		return nil, err
	}

	go func() {
		defer func() {
			s.Lock()
			delete(s.processesByPort, int(port))
			s.Unlock()
		}()

		args := []string{"ssync"}
		if port != 0 {
			args = append(args, "-port", strconv.Itoa(port))
		}
		if req.ToFileName != "" {
			args = append(args, "-daemon", req.ToFileName)
		}
		cmd := reexec.Command(args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Pdeathsig: syscall.SIGKILL,
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err = cmd.Start(); err != nil {
			logrus.Errorf("Error running %s %v: %v", "ssync", args, err)
			return
		}

		logrus.Infof("Running %s %v", "ssync", args)
		if err = cmd.Wait(); err != nil {
			logrus.Errorf("Error running %s %v: %v", "ssync", args, err)
			return
		}

		logrus.Infof("Done running %s %v", "ssync", args)
	}()

	return &ReceiverLaunchReply{Port: int32(port)}, nil
}

func (s *SyncAgentServer) BackupCreate(ctx context.Context, req *BackupCreateRequest) (*BackupCreateReply, error) {
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
		} else if os.Getenv(types.AWSAccessKey) == "" || os.Getenv(types.AWSSecretKey) == "" {
			return nil, errors.New("Could not backup to s3 without setting credential secret")
		}
	}

	backupID, replicaObj, err := backup.DoBackupCreate(req.VolumeName, req.SnapshotFileName, req.BackupTarget, req.Labels)
	if err != nil {
		logrus.Errorf("Error creating backup: %v", err)
		return nil, err
	}

	reply := &BackupCreateReply{
		Backup: backupID,
	}

	if err := s.BackupList.BackupAdd(backupID, replicaObj); err != nil {
		return nil, fmt.Errorf("failed to add the backup object: %v", err)
	}

	logrus.Infof("Done initiating backup creation, received backupID: %v", reply.Backup)
	return reply, nil
}

func (s *SyncAgentServer) BackupGetStatus(ctx context.Context, req *BackupProgressRequest) (*BackupStatusReply, error) {
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

	reply := &BackupStatusReply{
		Progress:     int32(replicaObj.Progress),
		BackupURL:    replicaObj.BackupURL,
		Error:        replicaObj.Error,
		SnapshotName: snapshotName,
		State:        string(replicaObj.State),
	}
	return reply, nil
}

func (*SyncAgentServer) BackupRemove(ctx context.Context, req *BackupRemoveRequest) (*empty.Empty, error) {
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
		s.RestoreInfo.Lock()
		restoreProgress = s.RestoreInfo.Progress
		restoreError = s.RestoreInfo.Error
		s.RestoreInfo.Unlock()

		if restoreProgress == 100 {
			logrus.Infof("Restore completed successfully in Server")
			periodicChecker.Stop()
			return nil
		}
		if restoreError != "" {
			logrus.Errorf("Backup Restore Error Found in Server[%v]", restoreError)
			periodicChecker.Stop()
			return fmt.Errorf("%v", restoreError)
		}
	}
	return nil
}

func (s *SyncAgentServer) BackupRestore(ctx context.Context, req *BackupRestoreRequest) (e *empty.Empty, err error) {
	backupType, err := util.CheckBackupType(req.Backup)
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
		} else if os.Getenv(types.AWSAccessKey) == "" || os.Getenv(types.AWSSecretKey) == "" {
			return nil, errors.New("Could not backup to s3 without setting credential secret")
		}
	}
	if err := s.PrepareRestore(""); err != nil {
		logrus.Errorf("failed to prepare backup restore: %v", err)
		return nil, err
	}

	s.Lock()
	defer s.Unlock()
	restoreObj := replica.NewRestore(req.SnapshotFileName, s.replicaAddress)
	restoreObj.BackupURL = req.Backup

	if err := backup.DoBackupRestore(req.Backup, req.SnapshotFileName, restoreObj); err != nil {
		// Reset the isRestoring flag to false
		if extraErr := s.FinishRestore(""); extraErr != nil {
			return nil, fmt.Errorf("%v: %v", extraErr, err)
		}
		return nil, fmt.Errorf("error initiating backup restore [%v]", err)
	}
	s.RestoreInfo = restoreObj
	logrus.Infof("Successfully initiated restore for %v to [%v]", req.Backup, req.SnapshotFileName)

	go s.completeBackupRestore()

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) completeBackupRestore() (err error) {
	defer func() {
		if err != nil {
			// Reset the isRestoring flag to false
			if err := s.FinishRestore(""); err != nil {
				logrus.Errorf("failed to finish backup restore: %v", err)
				return
			}
		}
	}()

	if err := s.waitForRestoreComplete(); err != nil {
		logrus.Errorf("failed to restore: %v", err)
		return err
	}

	s.Lock()
	if s.RestoreInfo == nil {
		s.Unlock()
		logrus.Errorf("BUG: Restore completed but object not found")
		return fmt.Errorf("cannot find restore status")
	}

	restoreStatus := &replica.RestoreStatus{
		SnapshotName:     s.RestoreInfo.SnapshotName,
		Progress:         s.RestoreInfo.Progress,
		Error:            s.RestoreInfo.Error,
		LastRestored:     s.RestoreInfo.LastRestored,
		SnapshotDiskName: s.RestoreInfo.SnapshotDiskName,
		BackupURL:        s.RestoreInfo.BackupURL,
	}
	s.Unlock()

	//Create the meta file as the file is now available
	if err := backup.CreateNewSnapshotMetafile(restoreStatus.SnapshotName + ".meta"); err != nil {
		logrus.Errorf("failed creating meta snapshot file: %v", err)
		return err
	}

	//Check if this is incremental fallback to full restore
	if strings.HasSuffix(restoreStatus.SnapshotName, ".snap_tmp") {
		if err := s.postIncrementalFullRestoreOperations(restoreStatus); err != nil {
			logrus.Errorf("failed to complete incremental fallback full restore: %v", err)
			return err
		}
		logrus.Infof("Done running restore %v to %v", restoreStatus.BackupURL, restoreStatus.SnapshotName)
		return nil
	}

	now := time.Now().UTC().Format(time.RFC3339)

	logrus.Infof("Reverting to snapshot %s on %s at %s", restoreStatus.SnapshotName, s.replicaAddress, now)
	if err := s.replicaRevert(restoreStatus.SnapshotName, now); err != nil {
		logrus.Errorf("Error on reverting to %s on %s: %v", restoreStatus.SnapshotName, s.replicaAddress, err)
		//TODO: Need to set replica mode to error
		return err
	}
	logrus.Infof("Reverting to snapshot %s on %s successful", restoreStatus.SnapshotName, s.replicaAddress)

	backupName := ""
	if backupName, err = backupstore.GetBackupFromBackupURL(util.UnescapeURL(restoreStatus.BackupURL)); err != nil {
		return err
	}
	if err = s.FinishRestore(backupName); err != nil {
		return err
	}

	logrus.Infof("Done running restore %v to %v", restoreStatus.BackupURL, restoreStatus.SnapshotName)
	return nil
}

func (s *SyncAgentServer) postIncrementalFullRestoreOperations(restoreStatus *replica.RestoreStatus) error {
	tmpSnapshotDiskName := restoreStatus.SnapshotName
	snapshotDiskName, err := replica.GetSnapshotNameFromTempFileName(tmpSnapshotDiskName)
	if err != nil {
		logrus.Errorf("failed to get snapshotName from tempFileName: %v", err)
		return err
	}
	snapshotDiskMetaName := replica.GenerateSnapshotDiskMetaName(snapshotDiskName)
	tmpSnapshotDiskMetaName := replica.GenerateSnapshotDiskMetaName(tmpSnapshotDiskName)

	defer func() {
		// try to cleanup tmp files
		if _, err := s.FileRemove(nil, &FileRemoveRequest{
			FileName: tmpSnapshotDiskName,
		}); err != nil {
			logrus.Warnf("Failed to cleanup delta file %s: %v", tmpSnapshotDiskName, err)
		}

		if _, err := s.FileRemove(nil, &FileRemoveRequest{
			FileName: tmpSnapshotDiskMetaName,
		}); err != nil {
			logrus.Warnf("Failed to cleanup delta file %s: %v", tmpSnapshotDiskMetaName, err)
		}
	}()

	// replace old snapshot
	fileRenameReq := &FileRenameRequest{
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

	//Reload the replica as snapshot files got changed
	if err := s.reloadReplica(); err != nil {
		logrus.Errorf("failed to reload replica: %v", err)
		return err
	}

	//Successfully finished, update the lastRestored
	backupName := ""
	if backupName, err = backupstore.GetBackupFromBackupURL(util.UnescapeURL(restoreStatus.BackupURL)); err != nil {
		return err
	}
	if err = s.FinishRestore(backupName); err != nil {
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
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaRevert(ctx, &replicarpc.ReplicaRevertRequest{
		Name:    name,
		Created: created,
	}); err != nil {
		return fmt.Errorf("failed to revert replica %v: %v", s.replicaAddress, err)
	}

	return nil
}

func (s *SyncAgentServer) BackupRestoreIncrementally(ctx context.Context,
	req *BackupRestoreIncrementallyRequest) (e *empty.Empty, err error) {
	backupType, err := util.CheckBackupType(req.Backup)
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
		} else if os.Getenv(types.AWSAccessKey) == "" || os.Getenv(types.AWSSecretKey) == "" {
			return nil, errors.New("Could not backup to s3 without setting credential secret")
		}
	}
	if err := s.PrepareRestore(req.LastRestoredBackupName); err != nil {
		logrus.Errorf("failed to prepare incremental restore: %v", err)
		return nil, err
	}

	s.Lock()
	defer s.Unlock()
	restoreObj := replica.NewRestore(req.DeltaFileName, s.replicaAddress)
	restoreObj.LastRestored = req.LastRestoredBackupName
	restoreObj.SnapshotDiskName = req.SnapshotDiskName
	restoreObj.BackupURL = req.Backup

	logrus.Infof("Running incremental restore %v to %s with lastRestoredBackup %s", req.Backup,
		req.DeltaFileName, req.LastRestoredBackupName)
	if err := backup.DoBackupRestoreIncrementally(req.Backup, req.DeltaFileName, req.LastRestoredBackupName,
		restoreObj); err != nil {
		if extraErr := s.FinishRestore(""); extraErr != nil {
			return nil, fmt.Errorf("%v: %v", extraErr, err)
		}
		return nil, fmt.Errorf("error initiating incremental restore [%v]", err)
	}

	s.RestoreInfo = restoreObj
	logrus.Infof("Successfully initiated incremental restore for %v to %v", req.Backup, req.DeltaFileName)

	go s.completeIncrementalBackupRestore()

	return &empty.Empty{}, nil
}

func (s *SyncAgentServer) completeIncrementalBackupRestore() (err error) {
	defer func() {
		if err != nil {
			if err := s.FinishRestore(""); err != nil {
				logrus.Errorf("failed to finish incremental restore: %v", err)
				return
			}
		}
	}()

	if err := s.waitForRestoreComplete(); err != nil {
		logrus.Errorf("failed to incremental restore: %v", err)
		return err
	}
	s.Lock()
	if s.RestoreInfo == nil {
		s.Unlock()
		logrus.Errorf("BUG: Restore completed but object not found")
		return fmt.Errorf("cannot find restore status")
	}

	restoreStatus := &replica.RestoreStatus{
		SnapshotName:     s.RestoreInfo.SnapshotName,
		Progress:         s.RestoreInfo.Progress,
		Error:            s.RestoreInfo.Error,
		LastRestored:     s.RestoreInfo.LastRestored,
		SnapshotDiskName: s.RestoreInfo.SnapshotDiskName,
		BackupURL:        s.RestoreInfo.BackupURL,
	}
	s.Unlock()

	if err := s.postIncrementalRestoreOperations(restoreStatus); err != nil {
		logrus.Errorf("failed to complete incremental restore: %v", err)
		return err
	}

	logrus.Infof("Done running incremental restore %v to %v", restoreStatus.BackupURL, restoreStatus.SnapshotName)
	return nil
}

func (s *SyncAgentServer) postIncrementalRestoreOperations(restoreStatus *replica.RestoreStatus) error {
	deltaFileName := restoreStatus.SnapshotName

	logrus.Infof("Cleaning up incremental restore by Coalescing and removing the file")
	// coalesce delta file to snapshot/disk file
	coalesceReq := &FileCoalesceRequest{
		FromFileName: deltaFileName,
		ToFileName:   restoreStatus.SnapshotDiskName,
	}
	if _, err := s.FileCoalesce(nil, coalesceReq); err != nil {
		logrus.Errorf("Failed to coalesce %s on %s: %v", deltaFileName, restoreStatus.SnapshotDiskName, err)
		return err
	}

	// cleanup
	fileRemoveReq := &FileRemoveRequest{
		FileName: deltaFileName,
	}
	if _, err := s.FileRemove(nil, fileRemoveReq); err != nil {
		logrus.Warnf("Failed to cleanup delta file %s: %v", deltaFileName, err)
		return err
	}

	//Reload the replica as snapshot files got changed
	if err := s.reloadReplica(); err != nil {
		logrus.Errorf("failed to reload replica: %v", err)
		return err
	}

	backupName, err := backupstore.GetBackupFromBackupURL(restoreStatus.BackupURL)
	if err != nil {
		return err
	}
	if err := s.FinishRestore(backupName); err != nil {
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
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaReload(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to reload replica %v: %v", s.replicaAddress, err)
	}

	return nil
}

func (s *SyncAgentServer) RestoreStatus(ctx context.Context, req *empty.Empty) (*RestoreStatusReply, error) {
	rs := &RestoreStatusReply{
		IsRestoring:  s.IsRestoring(),
		LastRestored: s.GetLastRestored(),
	}

	if s.RestoreInfo == nil {
		return rs, nil
	}
	s.RestoreInfo.Lock()
	defer s.RestoreInfo.Unlock()
	restoreStatus := &replica.RestoreStatus{
		SnapshotName: s.RestoreInfo.SnapshotName,
		Progress:     s.RestoreInfo.Progress,
		Error:        s.RestoreInfo.Error,
		State:        s.RestoreInfo.State,
		BackupURL:    s.RestoreInfo.BackupURL,
	}
	rs.Progress = int32(restoreStatus.Progress)
	rs.DestFileName = restoreStatus.SnapshotName
	rs.State = string(restoreStatus.State)
	rs.Error = restoreStatus.Error
	rs.BackupURL = restoreStatus.BackupURL
	return rs, nil
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
