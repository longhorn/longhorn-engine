package rpc

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/docker/docker/pkg/reexec"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-engine/backup"
	"github.com/longhorn/longhorn-engine/replica"
	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

const (
	MaxBackupSize = 5
)

type SyncAgentServer struct {
	sync.Mutex

	currentPort     int
	startPort       int
	endPort         int
	processesByPort map[int]string
	isRestoring     bool
	lastRestored    string

	BackupList *BackupList
}

type BackupList struct {
	sync.RWMutex
	backups []*BackupInfo
}

type BackupInfo struct {
	backupID     string
	backupStatus *replica.Backup
}

func NewSyncAgentServer(startPort, endPort int) *SyncAgentServer {
	return &SyncAgentServer{
		currentPort:     startPort,
		startPort:       startPort,
		endPort:         endPort,
		processesByPort: map[int]string{},

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
	return nil
}

func (s *SyncAgentServer) Reset(ctx context.Context, req *Empty) (*Empty, error) {
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
	return &Empty{}, nil
}

func (*SyncAgentServer) FileRemove(ctx context.Context, req *FileRemoveRequest) (*Empty, error) {
	logrus.Infof("Running rm %v", req.FileName)

	if err := os.Remove(req.FileName); err != nil {
		logrus.Infof("Error running %s %v: %v", "rm", req.FileName, err)
		return nil, err
	}

	logrus.Infof("Done running %s %v", "rm", req.FileName)
	return &Empty{}, nil
}

func (*SyncAgentServer) FileRename(ctx context.Context, req *FileRenameRequest) (*Empty, error) {
	logrus.Infof("Running rename file from %v to %v", req.OldFileName, req.NewFileName)

	if err := os.Rename(req.OldFileName, req.NewFileName); err != nil {
		logrus.Infof("Error running %s from %v to %v: %v", "rename", req.OldFileName, req.NewFileName, err)
		return nil, err
	}

	logrus.Infof("Done running %s from %v to %v", "rename", req.OldFileName, req.NewFileName)
	return &Empty{}, nil
}

func (*SyncAgentServer) FileCoalesce(ctx context.Context, req *FileCoalesceRequest) (*Empty, error) {
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

	return &Empty{}, nil
}

func (s *SyncAgentServer) FileSend(ctx context.Context, req *FileSendRequest) (*Empty, error) {
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
	return &Empty{}, nil
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

func (s *SyncAgentServer) BackupGetStatus(ctx context.Context, req *BackupProgressRequest) (*BackupProgressReply, error) {
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

	reply := &BackupProgressReply{
		Progress:     int32(replicaObj.BackupProgress),
		BackupURL:    replicaObj.BackupURL,
		BackupError:  replicaObj.BackupError,
		SnapshotName: snapshotName,
	}
	return reply, nil
}

func (*SyncAgentServer) BackupRemove(ctx context.Context, req *BackupRemoveRequest) (*Empty, error) {
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
	return &Empty{}, nil
}

func (s *SyncAgentServer) BackupRestore(ctx context.Context, req *BackupRestoreRequest) (e *Empty, err error) {
	if err := s.PrepareRestore(""); err != nil {
		logrus.Errorf("failed to prepare backup restore: %v", err)
		return nil, err
	}

	defer func() {
		if err != nil {
			// Reset the isRestoring flag to false
			if err := s.FinishRestore(""); err != nil {
				logrus.Errorf("failed to finish backup restore: %v", err)
				return
			}
		}
	}()

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

	if err := backup.DoBackupRestore(req.Backup, req.SnapshotFileName); err != nil {
		return nil, fmt.Errorf("error running backup restore [%v]", err)
	}

	backupName := ""
	if backupName, err = backupstore.GetBackupFromBackupURL(util.UnescapeURL(req.Backup)); err != nil {
		return nil, err
	}
	if err = s.FinishRestore(backupName); err != nil {
		return nil, err
	}

	logrus.Infof("Done running restore %v to %v", req.Backup, req.SnapshotFileName)
	return &Empty{}, nil
}

func (s *SyncAgentServer) BackupRestoreIncrementally(ctx context.Context,
	req *BackupRestoreIncrementallyRequest) (e *Empty, err error) {
	if err := s.PrepareRestore(req.LastRestoredBackupName); err != nil {
		logrus.Errorf("failed to prepare incremental restore: %v", err)
		return nil, err
	}

	defer func() {
		if err != nil {
			if err := s.FinishRestore(""); err != nil {
				logrus.Errorf("failed to finish incremental restore: %v", err)
				return
			}
		}
	}()

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

	logrus.Infof("Running incremental restore %v to %s with lastRestoredBackup %s", req.Backup,
		req.DeltaFileName, req.LastRestoredBackupName)
	if err := backup.DoBackupRestoreIncrementally(req.Backup, req.DeltaFileName, req.LastRestoredBackupName); err != nil {
		return nil, fmt.Errorf("error running incremental restore [%v]", err)
	}

	//Finish Incremental Restore
	if backupName, err := backupstore.GetBackupFromBackupURL(util.UnescapeURL(req.Backup)); err != nil {
		logrus.Errorf("failed to fetch backupName from backupURL: %v", err)
		return nil, err
	} else if err := s.FinishRestore(backupName); err != nil {
		logrus.Errorf("failed to finish restore(incremental): %v", err)
		return nil, err
	}
	logrus.Infof("Done running incremental restore %v to %v", req.Backup, req.DeltaFileName)
	return &Empty{}, nil
}

func (s *SyncAgentServer) RestoreStatus(ctx context.Context, req *Empty) (*RestoreStatusReply, error) {
	return &RestoreStatusReply{
		IsRestoring:  s.IsRestoring(),
		LastRestored: s.GetLastRestored(),
	}, nil
}

// The APIs BackupAdd, BackupGet, Refresh, BackupDelete implement the CRUD interface for the backup object
// The slice Backup.backupList is implemented similar to a FIFO queue.

// BackupAdd creates a new backupList object and appends to the end of the list maintained by backup object
func (b *BackupList) BackupAdd(backupID string, backup *replica.Backup) error {
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
func (b *BackupList) BackupGet(backupID string) (*replica.Backup, error) {
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
		if b.backups[index].backupStatus.BackupProgress == 100 {
			if completed == MaxBackupSize {
				break
			}
			completed++
		}
	}
	if completed == MaxBackupSize {
		//Remove all the older completed backups in the range backupList[0:index]
		for ; index >= 0; index-- {
			if b.backups[index].backupStatus.BackupProgress == 100 {
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
