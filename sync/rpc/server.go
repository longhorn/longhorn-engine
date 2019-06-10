package rpc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/docker/docker/pkg/reexec"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

type SyncAgentServer struct {
	sync.Mutex

	currentPort     int
	startPort       int
	endPort         int
	processesByPort map[int]string
}

func NewSyncAgentServer(startPort, endPort int) *SyncAgentServer {
	return &SyncAgentServer{
		currentPort:     startPort,
		startPort:       startPort,
		endPort:         endPort,
		processesByPort: map[int]string{},
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

func (*SyncAgentServer) BackupCreate(ctx context.Context, req *BackupCreateRequest) (*BackupCreateReply, error) {
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

	buf := new(bytes.Buffer)

	cmdline := []string{"sbackup", "create", req.SnapshotFileName,
		"--dest", req.BackupTarget,
		"--volume", req.VolumeName}
	for _, label := range req.Labels {
		cmdline = append(cmdline, "--label", label)
	}
	cmd := reexec.Command(cmdline...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = buf
	cmd.Stderr = os.Stdout
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	logrus.Infof("Running %s %v", cmd.Path, cmd.Args)
	err = cmd.Wait()

	reply := &BackupCreateReply{
		Backup: buf.String(),
	}
	fmt.Fprintf(os.Stdout, reply.Backup)
	if err != nil {
		logrus.Infof("Error running %s %v: %v", "sbackup", cmd.Args, err)
		return nil, err
	}

	logrus.Infof("Done running %s %v, returns %v", "sbackup", cmd.Args, reply.Backup)
	return reply, nil
}

func (s *SyncAgentServer) BackupGetStatus(ctx context.Context, req *BackupProgressRequest) (*BackupProgressReply, error) {
	//not implemented
	return nil, fmt.Errorf("not implemented")
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

func (*SyncAgentServer) BackupRestore(ctx context.Context, req *BackupRestoreRequest) (*Empty, error) {
	cmd := reexec.Command("sbackup", "restore", req.Backup, "--to", req.SnapshotFileName)
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

func (*SyncAgentServer) BackupRestoreIncrementally(ctx context.Context, req *BackupRestoreIncrementallyRequest) (*Empty, error) {
	cmd := reexec.Command("sbackup", "restore", req.Backup, "--incrementally", "--to", req.DeltaFileName, "--last-restored", req.LastRestoredBackupName)
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
