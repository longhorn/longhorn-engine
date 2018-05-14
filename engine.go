package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const (
	BackupListenPort = 9511
)

type Controller struct {
	volumeName string

	Binary       string
	Listen       string
	BackupListen string
	Backends     []string

	replicas     []string
	backupBinary string

	cmd *exec.Cmd
}

func NewController(binary, volumeName, listen string, backends, replicas []string) *Controller {
	return &Controller{
		Binary:     binary,
		volumeName: volumeName,
		Listen:     listen,
		Backends:   backends,
		replicas:   replicas,
	}
}

func (c *Controller) Start(launcherListen string) chan error {
	resp := make(chan error)

	exe, err := exec.LookPath(c.Binary)
	if err != nil {
		resp <- err
		return resp
	}

	exe, err = filepath.Abs(exe)
	if err != nil {
		resp <- err
		return resp
	}

	go func() {
		args := []string{
			"controller", c.volumeName,
			"--listen", c.Listen,
			"--frontend", "socket",
			"--launcher", launcherListen,
		}
		for _, b := range c.Backends {
			args = append(args, "--enable-backend", b)
		}
		for _, r := range c.replicas {
			args = append(args, "--replica", r)
		}
		cmd := exec.Command(exe, args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Pdeathsig: syscall.SIGKILL,
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		c.cmd = cmd
		resp <- cmd.Run()
	}()

	return resp
}

func (c *Controller) Stop() {
	c.cmd.Process.Signal(syscall.SIGINT)

	// wait for the controller to shutdown
	client := NewControllerClient("http://" + c.Listen)
	// we're listening as the backup
	if c.BackupListen != "" {
		client = NewControllerClient("http://" + c.BackupListen)
	}
	for i := 0; i < WaitCount; i++ {
		if err := client.TestConnection(); err != nil {
			return
		}
		logrus.Infof("launcher: wait for controller to shutdown")
		time.Sleep(WaitInterval)
	}
	logrus.Errorf("launcher: wait for controller to shutdown timed out, killing it")
	c.cmd.Process.Signal(syscall.SIGKILL)
}

func (c *Controller) BackupBinary() error {
	if c.backupBinary != "" {
		logrus.Warnf("launcher: backup binary %v already exists", c.backupBinary)
		return nil
	}
	backupBinary := c.Binary + ".bak"
	if err := cp(c.Binary, backupBinary); err != nil {
		return errors.Wrapf(err, "cannot make backup of %v", c.Binary)
	}
	c.backupBinary = backupBinary
	logrus.Infof("launcher: backup binary %v to %v", c.Binary, c.backupBinary)
	return nil
}

func (c *Controller) RemoveBackupBinary() error {
	if c.backupBinary == "" {
		logrus.Warnf("launcher: backup binary %v already removed", c.backupBinary)
		return nil
	}
	if err := rm(c.backupBinary); err != nil {
		return errors.Wrapf(err, "cannot remove backup binary %v", c.backupBinary)
	}
	logrus.Infof("launcher: removed backup binary %v", c.backupBinary)
	c.backupBinary = ""
	return nil
}

func (c *Controller) RestoreBackupBinary() error {
	if c.backupBinary == "" {
		return fmt.Errorf("cannot restore, backup binary doesn't exist")
	}
	if err := rm(c.Binary); err != nil {
		return errors.Wrapf(err, "cannot remove original binary %v", c.Binary)
	}
	if err := cp(c.backupBinary, c.Binary); err != nil {
		return errors.Wrapf(err, "cannot restore backup of %v from %v", c.Binary, c.backupBinary)
	}
	logrus.Infof("launcher: backup binary %v restored to %v", c.backupBinary, c.Binary)
	if err := c.RemoveBackupBinary(); err != nil {
		return errors.Wrapf(err, "failed to clean up backup binary %v", c.backupBinary)
	}
	return nil
}

func (c *Controller) SwitchPortToBackup() (err error) {
	client := NewControllerClient("http://" + c.Listen)
	if err := client.UpdatePort(BackupListenPort); err != nil {
		if !strings.Contains(err.Error(), "EOF") {
			return err
		}
	}
	addrs := strings.Split(c.Listen, ":")
	addr := addrs[0]
	c.BackupListen = addr + ":" + strconv.Itoa(BackupListenPort)

	client = NewControllerClient("http://" + c.BackupListen)
	if err := client.TestConnection(); err != nil {
		return errors.Wrapf(err, "test connection to %v failed", c.BackupListen)
	}
	logrus.Infof("launcher: original controller updated listen to %v", c.BackupListen)
	return nil
}

func (c *Controller) SwitchPortToOriginal() (err error) {
	if c.BackupListen == "" {
		return fmt.Errorf("backup listen wasn't set")
	}
	addrs := strings.Split(c.Listen, ":")
	port, err := strconv.Atoi(addrs[len(addrs)-1])
	if err != nil {
		return fmt.Errorf("unable to parse listen port %v", c.Listen)
	}

	client := NewControllerClient("http://" + c.BackupListen)
	if err := client.UpdatePort(port); err != nil {
		if !strings.Contains(err.Error(), "EOF") {
			return err
		}
	}

	client = NewControllerClient("http://" + c.Listen)
	if err := client.TestConnection(); err != nil {
		return errors.Wrapf(err, "test connection to %v failed", c.Listen)
	}
	c.BackupListen = ""
	logrus.Infof("launcher: controller updated listen to %v", c.Listen)
	return nil
}

func (c *Controller) PrepareUpgrade() error {
	logrus.Infof("launcher: prepare for upgrade")
	if err := c.BackupBinary(); err != nil {
		return errors.Wrap(err, "failed to backup old controller binary")
	}
	if err := c.SwitchPortToBackup(); err != nil {
		return errors.Wrapf(err, "failed to ask old controller to switch listening port %v", BackupListenPort)
	}
	logrus.Infof("launcher: preparation completed")
	return nil
}

func (c *Controller) RollbackUpgrade() error {
	logrus.Infof("launcher: rolling back upgrade")
	if err := c.SwitchPortToOriginal(); err != nil {
		return errors.Wrap(err, "failed to restore original port")
	}
	if err := c.RestoreBackupBinary(); err != nil {
		return errors.Wrap(err, "failed to restore old controller binary")
	}
	logrus.Infof("launcher: rollback completed")
	return nil
}

func (c *Controller) FinalizeUpgrade() error {
	c.RemoveBackupBinary()
	c.Stop()
	return nil
}

func cp(src, dst string) error {
	cmd := exec.Command("cp", src, dst)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "fail to copy file %v to %v", src, dst)
	}
	return nil
}

func rm(f string) error {
	if _, err := os.Stat(f); err != nil {
		// file doesn't exist
		return nil
	}
	cmd := exec.Command("rm", f)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "fail to remove file %v", f)
	}
	return nil
}
