package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const (
	BackupListenPort = 9511
)

type Controller struct {
	volumeName string

	Binary   string
	Listen   string
	Backends []string

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

func (c *Controller) Start() chan error {
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
	c.backupBinary = ""
	logrus.Infof("launcher: removed backup binary %v", c.backupBinary)
	return nil
}

func (c *Controller) RestoreBackupBinary() error {
	if c.backupBinary == "" {
		return fmt.Errorf("cannot restore, backup binary doesn't exist")
	}
	if err := cp(c.backupBinary, c.Binary); err != nil {
		return errors.Wrapf(err, "cannot restore backup of %v from %v", c.Binary, c.backupBinary)
	}
	if err := c.RemoveBackupBinary(); err != nil {
		return errors.Wrapf(err, "failed to clean up backup binary %v", c.backupBinary)
	}
	logrus.Infof("launcher: backup binary %v restored", c.backupBinary)
	return nil
}

func (c *Controller) SwitchToBackupListenPort() error {
	//addrs := strings.Split(c.listen, ":")
	//addr := addrs[0]
	//backupListen := addr + ":" + strconv.Itoa(BackupListenPort)

	client := NewControllerClient("http://" + c.Listen)
	if err := client.UpdatePort(BackupListenPort); err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil
		}
		return err
	}
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
	cmd := exec.Command("rm", f)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "fail to remove file %v", f)
	}
	return nil
}
