package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

type Controller struct {
	volumeName string

	binary   string
	listen   string
	backends []string
	replicas []string

	cmd *exec.Cmd
}

func NewController(binary, volumeName, listen string, backends, replicas []string) *Controller {
	return &Controller{
		binary:     binary,
		volumeName: volumeName,
		listen:     listen,
		backends:   backends,
		replicas:   replicas,
	}
}

func (c *Controller) Start() chan error {
	resp := make(chan error)

	exe, err := exec.LookPath(c.binary)
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
			"--listen", c.listen,
			"--frontend", "socket",
		}
		for _, b := range c.backends {
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
