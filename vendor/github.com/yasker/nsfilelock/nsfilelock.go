package nsfilelock

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	MountNamespaceFD   = "mnt"
	MaximumMessageSize = 255

	SuccessResponse = "NSFileLock LOCKED"
)

var (
	DefaultTimeout = 15 * time.Second
)

type NSFileLock struct {
	Namespace string
	FilePath  string
	Timeout   time.Duration

	done chan struct{}
}

func NewLock(ns string, filepath string) *NSFileLock {
	return &NSFileLock{
		Namespace: ns,
		FilePath:  filepath,
		Timeout:   DefaultTimeout,
	}
}

func NewLockWithTimeout(ns string, filepath string, timeout time.Duration) *NSFileLock {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	return &NSFileLock{
		Namespace: ns,
		FilePath:  filepath,
		Timeout:   timeout,
	}
}

func (l *NSFileLock) Lock() error {
	resp := ""

	l.done = make(chan struct{})
	result := make(chan string)
	timeout := make(chan struct{})

	cmdline := []string{}
	if l.Namespace != "" {
		nsFD := filepath.Join(l.Namespace, MountNamespaceFD)
		if _, err := os.Stat(nsFD); err != nil {
			return fmt.Errorf("Invalid namespace fd %s: %v", nsFD, err)
		}
		cmdline = []string{"nsenter", "--mount=" + nsFD}
	}

	lockCmd := fmt.Sprintf("\"\"exec 314>%s && flock 314 && echo %s && exec sleep 65535\"\"",
		l.FilePath, SuccessResponse)

	cmdline = append(cmdline, "bash", "-c", lockCmd)
	cmd := exec.Command(cmdline[0], cmdline[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	go func() {
		var err error
		buf := make([]byte, MaximumMessageSize)
		n := 0
		for n == 0 {
			n, err = stdout.Read(buf)
			if err != nil {
				result <- err.Error()
			}
		}
		result <- strings.Trim(string(buf), "\n\x00")
	}()

	go func() {
		var err error
		buf := make([]byte, MaximumMessageSize)
		n := 0
		for n == 0 {
			n, err = stderr.Read(buf)
			if err != nil {
				result <- err.Error()
			}
		}
		result <- strings.Trim(string(buf), "\n\x00")
	}()

	go func() {
		time.Sleep(l.Timeout)
		timeout <- struct{}{}
	}()

	select {
	case resp = <-result:
		if resp != SuccessResponse {
			return fmt.Errorf("Failed to lock, response: %s, expected %s", resp, SuccessResponse)
		}
	case <-timeout:
		syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
		return fmt.Errorf("Timeout waiting for lock")
	}

	// Wait for unlock
	go func() {
		select {
		case <-l.done:
			syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
			return
		}
	}()
	return nil
}

func (l *NSFileLock) Unlock() {
	l.done <- struct{}{}
}
