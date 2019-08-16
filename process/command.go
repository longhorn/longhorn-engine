package process

import (
	"io"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
)

type Executor interface {
	NewCommand(name string, arg ...string) (Command, error)
}

type Command interface {
	Run() error
	SetOutput(io.Writer)
	Started() bool
	Stop()
	Kill()
}

type BinaryExecutor struct{}

func (be *BinaryExecutor) NewCommand(name string, arg ...string) (Command, error) {
	return NewBinaryCommand(name, arg...)
}

type BinaryCommand struct {
	*sync.RWMutex
	*exec.Cmd
}

func NewBinaryCommand(binary string, arg ...string) (*BinaryCommand, error) {
	var err error

	binary, err = exec.LookPath(binary)
	if err != nil {
		return nil, err
	}

	binary, err = filepath.Abs(binary)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(binary, arg...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	return &BinaryCommand{
		Cmd:     cmd,
		RWMutex: &sync.RWMutex{},
	}, nil
}

func (bc *BinaryCommand) SetOutput(writer io.Writer) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stdout = writer
	bc.Stderr = writer
}

func (bc *BinaryCommand) Started() bool {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Process != nil
}

func (bc *BinaryCommand) Stop() {
	bc.RLock()
	defer bc.RUnlock()
	if bc.Process != nil {
		bc.Process.Signal(syscall.SIGINT)
	}
}

func (bc *BinaryCommand) Kill() {
	bc.RLock()
	defer bc.RUnlock()
	if bc.Process != nil {
		bc.Process.Signal(syscall.SIGKILL)
	}
}

type TestExecutor struct{}

func (te *TestExecutor) NewCommand(name string, arg ...string) (Command, error) {
	return NewTestCommand(name, arg...)
}

type TestCommand struct {
	*sync.RWMutex

	Binary string
	Args   []string

	stopCh chan error

	started bool
	stopped bool
}

func NewTestCommand(name string, arg ...string) (*TestCommand, error) {
	return &TestCommand{
		RWMutex: &sync.RWMutex{},

		Binary: name,
		Args:   arg,

		stopCh: make(chan error),

		started: false,
		stopped: false,
	}, nil
}

func (tc *TestCommand) Run() error {
	tc.Lock()
	tc.started = true
	tc.Unlock()

	return <-tc.stopCh
}

func (tc *TestCommand) SetOutput(writer io.Writer) {
}

func (tc *TestCommand) Started() bool {
	tc.RLock()
	defer tc.RUnlock()
	return tc.started
}

func (tc *TestCommand) Stop() {
	tc.Lock()
	tc.stopped = true
	tc.Unlock()

	tc.stopCh <- nil
}

func (tc *TestCommand) Kill() {
}
