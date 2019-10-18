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

type MockExecutor struct{}

func (me *MockExecutor) NewCommand(name string, arg ...string) (Command, error) {
	return NewMockCommand(name, arg...)
}

type MockCommand struct {
	*sync.RWMutex

	Binary string
	Args   []string

	stopCh chan error

	started bool
	stopped bool
}

func NewMockCommand(name string, arg ...string) (*MockCommand, error) {
	return &MockCommand{
		RWMutex: &sync.RWMutex{},

		Binary: name,
		Args:   arg,

		stopCh: make(chan error),

		started: false,
		stopped: false,
	}, nil
}

func (mc *MockCommand) Run() error {
	mc.Lock()
	mc.started = true
	mc.Unlock()

	return <-mc.stopCh
}

func (mc *MockCommand) SetOutput(writer io.Writer) {
}

func (mc *MockCommand) Started() bool {
	mc.RLock()
	defer mc.RUnlock()
	return mc.started
}

func (mc *MockCommand) Stop() {
	mc.Lock()
	mc.stopped = true
	mc.Unlock()

	mc.stopCh <- nil
}

func (mc *MockCommand) Kill() {
}
