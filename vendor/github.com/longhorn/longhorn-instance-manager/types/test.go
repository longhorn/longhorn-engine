package types

import (
	"io"
	"sync"
)

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
