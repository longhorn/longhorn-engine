package process

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/util"
)

type State string

const (
	StateStarting = State(types.ProcessStateStarting)
	StateRunning  = State(types.ProcessStateRunning)
	StateStopping = State(types.ProcessStateStopping)
	StateStopped  = State(types.ProcessStateStopped)
	StateError    = State(types.ProcessStateError)
)

type Process struct {
	Name      string
	Binary    string
	Args      []string
	PortCount int32
	PortArgs  []string

	State     State
	ErrorMsg  string
	PortStart int32
	PortEnd   int32

	lock     *sync.RWMutex
	cmd      Command
	UpdateCh chan *Process

	logger *util.LonghornWriter

	executor      Executor
	healthChecker HealthChecker
}

func (p *Process) Start() error {
	p.lock.Lock()
	cmd, err := p.executor.NewCommand(p.Binary, p.Args...)
	if err != nil {
		return err
	}
	cmd.SetOutput(p.logger)
	p.cmd = cmd
	p.lock.Unlock()

	go func() {
		if err := cmd.Run(); err != nil {
			p.lock.Lock()
			p.State = StateError
			p.ErrorMsg = err.Error()
			logrus.Infof("Process Manager: process %v error out, error msg: %v", p.Name, p.ErrorMsg)
			p.lock.Unlock()

			p.UpdateCh <- p
			return
		}
		p.lock.Lock()
		p.State = StateStopped
		logrus.Infof("Process Manager: process %v stopped", p.Name)
		p.lock.Unlock()

		p.UpdateCh <- p
	}()

	go func() {
		if p.PortStart != 0 {
			address := util.GetURL("localhost", int(p.PortStart))
			if p.healthChecker.WaitForRunning(address, p.Name) {
				p.lock.Lock()
				p.State = StateRunning
				p.lock.Unlock()
				p.UpdateCh <- p
				return
			}
			// fail to start the process, then try to stop it.
			if !p.IsStopped() {
				p.Stop()
			}
		} else {
			// Process Manager doesn't know the grpc address. directly set running state
			p.lock.Lock()
			p.State = StateRunning
			p.lock.Unlock()
			p.UpdateCh <- p
		}
		return
	}()

	return nil
}

func (p *Process) RPCResponse() *rpc.ProcessResponse {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.ErrorMsg != "" {
		logrus.Debugf("Process update: %v: state %v: Error: %v", p.Name, p.State, p.ErrorMsg)
	}
	return &rpc.ProcessResponse{
		Spec: &rpc.ProcessSpec{
			Name:      p.Name,
			Binary:    p.Binary,
			Args:      p.Args,
			PortCount: p.PortCount,
			PortArgs:  p.PortArgs,
		},

		Status: &rpc.ProcessStatus{
			State:     string(p.State),
			ErrorMsg:  p.ErrorMsg,
			PortStart: p.PortStart,
			PortEnd:   p.PortEnd,
		},
	}
}

func (p *Process) Stop() {
	needStop := false
	p.lock.Lock()
	if p.State != StateStopping && p.State != StateStopped && p.State != StateError {
		p.State = StateStopping
		needStop = true
	}
	p.lock.Unlock()

	if !needStop {
		return
	}
	p.UpdateCh <- p

	p.lock.RLock()
	cmd := p.cmd
	p.lock.RUnlock()

	go func() {
		defer func() {
			if err := p.logger.Close(); err != nil {
				logrus.Warnf("Process Manager: failed to close process %v 's logger: %v", p.Name, err)
			}
		}()

		if cmd == nil || !cmd.Started() {
			logrus.Errorf("Process Manager: cmd of %v hasn't started, no need to stop", p.Name)
			return
		}

		// no need for lock
		logrus.Debugf("Process Manager: trying to stop process %v", p.Name)
		cmd.Stop()
		for i := 0; i < types.WaitCount; i++ {
			if p.IsStopped() {
				return
			}
			logrus.Infof("wait for process %v to shutdown", p.Name)
			time.Sleep(types.WaitInterval)
		}
		logrus.Debugf("Process Manager: cannot graceful stop process %v in %v seconds, will kill the process", p.Name, types.WaitCount)
		cmd.Kill()
	}()
}

func (p *Process) IsStopped() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.State == StateStopped || p.State == StateError
}
