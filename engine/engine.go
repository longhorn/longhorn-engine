package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/util"
)

type Engine struct {
	EngineName string
	Size       int64
	Binary     string
	Replicas   []string

	VolumeName   string
	LauncherName string
	LauncherAddr string
	ListenIP     string
	Frontend     string
	Backends     []string

	StatusLock *sync.RWMutex
	Listen     string

	pm rpc.ProcessManagerServiceServer
	ec VolumeClientService
}

func GenerateEngineName(name string) string {
	return name + "-" + uuid.NewV4().String()[:8]
}

func NewEngine(spec *rpc.EngineSpec, launcherAddr string,
	pm rpc.ProcessManagerServiceServer,
	ec VolumeClientService) *Engine {

	e := &Engine{
		EngineName: GenerateEngineName(spec.Name),
		Size:       spec.Size,
		Binary:     spec.Binary,
		Replicas:   spec.Replicas,

		VolumeName:   spec.VolumeName,
		LauncherName: spec.Name,
		LauncherAddr: launcherAddr,
		ListenIP:     spec.ListenIp,
		Frontend:     spec.Frontend,
		Backends:     spec.Backends,

		StatusLock: &sync.RWMutex{},
		Listen:     spec.Listen,

		pm: pm,
		ec: ec,
	}
	return e
}

func (e *Engine) startFrontend(frontend string) error {
	return e.ec.VolumeFrontendStart(e.GetListen(), e.LauncherName, frontend)
}

func (e *Engine) shutdownFrontend() error {
	return e.ec.VolumeFrontendShutdown(e.GetListen(), e.LauncherName)
}

func (e *Engine) ProcessStatus() *rpc.ProcessStatus {
	process, err := e.pm.ProcessGet(context.TODO(), &rpc.ProcessGetRequest{
		Name: e.EngineName,
	})
	if err != nil {
		logrus.Warnf("failed to get the related process info for engine %v: %v",
			e.EngineName, err)
		return &rpc.ProcessStatus{
			ErrorMsg: err.Error(),
		}
	}
	return process.Status
}

func (e *Engine) Start() error {
	logrus.Debugf("engine %v: prepare to start engine process at %v", e.EngineName, e.Listen)

	portCount := 0

	args := []string{
		"controller", e.VolumeName,
		"--launcher", e.LauncherAddr,
		"--launcher-id", e.LauncherName,
	}

	portArgs := []string{}

	listen := e.GetListen()
	if listen != "" {
		args = append(args, "--listen", listen)
	} else {
		if e.ListenIP == "" {
			return fmt.Errorf("neither arg listen nor arg listenIP is provided for engine %v", e.EngineName)
		}
		portArgs = append(portArgs, fmt.Sprintf("--listen,%s:", e.ListenIP))
		portCount = portCount + 1
	}

	if e.Frontend != "" {
		args = append(args, "--frontend", "socket")
	}
	for _, b := range e.Backends {
		args = append(args, "--enable-backend", b)
	}

	for _, r := range e.Replicas {
		args = append(args, "--replica", r)
	}

	req := &rpc.ProcessCreateRequest{
		Spec: &rpc.ProcessSpec{
			Name:      e.EngineName,
			Binary:    e.Binary,
			Args:      args,
			PortArgs:  portArgs,
			PortCount: int32(portCount),
		},
	}

	ret, err := e.pm.ProcessCreate(nil, req)
	if err != nil {
		return err
	}

	if listen == "" {
		listen = util.GetURL(e.ListenIP, int(ret.Status.PortStart))
		e.SetListen(listen)
	}

	logrus.Debugf("engine %v: succeed to create engine process at %v", e.EngineName, listen)

	return nil
}

func (e *Engine) GetListen() string {
	e.StatusLock.RLock()
	defer e.StatusLock.RUnlock()
	return e.Listen
}

func (e *Engine) SetListen(listen string) {
	e.StatusLock.Lock()
	defer e.StatusLock.Unlock()
	e.Listen = listen
}

func (e *Engine) Stop() (*rpc.ProcessResponse, error) {
	return e.pm.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: e.EngineName,
	})
}

func (e *Engine) WaitForRunning() error {
	return e.waitForState(types.ProcessStateRunning, false)
}

func (e *Engine) WaitForDeletion() error {
	return e.waitForState("", true)
}

func (e *Engine) waitForState(state string, waitForDeletion bool) (err error) {
	defer func() {
		if err != nil {
			logrus.Errorf("engine %v: error when wait for state %v: %v", e.EngineName, state, err)
		}
	}()
	for i := 0; i < types.WaitCount; i++ {
		resp, err := e.pm.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: e.EngineName,
		})
		if err != nil {
			errStatus, ok := status.FromError(err)
			if !ok {
				return err
			}
			if waitForDeletion && errStatus.Code() == codes.NotFound {
				return nil
			}
			return err
		}
		if resp.Status.State == state {
			return nil
		}
		logrus.Infof("engine %v: waiting for state %v", e.EngineName, state)
		time.Sleep(types.WaitInterval)
	}
	return fmt.Errorf("engine %v: failed to wait for the state %v", e.EngineName, state)
}

func (e *Engine) Log(srv rpc.EngineManagerService_EngineLogServer) error {
	return e.pm.ProcessLog(&rpc.LogRequest{
		Name: e.EngineName,
	}, srv)
}
