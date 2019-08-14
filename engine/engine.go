package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	uuid "github.com/satori/go.uuid"

	"github.com/longhorn/longhorn-engine/controller/client"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/util"
)

type Engine struct {
	EngineName string
	Size       int64
	Binary     string
	Listen     string
	Replicas   []string

	VolumeName   string
	LauncherName string
	LauncherAddr string
	ListenIP     string
	Frontend     string
	Backends     []string

	pm rpc.ProcessManagerServiceServer
}

func GenerateEngineName(name string) string {
	return name + "-" + uuid.NewV4().String()[:8]
}

func NewEngine(spec *rpc.EngineSpec, launcherAddr string, pm rpc.ProcessManagerServiceServer) *Engine {
	e := &Engine{
		EngineName: GenerateEngineName(spec.Name),
		Size:       spec.Size,
		Binary:     spec.Binary,
		Listen:     spec.Listen,
		Replicas:   spec.Replicas,

		VolumeName:   spec.VolumeName,
		LauncherName: spec.Name,
		LauncherAddr: launcherAddr,
		ListenIP:     spec.ListenIp,
		Frontend:     spec.Frontend,
		Backends:     spec.Backends,

		pm: pm,
	}
	return e
}

func (e *Engine) startFrontend(frontend string) error {
	controllerCli := client.NewControllerClient(e.Listen)

	if err := controllerCli.VolumeFrontendStart(frontend); err != nil {
		return err
	}

	return nil
}

func (e *Engine) shutdownFrontend() error {
	controllerCli := client.NewControllerClient(e.Listen)

	if err := controllerCli.VolumeFrontendShutdown(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) ProcessStatus() *rpc.ProcessStatus {
	process, err := e.pm.ProcessGet(context.TODO(), &rpc.ProcessGetRequest{
		Name: e.EngineName,
	})
	if err != nil {
		logrus.Warnf("failed to get the related process info for engine %v: %v",
			e.EngineName, err)
		return &rpc.ProcessStatus{
			State:    types.ProcessStateNotFound,
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

	if e.Listen != "" {
		args = append(args, "--listen", e.Listen)
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

	if e.Listen == "" {
		e.Listen = util.GetURL(e.ListenIP, int(ret.Status.PortStart))
	}

	logrus.Debugf("engine %v: succeed to create engine process at %v", e.EngineName, e.Listen)

	return nil
}

func (e *Engine) Stop() (*rpc.ProcessResponse, error) {
	return e.pm.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: e.EngineName,
	})
}

func (e *Engine) WaitForState(state string) error {
	done := false
	for i := 0; i < types.WaitCount; i++ {
		resp, err := e.pm.ProcessGet(nil, &rpc.ProcessGetRequest{
			Name: e.EngineName,
		})
		if err != nil {
			logrus.Errorf("engine %v: error when wait for state %v: %v", e.EngineName, state, err)
			return err
		}
		if resp.Status.State == state {
			done = true
			break
		}
		logrus.Infof("engine %v: waiting for state %v", e.EngineName, state)
		time.Sleep(types.WaitInterval)
	}
	if !done {
		return fmt.Errorf("engine %v: failed to wait for the state %v", e.EngineName, state)
	}
	return nil
}

func (e *Engine) Log(srv rpc.EngineManagerService_EngineLogServer) error {
	return e.pm.ProcessLog(&rpc.LogRequest{
		Name: e.EngineName,
	}, srv)
}
