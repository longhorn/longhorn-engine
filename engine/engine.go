package engine

import (
	"context"

	"github.com/sirupsen/logrus"

	uuid "github.com/satori/go.uuid"

	"github.com/longhorn/longhorn-engine/controller/client"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
)

type Engine struct {
	EngineName string
	Size       int64
	Binary     string
	Listen     string
	Replicas   []string

	pm rpc.ProcessManagerServiceServer
}

func GenerateEngineName(name string) string {
	return name + "-" + uuid.NewV4().String()[:8]
}

func NewEngine(spec *rpc.EngineSpec, pm rpc.ProcessManagerServiceServer) *Engine {
	e := &Engine{
		EngineName: GenerateEngineName(spec.Name),
		Size:       spec.Size,
		Binary:     spec.Binary,
		Listen:     spec.Listen,
		Replicas:   spec.Replicas,

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
