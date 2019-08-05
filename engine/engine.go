package engine

import (
	uuid "github.com/satori/go.uuid"

	"github.com/longhorn/longhorn-engine/controller/client"

	"github.com/longhorn/longhorn-instance-manager/rpc"
)

type Engine struct {
	EngineName string
	Size       int64
	Binary     string
	Listen     string
	Replicas   []string
}

func GenerateEngineName(name string) string {
	return name + "-" + uuid.NewV4().String()[:8]
}

func NewEngine(spec *rpc.EngineSpec) *Engine {
	e := &Engine{
		EngineName: GenerateEngineName(spec.Name),
		Size:       spec.Size,
		Binary:     spec.Binary,
		Listen:     spec.Listen,
		Replicas:   spec.Replicas,
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
