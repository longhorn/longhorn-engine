package engine

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/controller/client"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/util"
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

func (e *Engine) updatePort(newPort int) (string, error) {
	controllerCli := client.NewControllerClient(e.Listen)
	if err := controllerCli.PortUpdate(newPort); err != nil {
		return "", err
	}

	addrParts := strings.Split(e.Listen, ":")
	newListen := util.GetURL(addrParts[0], newPort)

	controllerCli = client.NewControllerClient(newListen)
	for i := 0; i < SwitchWaitCount; i++ {
		if err := controllerCli.Check(); err == nil {
			break
		}
		logrus.Infof("launcher: wait for engine controller to switch to %v", newListen)
		time.Sleep(SwitchWaitInterval)
	}
	if err := controllerCli.Check(); err != nil {
		return "", errors.Wrapf(err, "test connection to %v failed", newListen)
	}

	e.Listen = newListen

	return newListen, nil
}
