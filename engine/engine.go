package engine

import (
	"fmt"
	"sync"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
)

type Engine struct {
	lock *sync.RWMutex

	Name       string
	VolumeName string
	Binary     string
	ListenAddr string
	Listen     string
	Size       int64
	Frontend   string
	Backends   []string
	Replicas   []string

	backupListen string
	backupBinary string

	Endpoint string
}

func NewEngine(req *rpc.EngineCreateRequest) *Engine {
	e := &Engine{
		Name:       req.Spec.Name,
		VolumeName: req.Spec.VolumeName,
		Binary:     req.Spec.Binary,
		Size:       req.Spec.Size,
		Listen:     req.Spec.Listen,
		ListenAddr: req.Spec.ListenAddr,
		Frontend:   req.Spec.Frontend,
		Backends:   req.Spec.Backends,
		Replicas:   req.Spec.Replicas,

		lock: &sync.RWMutex{},
	}
	return e
}

func (e *Engine) RPCResponse() *rpc.EngineResponse {
	e.lock.RLock()
	defer e.lock.RUnlock()

	return &rpc.EngineResponse{
		Spec: &rpc.EngineSpec{
			Name:       e.Name,
			VolumeName: e.VolumeName,
			Binary:     e.Binary,
			Listen:     e.Listen,
			ListenAddr: e.ListenAddr,
			Size:       e.Size,
			Frontend:   e.Frontend,
			Backends:   e.Backends,
			Replicas:   e.Replicas,
		},
		Status: &rpc.EngineStatus{
			Endpoint: e.Endpoint,
		},
	}
}

func (e *Engine) startFrontendCallback() error {
	return fmt.Errorf("not implemented")
}

func (e *Engine) shutdownFrontendCallback() error {
	return fmt.Errorf("not implemented")
}
