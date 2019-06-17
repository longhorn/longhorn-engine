package engine

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
)

type Service struct {
	lock     *sync.RWMutex
	launcher rpc.LonghornProcessLauncherServiceServer
	listen   string

	engines map[string]*Engine
}

func NewService(l rpc.LonghornProcessLauncherServiceServer, listen string) (*Service, error) {
	return &Service{
		lock:     &sync.RWMutex{},
		launcher: l,
		listen:   listen,
	}, nil
}

func (s *Service) EngineCreate(ctx context.Context, req *rpc.EngineCreateRequest) (ret *rpc.EngineResponse, err error) {
	e := NewEngine(req)
	if err := s.registerEngine(e); err != nil {
		return nil, errors.Wrapf(err, "failed to register engine %v", e.Name)
	}
	if err := s.startEngine(e); err != nil {
		return nil, errors.Wrapf(err, "failed to start engine %v", e.Name)
	}
	// Field Listen maybe updated
	if err := s.updateEngine(e); err != nil {
		return nil, errors.Wrapf(err, "failed to update engine %v", e.Name)
	}
	return e.RPCResponse(), nil
}

func (s *Service) registerEngine(e *Engine) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, exists := s.engines[e.Name]
	if exists {
		return fmt.Errorf("engine %v already exists", e.Name)
	}

	s.engines[e.Name] = e
	return nil
}

func (s *Service) updateEngine(e *Engine) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.engines[e.Name] = e
	return nil
}

func (s *Service) unregisterEngine(e *Engine) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, exists := s.engines[e.Name]
	if !exists {
		return nil
	}

	delete(s.engines, e.Name)

	return nil
}
func (s *Service) startEngine(e *Engine) error {
	portArgs := []string{}
	portCount := 0
	args := []string{
		"controller", e.VolumeName,
		"--launcher", s.listen,
		"--launcher-id", e.Name,
	}
	if e.Listen != "" {
		args = append(args, "--listen", e.Listen)
	} else {
		if e.ListenAddr == "" {
			return fmt.Errorf("neither listen to listen addr is provided for engine %v", e.Name)
		}
		portArgs = append(portArgs, "--listen,"+e.ListenAddr)
		portCount = 1
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
			Name:      e.Name,
			Binary:    e.Binary,
			Args:      args,
			PortArgs:  portArgs,
			PortCount: int32(portCount),
		},
	}
	ret, err := s.launcher.ProcessCreate(nil, req)
	if err != nil {
		return err
	}
	if portCount != 0 {
		e.Listen = e.ListenAddr + strconv.Itoa(int(ret.Status.PortStart))
	}
	return nil
}

func (s *Service) EngineDelete(ctx context.Context, req *rpc.EngineRequest) (*rpc.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.engines[req.Name]
	if e == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}

	_, err := s.launcher.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: req.Name,
	})
	if err != nil {
		return nil, err
	}

	if err := s.unregisterEngine(e); err != nil {
		return nil, err
	}

	return &rpc.Empty{}, nil
}

func (s *Service) EngineGet(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.EngineResponse, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	e := s.engines[req.Name]
	if e == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}
	return e.RPCResponse(), nil
}

func (s *Service) FrontendStartCallback(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.Empty, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	name := req.Name
	e, exists := s.engines[name]
	if !exists {
		return nil, fmt.Errorf("engine %v not found", name)
	}
	if err := e.startFrontendCallback(); err != nil {
		return nil, errors.Wrapf(err, "failed to execute startFrontendCallback for %v", name)
	}
	return &rpc.Empty{}, nil
}

func (s *Service) FrontendShutdownCallback(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.Empty, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	name := req.Name
	e, exists := s.engines[name]
	if !exists {
		return nil, fmt.Errorf("engine %v not found", name)
	}
	if err := e.shutdownFrontendCallback(); err != nil {
		return nil, errors.Wrapf(err, "failed to execute stopFrontendCallback for %v", name)
	}
	return &rpc.Empty{}, nil
}

func (s *Service) EngineUpgrade(ctx context.Context, req *rpc.EngineUpgradeRequest) (ret *rpc.Empty, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Service) FrontendStart(ctx context.Context, req *rpc.FrontendStartRequest) (ret *rpc.Empty, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Service) FrontendShutdown(ctx context.Context, req *rpc.FrontendShutdownRequest) (ret *rpc.Empty, err error) {
	return nil, fmt.Errorf("not implemented")
}
