package api

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/rpc"
)

type Process struct {
	Name      string   `json:"name"`
	Binary    string   `json:"binary"`
	Args      []string `json:"args"`
	PortCount int32    `json:"portCount"`
	PortArgs  []string `json:"portArgs"`

	ProcessStatus ProcessStatus `json:"processStatus"`

	Deleted bool `json:"deleted"`
}

func RPCToProcess(obj *rpc.ProcessResponse) *Process {
	return &Process{
		Name:          obj.Spec.Name,
		Binary:        obj.Spec.Binary,
		Args:          obj.Spec.Args,
		PortCount:     obj.Spec.PortCount,
		PortArgs:      obj.Spec.PortArgs,
		ProcessStatus: RPCToProcessStatus(obj.Status),
	}
}

func RPCToProcessList(obj *rpc.ProcessListResponse) map[string]*Process {
	ret := map[string]*Process{}
	for name, p := range obj.Processes {
		ret[name] = RPCToProcess(p)
	}
	return ret
}

type ProcessStatus struct {
	State     string `json:"state"`
	ErrorMsg  string `json:"errorMsg"`
	PortStart int32  `json:"portStart"`
	PortEnd   int32  `json:"portEnd"`
}

func RPCToProcessStatus(obj *rpc.ProcessStatus) ProcessStatus {
	return ProcessStatus{
		State:     obj.State,
		ErrorMsg:  obj.ErrorMsg,
		PortStart: obj.PortStart,
		PortEnd:   obj.PortEnd,
	}
}

type Engine struct {
	Name       string   `json:"name"`
	VolumeName string   `json:"volumeName"`
	Binary     string   `json:"binary"`
	ListenIP   string   `json:"listenIP"`
	Listen     string   `json:"listen"`
	Size       int64    `json:"size"`
	Frontend   string   `json:"frontend"`
	Backends   []string `json:"backends"`
	Replicas   []string `json:"replicas"`

	ProcessStatus ProcessStatus `json:"processStatus"`
	Endpoint      string        `json:"endpoint"`

	Deleted bool `json:"deleted"`
}

func RPCToEngine(obj *rpc.EngineResponse) *Engine {
	return &Engine{
		Name:       obj.Spec.Name,
		VolumeName: obj.Spec.VolumeName,
		Binary:     obj.Spec.Binary,
		ListenIP:   obj.Spec.ListenIp,
		Listen:     obj.Spec.Listen,
		Size:       obj.Spec.Size,
		Frontend:   obj.Spec.Frontend,
		Backends:   obj.Spec.Backends,
		Replicas:   obj.Spec.Replicas,

		ProcessStatus: RPCToProcessStatus(obj.Status.ProcessStatus),
		Endpoint:      obj.Status.Endpoint,

		Deleted: obj.Deleted,
	}
}

func RPCToEngineList(obj *rpc.EngineListResponse) map[string]*Engine {
	ret := map[string]*Engine{}
	for name, e := range obj.Engines {
		ret[name] = RPCToEngine(e)
	}
	return ret
}

type EngineStream struct {
	conn      *grpc.ClientConn
	ctxCancel context.CancelFunc
	stream    rpc.EngineManagerService_EngineWatchClient
}

func NewEngineStream(conn *grpc.ClientConn, ctxCancel context.CancelFunc, stream rpc.EngineManagerService_EngineWatchClient) *EngineStream {
	return &EngineStream{
		conn,
		ctxCancel,
		stream,
	}
}

func (s *EngineStream) Recv() (*Engine, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return RPCToEngine(resp), nil
}

func (s *EngineStream) Close() error {
	s.ctxCancel()
	if err := s.conn.Close(); err != nil {
		return errors.Wrapf(err, "error closing engine watcher gRPC connection")
	}
	return nil
}

type ProcessStream struct {
	conn      *grpc.ClientConn
	ctxCancel context.CancelFunc
	stream    rpc.ProcessManagerService_ProcessWatchClient
}

func NewProcessStream(conn *grpc.ClientConn, ctxCancel context.CancelFunc, stream rpc.ProcessManagerService_ProcessWatchClient) *ProcessStream {
	return &ProcessStream{
		conn,
		ctxCancel,
		stream,
	}
}

func (s *ProcessStream) Close() error {
	s.ctxCancel()
	if err := s.conn.Close(); err != nil {
		return errors.Wrapf(err, "error closing process watcher gRPC connection")
	}
	return nil
}

func (s *ProcessStream) Recv() (*Process, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return RPCToProcess(resp), nil
}

func NewLogStream(conn *grpc.ClientConn, ctxCancel context.CancelFunc, stream rpc.ProcessManagerService_ProcessLogClient) *LogStream {
	return &LogStream{
		conn,
		ctxCancel,
		stream,
	}
}

type LogStream struct {
	conn      *grpc.ClientConn
	ctxCancel context.CancelFunc
	stream    rpc.ProcessManagerService_ProcessLogClient
}

func (s *LogStream) Close() error {
	s.ctxCancel()
	if err := s.conn.Close(); err != nil {
		return errors.Wrapf(err, "error closing logs gRPC connection")
	}
	return nil
}

func (s *LogStream) Recv() (string, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return "", err
	}
	return resp.Line, nil
}
