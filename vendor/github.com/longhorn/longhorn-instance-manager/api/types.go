package api

import "github.com/longhorn/longhorn-instance-manager/rpc"

type Process struct {
	Name      string   `json:"name"`
	Binary    string   `json:"binary"`
	Args      []string `json:"args"`
	PortCount int32    `json:"portCount"`
	PortArgs  []string `json:"portArgs"`

	ProcessStatus ProcessStatus `json:"processStatus"`
}

type ProcessStatus struct {
	State     string `json:"state"`
	ErrorMsg  string `json:"errorMsg"`
	PortStart int32  `json:"portStart"`
	PortEnd   int32  `json:"portEnd"`
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
}

func NewLogStream(stream rpc.ProcessManagerService_ProcessLogClient) *LogStream {
	return &LogStream{
		stream: stream,
	}
}

type LogStream struct {
	stream rpc.ProcessManagerService_ProcessLogClient
}

func (s *LogStream) Recv() (string, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return "", err
	}
	return resp.Line, nil
}
