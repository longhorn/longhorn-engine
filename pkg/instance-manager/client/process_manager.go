package client

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/pkg/instance-manager/api"
	"github.com/longhorn/longhorn-engine/pkg/instance-manager/types"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

type ProcessManagerClient struct {
	Address string
}

func NewProcessManagerClient(address string) *ProcessManagerClient {
	return &ProcessManagerClient{
		Address: address,
	}
}

func (cli *ProcessManagerClient) ProcessCreate(name, binary string, portCount int, args, portArgs []string) (*api.Process, error) {
	if name == "" || binary == "" {
		return nil, fmt.Errorf("failed to start process: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.ProcessCreate(ctx, &ptypes.ProcessCreateRequest{
		Spec: &ptypes.ProcessSpec{
			Name:      name,
			Binary:    binary,
			Args:      args,
			PortCount: int32(portCount),
			PortArgs:  portArgs,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start process: %v", err)
	}
	return api.RPCToProcess(p), nil
}

func (cli *ProcessManagerClient) ProcessDelete(name string) (*api.Process, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to delete process: missing required parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.ProcessDelete(ctx, &ptypes.ProcessDeleteRequest{
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to delete process %v: %v", name, err)
	}
	return api.RPCToProcess(p), nil
}

func (cli *ProcessManagerClient) ProcessGet(name string) (*api.Process, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get process: missing required parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.ProcessGet(ctx, &ptypes.ProcessGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get process %v: %v", name, err)
	}
	return api.RPCToProcess(p), nil
}

func (cli *ProcessManagerClient) ProcessList() (map[string]*api.Process, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	ps, err := client.ProcessList(ctx, &ptypes.ProcessListRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list processes: %v", err)
	}
	return api.RPCToProcessList(ps), nil
}

func (cli *ProcessManagerClient) ProcessLog(name string) (*api.LogStream, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get process: missing required parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}

	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	stream, err := client.ProcessLog(ctx, &ptypes.LogRequest{
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get process log of %v: %v", name, err)
	}
	return api.NewLogStream(conn, cancel, stream), nil
}

func (cli *ProcessManagerClient) ProcessWatch() (*api.ProcessStream, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}

	// Don't cleanup the Client here, we don't know when the user will be done with the Stream. Pass it to the wrapper
	// and allow the user to take care of it.
	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.ProcessWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open process update stream")
	}

	return api.NewProcessStream(conn, cancel, stream), nil
}

func (cli *ProcessManagerClient) ProcessReplace(name, binary string, portCount int, args, portArgs []string, terminateSignal string) (*api.Process, error) {
	if name == "" || binary == "" {
		return nil, fmt.Errorf("failed to start process: missing required parameter")
	}
	if terminateSignal != "SIGHUP" {
		return nil, fmt.Errorf("Unsupported terminate signal %v", terminateSignal)
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect process manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := ptypes.NewProcessManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	p, err := client.ProcessReplace(ctx, &ptypes.ProcessReplaceRequest{
		Spec: &ptypes.ProcessSpec{
			Name:      name,
			Binary:    binary,
			Args:      args,
			PortCount: int32(portCount),
			PortArgs:  portArgs,
		},
		TerminateSignal: terminateSignal,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start process: %v", err)
	}
	return api.RPCToProcess(p), nil
}
