package client

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-instance-manager/api"
	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/types"
)

type EngineManagerClient struct {
	Address string
}

func NewEngineManagerClient(address string) *EngineManagerClient {
	return &EngineManagerClient{
		Address: address,
	}
}

func (cli *EngineManagerClient) EngineCreate(size int64, uuid, name, volumeName, binary, listen, listenIP, frontend string, backends, replicas []string) (*api.Engine, error) {
	if uuid == "" || name == "" || volumeName == "" || binary == "" {
		return nil, fmt.Errorf("failed to call gRPC EngineCreate: missing required parameter")
	}
	if listen == "" && listenIP == "" {
		return nil, fmt.Errorf("failed to call gRPC EngineCreate: missing required parameter")
	}
	if backends == nil || len(backends) == 0 {
		backends = []string{"tcp"}
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	e, err := client.EngineCreate(ctx, &rpc.EngineCreateRequest{
		Spec: &rpc.EngineSpec{
			Uuid:       uuid,
			Name:       name,
			VolumeName: volumeName,
			Size:       size,
			Binary:     binary,
			Listen:     listen,
			ListenIp:   listenIP,
			Frontend:   frontend,
			Backends:   backends,
			Replicas:   replicas,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call gRPC EngineCreate for volume %v: %v", volumeName, err)
	}

	return api.RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineGet(name string) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to call gRPC EngineGet: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	e, err := client.EngineGet(ctx, &rpc.EngineRequest{
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call gRPC EngineGet for engine %v: %v", name, err)
	}
	return api.RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineList() (map[string]*api.Engine, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	es, err := client.EngineList(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to call gRPC EngineList: %v", err)
	}
	return api.RPCToEngineList(es), nil
}

func (cli *EngineManagerClient) EngineUpgrade(size int64, name, binary string, replicas []string) (*api.Engine, error) {
	if name == "" || binary == "" {
		return nil, fmt.Errorf("failed to call gRPC EngineUpgrade: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	e, err := client.EngineUpgrade(ctx, &rpc.EngineUpgradeRequest{
		Spec: &rpc.EngineSpec{
			Name:       name,
			VolumeName: "",
			Size:       size,
			Binary:     binary,
			Listen:     "",
			ListenIp:   "",
			Frontend:   "",
			Backends:   []string{},
			Replicas:   replicas,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call gRPC EngineUpgrade for engine %v: %v", name, err)
	}

	return api.RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineDelete(name string) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to call gRPC EngineDelete: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	e, err := client.EngineDelete(ctx, &rpc.EngineRequest{
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call gRPC EngineDelete for engine %v: %v", name, err)
	}
	return api.RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineLog(volumeName string) (*api.LogStream, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("failed to delete engine: missing parameter volumeName")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}

	client := rpc.NewEngineManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	stream, err := client.EngineLog(ctx, &rpc.LogRequest{
		Name: volumeName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get engine log of volume %v: %v", volumeName, err)
	}

	return api.NewLogStream(conn, cancel, stream), nil
}

func (cli *EngineManagerClient) EngineWatch() (*api.EngineStream, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}

	// Don't cleanup the Client here, we don't know when the user will be done with the Stream. Pass it to the wrapper
	// and allow the user to take care of it.
	client := rpc.NewEngineManagerServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.EngineWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to start engine update watch")
	}

	return api.NewEngineStream(conn, cancel, stream), nil
}

func (cli *EngineManagerClient) FrontendStart(name, frontend string) error {
	if name == "" || frontend == "" {
		return fmt.Errorf("failed to call gRPC FrontendStart: missing parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendStart(ctx, &rpc.FrontendStartRequest{
		Name:     name,
		Frontend: frontend,
	}); err != nil {
		return fmt.Errorf("failed to call gRPC FrontendStart for engine %v: %v", name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendShutdown(name string) error {
	if name == "" {
		return fmt.Errorf("failed to call gRPC FrontendShutdown: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendShutdown(ctx, &rpc.EngineRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to call gRPC FrontendShutdown for engine %v: %v", name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendStartCallback(name string) error {
	if name == "" {
		return fmt.Errorf("failed to call gRPC FrontendStartCallback: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendStartCallback(ctx, &rpc.EngineRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to call gRPC FrontendStartCallback for engine %v: %v", name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendShutdownCallback(name string) error {
	if name == "" {
		return fmt.Errorf("failed to call gRPC FrontendShutdownCallback: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendShutdownCallback(ctx, &rpc.EngineRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to call gRPC FrontendShutdownCallback for engine %v: %v", name, err)
	}
	return nil
}
