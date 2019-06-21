package client

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
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

func RPCToProcessStatus(obj *rpc.ProcessStatus) api.ProcessStatus {
	return api.ProcessStatus{
		State:     obj.State,
		ErrorMsg:  obj.ErrorMsg,
		PortStart: obj.PortStart,
		PortEnd:   obj.PortEnd,
	}
}

func RPCToEngine(obj *rpc.EngineResponse) *api.Engine {
	return &api.Engine{
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
	}
}

func RPCToEngineList(obj *rpc.EngineListResponse) map[string]*api.Engine {
	ret := map[string]*api.Engine{}
	for name, e := range obj.Engines {
		ret[name] = RPCToEngine(e)
	}
	return ret
}

func (cli *EngineManagerClient) EngineCreate(size int64, name, volumeName, binary, listen, listenIP, frontend string, backends, replicas []string) (*api.Engine, error) {
	if name == "" || volumeName == "" || binary == "" {
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

	return RPCToEngine(e), nil
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
	return RPCToEngine(e), nil
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
	return RPCToEngineList(es), nil
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

	return RPCToEngine(e), nil
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
	return RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineLog(volumeName string) (*api.LogStream, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("failed to delete engine: missing parameter volumeName")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewEngineManagerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	stream, err := client.EngineLog(ctx, &rpc.LogRequest{
		Name: volumeName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get engine log of volume %v: %v", volumeName, err)
	}

	return api.NewLogStream(stream), nil
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
