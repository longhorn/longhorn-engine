package client

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine-launcher/api"
	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/types"
)

type EngineManagerClient struct {
	Address string
}

func NewEngineManagerClient(address string) *EngineManagerClient {
	return &EngineManagerClient{
		Address: address,
	}
}

func RPCToEngine(obj *rpc.EngineResponse) *api.Engine {
	return &api.Engine{
		Name:       obj.Spec.Name,
		VolumeName: obj.Spec.VolumeName,
		Binary:     obj.Spec.Binary,
		ListenAddr: obj.Spec.ListenAddr,
		Listen:     obj.Spec.Listen,
		Size:       obj.Spec.Size,
		Frontend:   obj.Spec.Frontend,
		Backends:   obj.Spec.Backends,
		Replicas:   obj.Spec.Replicas,

		Endpoint: obj.Status.Endpoint,
	}
}

func RPCToEngineList(obj *rpc.EngineListResponse) map[string]*api.Engine {
	ret := map[string]*api.Engine{}
	for name, e := range obj.Engines {
		ret[name] = RPCToEngine(e)
	}
	return ret
}

func (cli *EngineManagerClient) EngineCreate(size int64, name, volumeName, binary, listen, listenAddr, frontend string, backends, replicas []string) (*api.Engine, error) {
	if name == "" || volumeName == "" || binary == "" {
		return nil, fmt.Errorf("failed to create engine process: missing required parameter")
	}
	if listenAddr != "" {
		listenAddr = listenAddr + ":"
	}
	if listen == "" && listenAddr == "" {
		return nil, fmt.Errorf("failed to create engine process: missing required parameter")
	}
	if backends == nil || len(backends) == 0 {
		backends = []string{"tcp"}
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	e, err := client.EngineCreate(ctx, &rpc.EngineCreateRequest{
		Spec: &rpc.EngineSpec{
			Name:       name,
			VolumeName: volumeName,
			Size:       size,
			Binary:     binary,
			Listen:     listen,
			ListenAddr: listenAddr,
			Frontend:   frontend,
			Backends:   backends,
			Replicas:   replicas,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create engine process %v for volume %v: %v", name, volumeName, err)
	}

	return RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineGet(name string) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get engine: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	e, err := client.EngineGet(ctx, &rpc.EngineRequest{
		Name: name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get engine for engine %v: %v", name, err)
	}
	return RPCToEngine(e), nil
}

func (cli *EngineManagerClient) EngineList() (map[string]*api.Engine, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	es, err := client.EngineList(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to list engines: %v", err)
	}
	return RPCToEngineList(es), nil
}

func (cli *EngineManagerClient) EngineUpgrade(size int64, name, binary string, replicas []string) error {
	if name == "" || binary == "" {
		return fmt.Errorf("failed to upgrade engine process: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.EngineUpgrade(ctx, &rpc.EngineUpgradeRequest{
		Spec: &rpc.EngineSpec{
			Name:       name,
			VolumeName: "",
			Size:       size,
			Binary:     binary,
			Listen:     "",
			ListenAddr: "",
			Frontend:   "",
			Backends:   []string{},
			Replicas:   replicas,
		},
	}); err != nil {
		return fmt.Errorf("failed to upgrade engine process %v: %v", name, err)
	}

	return nil
}

func (cli *EngineManagerClient) EngineDelete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete engine: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.EngineDelete(ctx, &rpc.EngineRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to delete engine %v: %v", name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendStart(name, frontend string) error {
	if name == "" || frontend == "" {
		return fmt.Errorf("failed to start frontend: missing parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendStart(ctx, &rpc.FrontendStartRequest{
		Name:     name,
		Frontend: frontend,
	}); err != nil {
		return fmt.Errorf("failed to start frontend %v for engine %v: %v", frontend, name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendShutdown(name string) error {
	if name == "" {
		return fmt.Errorf("failed to shutdown frontend: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendShutdown(ctx, &rpc.FrontendShutdownRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to shutdown frontend for engine %v: %v", name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendStartCallback(name string) error {
	if name == "" {
		return fmt.Errorf("failed to callback for frontend start: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendStartCallback(ctx, &rpc.EngineRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to callback for engine %v frontend start: %v", name, err)
	}
	return nil
}

func (cli *EngineManagerClient) FrontendShutdownCallback(name string) error {
	if name == "" {
		return fmt.Errorf("failed to callback for frontend shutdown: missing parameter name")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to EngineManager Service %v: %v", cli.Address, err)
	}
	defer conn.Close()
	client := rpc.NewLonghornEngineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.FrontendShutdownCallback(ctx, &rpc.EngineRequest{
		Name: name,
	}); err != nil {
		return fmt.Errorf("failed to callback for engine %v frontend shutdown: %v", name, err)
	}
	return nil
}
