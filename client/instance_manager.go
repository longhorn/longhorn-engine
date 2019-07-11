package client

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/types"
)

type InstanceManagerClient struct {
	Address string
}

func NewInstanceManagerClient(address string) *InstanceManagerClient {
	return &InstanceManagerClient{
		Address: address,
	}
}

func (cli *InstanceManagerClient) EnvironmentVariableSet(variables map[string]string) error {
	if variables == nil || len(variables) == 0 {
		return fmt.Errorf("failed to set environment variables: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect instance manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewInstanceManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	if _, err := client.EnvironmentVariableSet(ctx, &rpc.EnvironmentVariableSetRequset{
		Variables: variables,
	}); err != nil {
		return fmt.Errorf("failed to set environment variables: %v", err)
	}
	return nil
}
