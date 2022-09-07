package client

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/pkg/meta"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type ControllerServiceContext struct {
	cc      *grpc.ClientConn
	service ptypes.ControllerServiceClient
}

func (c ControllerServiceContext) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

type ControllerClient struct {
	serviceURL string
	ControllerServiceContext
}

func (c *ControllerClient) getControllerServiceClient() ptypes.ControllerServiceClient {
	return c.service
}

const (
	GRPCServiceTimeout = 3 * time.Minute
)

func NewControllerClient(address string) (*ControllerClient, error) {
	getControllerServiceContext := func(serviceUrl string) (ControllerServiceContext, error) {
		connection, err := grpc.Dial(serviceUrl, grpc.WithInsecure())
		if err != nil {
			return ControllerServiceContext{}, fmt.Errorf("cannot connect to ControllerService %v: %v", serviceUrl, err)
		}

		return ControllerServiceContext{
			cc:      connection,
			service: ptypes.NewControllerServiceClient(connection),
		}, nil
	}

	serviceURL := util.GetGRPCAddress(address)
	serviceContext, err := getControllerServiceContext(serviceURL)
	if err != nil {
		return nil, err
	}

	return &ControllerClient{
		serviceURL:               serviceURL,
		ControllerServiceContext: serviceContext,
	}, nil
}

func GetVolumeInfo(v *ptypes.Volume) *types.VolumeInfo {
	return &types.VolumeInfo{
		Name:                  v.Name,
		Size:                  v.Size,
		ReplicaCount:          int(v.ReplicaCount),
		Endpoint:              v.Endpoint,
		Frontend:              v.Frontend,
		FrontendState:         v.FrontendState,
		IsExpanding:           v.IsExpanding,
		LastExpansionError:    v.LastExpansionError,
		LastExpansionFailedAt: v.LastExpansionFailedAt,
	}
}

func GetControllerReplicaInfo(cr *ptypes.ControllerReplica) *types.ControllerReplicaInfo {
	return &types.ControllerReplicaInfo{
		Address: cr.Address.Address,
		Mode:    types.Mode(cr.Mode.String()),
	}
}

func GetControllerReplica(r *types.ControllerReplicaInfo) *ptypes.ControllerReplica {
	return &ptypes.ControllerReplica{
		Address: &ptypes.ReplicaAddress{
			Address: r.Address,
		},
		Mode: ptypes.ReplicaModeToGRPCReplicaMode(r.Mode),
	}
}

func GetSyncFileInfoList(list []*ptypes.SyncFileInfo) []types.SyncFileInfo {
	res := []types.SyncFileInfo{}
	for _, info := range list {
		res = append(res, GetSyncFileInfo(info))
	}
	return res
}

func GetSyncFileInfo(info *ptypes.SyncFileInfo) types.SyncFileInfo {
	return types.SyncFileInfo{
		FromFileName: info.FromFileName,
		ToFileName:   info.ToFileName,
		ActualSize:   info.ActualSize,
	}
}

func (c *ControllerClient) VolumeGet() (*types.VolumeInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	volume, err := controllerServiceClient.VolumeGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get volume %v: %v", c.serviceURL, err)
	}

	return GetVolumeInfo(volume), nil
}

func (c *ControllerClient) VolumeStart(size, currentSize int64, replicas ...string) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeStart(ctx, &ptypes.VolumeStartRequest{
		ReplicaAddresses: replicas,
		Size:             size,
		CurrentSize:      currentSize,
	}); err != nil {
		return fmt.Errorf("failed to start volume %v: %v", c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) VolumeSnapshot(name string, labels map[string]string) (string, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.VolumeSnapshot(ctx, &ptypes.VolumeSnapshotRequest{
		Name:   name,
		Labels: labels,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot %v for volume %v: %v", name, c.serviceURL, err)
	}

	return reply.Name, nil
}

func (c *ControllerClient) VolumeRevert(snapshot string) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeRevert(ctx, &ptypes.VolumeRevertRequest{
		Name: snapshot,
	}); err != nil {
		return fmt.Errorf("failed to revert to snapshot %v for volume %v: %v", snapshot, c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) VolumeExpand(size int64) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeExpand(ctx, &ptypes.VolumeExpandRequest{
		Size: size,
	}); err != nil {
		return fmt.Errorf("failed to expand to size %v for volume %v: %v", size, c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) VolumeFrontendStart(frontend string) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeFrontendStart(ctx, &ptypes.VolumeFrontendStartRequest{
		Frontend: frontend,
	}); err != nil {
		return fmt.Errorf("failed to start frontend %v for volume %v: %v", frontend, c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) VolumeFrontendShutdown() error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeFrontendShutdown(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to shutdown frontend for volume %v: %v", c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) ReplicaList() ([]*types.ControllerReplicaInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ReplicaList(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to list replicas for volume %v: %v", c.serviceURL, err)
	}

	replicas := []*types.ControllerReplicaInfo{}
	for _, cr := range reply.Replicas {
		replicas = append(replicas, GetControllerReplicaInfo(cr))
	}

	return replicas, nil
}

func (c *ControllerClient) ReplicaGet(address string) (*types.ControllerReplicaInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ReplicaGet(ctx, &ptypes.ReplicaAddress{
		Address: address,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get replica %v for volume %v: %v", address, c.serviceURL, err)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaCreate(address string, snapshotRequired bool, mode types.Mode) (*types.ControllerReplicaInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ControllerReplicaCreate(ctx, &ptypes.ControllerReplicaCreateRequest{
		Address:          address,
		SnapshotRequired: snapshotRequired,
		Mode:             ptypes.ReplicaModeToGRPCReplicaMode(mode),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create replica %v for volume %v: %v", address, c.serviceURL, err)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaDelete(address string) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.ReplicaDelete(ctx, &ptypes.ReplicaAddress{
		Address: address,
	}); err != nil {
		return fmt.Errorf("failed to delete replica %v for volume %v: %v", address, c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) ReplicaUpdate(replica *types.ControllerReplicaInfo) (*types.ControllerReplicaInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ReplicaUpdate(ctx, GetControllerReplica(replica))
	if err != nil {
		return nil, fmt.Errorf("failed to update replica %v for volume %v: %v", replica.Address, c.serviceURL, err)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaPrepareRebuild(address string) ([]types.SyncFileInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ReplicaPrepareRebuild(ctx, &ptypes.ReplicaAddress{
		Address: address,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to prepare rebuilding replica %v for volume %v: %v", address, c.serviceURL, err)
	}

	return GetSyncFileInfoList(reply.SyncFileInfoList), nil
}

func (c *ControllerClient) ReplicaVerifyRebuild(address string) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.ReplicaVerifyRebuild(ctx, &ptypes.ReplicaAddress{
		Address: address,
	}); err != nil {
		return fmt.Errorf("failed to verify rebuilt replica %v for volume %v: %v", address, c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) JournalList(limit int) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.JournalList(ctx, &ptypes.JournalListRequest{
		Limit: int64(limit),
	}); err != nil {
		return fmt.Errorf("failed to list journal for volume %v: %v", c.serviceURL, err)
	}

	return nil
}

func (c *ControllerClient) VersionDetailGet() (*meta.VersionOutput, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.VersionDetailGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get version detail: %v", err)
	}

	return &meta.VersionOutput{
		Version:                 reply.Version.Version,
		GitCommit:               reply.Version.GitCommit,
		BuildDate:               reply.Version.BuildDate,
		CLIAPIVersion:           int(reply.Version.CliAPIVersion),
		CLIAPIMinVersion:        int(reply.Version.CliAPIMinVersion),
		ControllerAPIVersion:    int(reply.Version.ControllerAPIVersion),
		ControllerAPIMinVersion: int(reply.Version.ControllerAPIMinVersion),
		DataFormatVersion:       int(reply.Version.DataFormatVersion),
		DataFormatMinVersion:    int(reply.Version.DataFormatMinVersion),
	}, nil

}

func (c *ControllerClient) Check() error {
	conn, err := grpc.Dial(c.serviceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.serviceURL, err)
	}
	defer conn.Close()
	// TODO: JM we can reuse the controller service context connection for the health requests
	healthCheckClient := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := healthCheckClient.Check(ctx, &healthpb.HealthCheckRequest{
		Service: "",
	})
	if err != nil {
		return fmt.Errorf("failed to check health for gRPC controller server %v: %v", c.serviceURL, err)
	}

	if reply.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("gRPC controller server is not serving")
	}

	return nil
}
