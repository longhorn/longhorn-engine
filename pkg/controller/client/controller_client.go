package client

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/longhorn-engine/pkg/meta"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
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
	VolumeName string
	ControllerServiceContext
}

func (c *ControllerClient) getControllerServiceClient() ptypes.ControllerServiceClient {
	return c.service
}

const (
	GRPCServiceTimeout = 3 * time.Minute
)

func NewControllerClient(address, volumeName, instanceName string) (*ControllerClient, error) {
	getControllerServiceContext := func(serviceUrl string) (ControllerServiceContext, error) {
		connection, err := grpc.Dial(serviceUrl, grpc.WithTransportCredentials(insecure.NewCredentials()),
			ptypes.WithIdentityValidationClientInterceptor(volumeName, instanceName))
		if err != nil {
			return ControllerServiceContext{}, errors.Wrapf(err, "cannot connect to ControllerService %v", serviceUrl)
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
		VolumeName:               volumeName,
		ControllerServiceContext: serviceContext,
	}, nil
}

func GetVolumeInfo(v *ptypes.Volume) *types.VolumeInfo {
	return &types.VolumeInfo{
		Name:                      v.Name,
		Size:                      v.Size,
		ReplicaCount:              int(v.ReplicaCount),
		Endpoint:                  v.Endpoint,
		Frontend:                  v.Frontend,
		FrontendState:             v.FrontendState,
		IsExpanding:               v.IsExpanding,
		LastExpansionError:        v.LastExpansionError,
		LastExpansionFailedAt:     v.LastExpansionFailedAt,
		UnmapMarkSnapChainRemoved: v.UnmapMarkSnapChainRemoved,
		SnapshotMaxCount:          int(v.SnapshotMaxCount),
		SnapshotMaxSize:           v.SnapshotMaxSize,
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

	volume, err := controllerServiceClient.VolumeGet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get volume %v", c.serviceURL)
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
		return errors.Wrapf(err, "failed to start volume %v", c.serviceURL)
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
		return "", errors.Wrapf(err, "failed to create snapshot %v for volume %v", name, c.serviceURL)
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
		return errors.Wrapf(err, "failed to revert to snapshot %v for volume %v", snapshot, c.serviceURL)
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
		return errors.Wrapf(err, "failed to expand to size %v for volume %v", size, c.serviceURL)
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
		return errors.Wrapf(err, "failed to start frontend %v for volume %v", frontend, c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) VolumeFrontendShutdown() error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeFrontendShutdown(ctx, &emptypb.Empty{}); err != nil {
		return errors.Wrapf(err, "failed to shutdown frontend for volume %v", c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) VolumeUnmapMarkSnapChainRemovedSet(enabled bool) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeUnmapMarkSnapChainRemovedSet(ctx, &ptypes.VolumeUnmapMarkSnapChainRemovedSetRequest{
		Enabled: enabled,
	}); err != nil {
		return errors.Wrapf(err, "failed to set UnmapMarkSnapChainRemoved to %v for volume %v", enabled, c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) VolumeSnapshotMaxCountSet(count int) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeSnapshotMaxCountSet(ctx, &ptypes.VolumeSnapshotMaxCountSetRequest{
		Count: int32(count),
	}); err != nil {
		return errors.Wrapf(err, "failed to set SnapshotMaxCount to %d for volume %s", count, c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) VolumeSnapshotMaxSizeSet(size int64) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeSnapshotMaxSizeSet(ctx, &ptypes.VolumeSnapshotMaxSizeSetRequest{
		Size: size,
	}); err != nil {
		return errors.Wrapf(err, "failed to set SnapshotMaxSize to %d for volume %s", size, c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) ReplicaList() ([]*types.ControllerReplicaInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ReplicaList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replicas for volume %v", c.serviceURL)
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
		return nil, errors.Wrapf(err, "failed to get replica %v for volume %v", address, c.serviceURL)
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
		return nil, errors.Wrapf(err, "failed to create replica %v for volume %v", address, c.serviceURL)
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
		return errors.Wrapf(err, "failed to delete replica %v for volume %v", address, c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) ReplicaUpdate(address string, mode types.Mode) (*types.ControllerReplicaInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ReplicaUpdate(ctx, GetControllerReplica(&types.ControllerReplicaInfo{
		Address: address,
		Mode:    mode,
	}))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to update replica %v for volume %v", address, c.serviceURL)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaPrepareRebuild(address, instanceName string) ([]types.SyncFileInfo, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ReplicaPrepareRebuild(ctx, &ptypes.ReplicaAddress{
		Address:      address,
		InstanceName: instanceName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to prepare rebuilding replica %v for volume %v", address, c.serviceURL)
	}

	return GetSyncFileInfoList(reply.SyncFileInfoList), nil
}

func (c *ControllerClient) ReplicaVerifyRebuild(address, instanceName string) error {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.ReplicaVerifyRebuild(ctx, &ptypes.ReplicaAddress{
		Address:      address,
		InstanceName: instanceName,
	}); err != nil {
		return errors.Wrapf(err, "failed to verify rebuilt replica %v for volume %v", address, c.serviceURL)
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
		return errors.Wrapf(err, "failed to list journal for volume %v", c.serviceURL)
	}

	return nil
}

func (c *ControllerClient) VersionDetailGet() (*meta.VersionOutput, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.VersionDetailGet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get version detail")
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
	conn, err := grpc.Dial(c.serviceURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ControllerService %v", c.serviceURL)
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
		return errors.Wrapf(err, "failed to check health for gRPC controller server %v", c.serviceURL)
	}

	if reply.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("gRPC controller server is not serving")
	}

	return nil
}

func (c *ControllerClient) MetricsGet() (*types.Metrics, error) {
	controllerServiceClient := c.getControllerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.MetricsGet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metrics for volume %v", c.serviceURL)
	}
	return &types.Metrics{
		Throughput: types.RWMetrics{
			Read:  reply.Metrics.ReadThroughput,
			Write: reply.Metrics.WriteThroughput,
		},
		TotalLatency: types.RWMetrics{
			Read:  reply.Metrics.ReadLatency,
			Write: reply.Metrics.WriteLatency,
		},
		IOPS: types.RWMetrics{
			Read:  reply.Metrics.ReadIOPS,
			Write: reply.Metrics.WriteIOPS,
		},
	}, nil
}
