package rpc

import (
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/go-common-libs/profiler"
	journal "github.com/longhorn/sparse-tools/stats"
	"github.com/longhorn/types/pkg/generated/enginerpc"
	"github.com/longhorn/types/pkg/generated/profilerrpc"

	"github.com/longhorn/longhorn-engine/pkg/controller"
	"github.com/longhorn/longhorn-engine/pkg/interceptor"
	"github.com/longhorn/longhorn-engine/pkg/meta"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

const (
	GRPCRetryCount = 5
)

type ControllerServer struct {
	enginerpc.UnimplementedControllerServiceServer
	c *controller.Controller
}

type ControllerHealthCheckServer struct {
	cs *ControllerServer
}

func NewControllerServer(c *controller.Controller) *ControllerServer {
	return &ControllerServer{
		c: c,
	}
}

func NewControllerHealthCheckServer(cs *ControllerServer) *ControllerHealthCheckServer {
	return &ControllerHealthCheckServer{
		cs: cs,
	}
}

func GetControllerGRPCServer(volumeName, instanceName string, c *controller.Controller) *grpc.Server {
	cs := NewControllerServer(c)
	server := grpc.NewServer(interceptor.WithIdentityValidationControllerServerInterceptor(volumeName, instanceName))
	enginerpc.RegisterControllerServiceServer(server, cs)
	healthpb.RegisterHealthServer(server, NewControllerHealthCheckServer(cs))
	reflection.Register(server)
	profilerrpc.RegisterProfilerServer(server, profiler.NewServer(volumeName))
	return server
}

func (cs *ControllerServer) replicaToControllerReplica(r *types.Replica) *enginerpc.ControllerReplica {
	return &enginerpc.ControllerReplica{
		Address: &enginerpc.ReplicaAddress{
			Address: r.Address,
		},
		Mode: types.ReplicaModeToGRPCReplicaMode(r.Mode),
	}
}

func (cs *ControllerServer) syncFileInfoListToControllerFormat(list []types.SyncFileInfo) []*enginerpc.SyncFileInfo {
	res := []*enginerpc.SyncFileInfo{}
	for _, info := range list {
		res = append(res, cs.syncFileInfoToControllerFormat(info))
	}
	return res
}

func (cs *ControllerServer) syncFileInfoToControllerFormat(info types.SyncFileInfo) *enginerpc.SyncFileInfo {
	return &enginerpc.SyncFileInfo{
		FromFileName: info.FromFileName,
		ToFileName:   info.ToFileName,
		ActualSize:   info.ActualSize,
	}
}

func (cs *ControllerServer) getVolume() *enginerpc.Volume {
	lastExpansionError, lastExpansionFailedAt := cs.c.GetExpansionErrorInfo()
	return &enginerpc.Volume{
		Name:                      cs.c.VolumeName,
		Size:                      cs.c.Size(),
		ReplicaCount:              int32(len(cs.c.ListReplicas())),
		Endpoint:                  cs.c.Endpoint(),
		Frontend:                  cs.c.Frontend(),
		FrontendState:             cs.c.FrontendState(),
		IsExpanding:               cs.c.IsExpanding(),
		LastExpansionError:        lastExpansionError,
		LastExpansionFailedAt:     lastExpansionFailedAt,
		UnmapMarkSnapChainRemoved: cs.c.GetUnmapMarkSnapChainRemoved(),
		SnapshotMaxCount:          int32(cs.c.GetSnapshotMaxCount()),
		SnapshotMaxSize:           cs.c.GetSnapshotMaxSize(),
	}
}

func (cs *ControllerServer) getControllerReplica(address string) *enginerpc.ControllerReplica {
	for _, r := range cs.c.ListReplicas() {
		if r.Address == address {
			return cs.replicaToControllerReplica(&r)
		}
	}

	return nil
}

func (cs *ControllerServer) listControllerReplica() []*enginerpc.ControllerReplica {
	csList := []*enginerpc.ControllerReplica{}
	for _, r := range cs.c.ListReplicas() {
		csList = append(csList, cs.replicaToControllerReplica(&r))
	}

	return csList
}

func (cs *ControllerServer) VolumeGet(ctx context.Context, req *emptypb.Empty) (*enginerpc.Volume, error) {
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeStart(ctx context.Context, req *enginerpc.VolumeStartRequest) (*enginerpc.Volume, error) {
	if err := cs.c.Start(req.Size, req.CurrentSize, req.ReplicaAddresses...); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeShutdown(ctx context.Context, req *emptypb.Empty) (*enginerpc.Volume, error) {
	if err := cs.c.Shutdown(); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeSnapshot(ctx context.Context, req *enginerpc.VolumeSnapshotRequest) (*enginerpc.VolumeSnapshotReply, error) {
	name, err := cs.c.Snapshot(req.Name, req.Labels, req.FreezeFilesystem)
	if err != nil {
		return nil, err
	}

	return &enginerpc.VolumeSnapshotReply{
		Name: name,
	}, nil
}

func (cs *ControllerServer) VolumeRevert(ctx context.Context, req *enginerpc.VolumeRevertRequest) (*enginerpc.Volume, error) {
	if err := cs.c.Revert(req.Name); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeExpand(ctx context.Context, req *enginerpc.VolumeExpandRequest) (*enginerpc.Volume, error) {
	if err := cs.c.Expand(req.Size); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendStart(ctx context.Context, req *enginerpc.VolumeFrontendStartRequest) (*enginerpc.Volume, error) {
	if err := cs.c.StartFrontend(req.Frontend); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendShutdown(ctx context.Context, req *emptypb.Empty) (*enginerpc.Volume, error) {
	if err := cs.c.ShutdownFrontend(); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *enginerpc.VolumeUnmapMarkSnapChainRemovedSetRequest) (*enginerpc.Volume, error) {
	if err := cs.c.SetUnmapMarkSnapChainRemoved(req.Enabled); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeSnapshotMaxCountSet(ctx context.Context, req *enginerpc.VolumeSnapshotMaxCountSetRequest) (*enginerpc.Volume, error) {
	if err := cs.c.SetSnapshotMaxCount(int(req.Count)); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeSnapshotMaxSizeSet(ctx context.Context, req *enginerpc.VolumeSnapshotMaxSizeSetRequest) (*enginerpc.Volume, error) {
	if err := cs.c.SetSnapshotMaxSize(req.Size); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) ReplicaList(ctx context.Context, req *emptypb.Empty) (*enginerpc.ReplicaListReply, error) {
	return &enginerpc.ReplicaListReply{
		Replicas: cs.listControllerReplica(),
	}, nil
}

func (cs *ControllerServer) ReplicaGet(ctx context.Context, req *enginerpc.ReplicaAddress) (*enginerpc.ControllerReplica, error) {
	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ControllerReplicaCreate(ctx context.Context, req *enginerpc.ControllerReplicaCreateRequest) (*enginerpc.ControllerReplica, error) {
	if err := cs.c.AddReplica(req.Address, req.SnapshotRequired, types.GRPCReplicaModeToReplicaMode(req.Mode)); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ReplicaDelete(ctx context.Context, req *enginerpc.ReplicaAddress) (*emptypb.Empty, error) {
	if err := cs.c.RemoveReplica(req.Address); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (cs *ControllerServer) ReplicaUpdate(ctx context.Context, req *enginerpc.ControllerReplica) (*enginerpc.ControllerReplica, error) {
	if err := cs.c.SetReplicaMode(req.Address.Address, types.Mode(req.Mode.String())); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address.Address), nil
}

func (cs *ControllerServer) ReplicaPrepareRebuild(ctx context.Context, req *enginerpc.ReplicaAddress) (*enginerpc.ReplicaPrepareRebuildReply, error) {
	list, err := cs.c.PrepareRebuildReplica(req.Address, req.InstanceName)
	if err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaPrepareRebuildReply{
		Replica:          cs.getControllerReplica(req.Address),
		SyncFileInfoList: cs.syncFileInfoListToControllerFormat(list),
	}, nil
}

func (cs *ControllerServer) ReplicaVerifyRebuild(ctx context.Context, req *enginerpc.ReplicaAddress) (*enginerpc.ControllerReplica, error) {
	if err := cs.c.VerifyRebuildReplica(req.Address, req.InstanceName); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) JournalList(ctx context.Context, req *enginerpc.JournalListRequest) (*emptypb.Empty, error) {
	//ListJournal flushes operation journal (replica read/write, ping, etc.) accumulated since previous flush
	journal.PrintLimited(int(req.Limit))
	return &emptypb.Empty{}, nil
}

func (cs *ControllerServer) VersionDetailGet(ctx context.Context, req *emptypb.Empty) (*enginerpc.VersionDetailGetReply, error) {
	version := meta.GetVersion()
	return &enginerpc.VersionDetailGetReply{
		Version: &enginerpc.VersionOutput{
			Version:                 version.Version,
			GitCommit:               version.GitCommit,
			BuildDate:               version.BuildDate,
			CliAPIVersion:           int64(version.CLIAPIVersion),
			CliAPIMinVersion:        int64(version.CLIAPIMinVersion),
			ControllerAPIVersion:    int64(version.ControllerAPIVersion),
			ControllerAPIMinVersion: int64(version.ControllerAPIMinVersion),
			DataFormatVersion:       int64(version.DataFormatVersion),
			DataFormatMinVersion:    int64(version.DataFormatMinVersion),
		},
	}, nil
}

func (cs *ControllerServer) MetricsGet(ctx context.Context, req *emptypb.Empty) (*enginerpc.MetricsGetReply, error) {
	return &enginerpc.MetricsGetReply{
		Metrics: cs.c.GetLatestMetics(),
	}, nil
}

func (hc *ControllerHealthCheckServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.cs.c != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (hc *ControllerHealthCheckServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.cs.c != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for gRPC controller server",
					healthpb.HealthCheckResponse_SERVING)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for gRPC controller server",
					healthpb.HealthCheckResponse_NOT_SERVING)
			}

		}
		time.Sleep(time.Second)
	}
}
