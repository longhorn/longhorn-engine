package rpc

import (
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine/pkg/meta"
	journal "github.com/longhorn/sparse-tools/stats"

	"github.com/longhorn/longhorn-engine/pkg/controller"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

const (
	GRPCRetryCount = 5
)

type ControllerServer struct {
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
	server := grpc.NewServer(ptypes.WithIdentityValidationControllerServerInterceptor(volumeName, instanceName))
	ptypes.RegisterControllerServiceServer(server, cs)
	healthpb.RegisterHealthServer(server, NewControllerHealthCheckServer(cs))
	reflection.Register(server)
	return server
}

func (cs *ControllerServer) replicaToControllerReplica(r *types.Replica) *ptypes.ControllerReplica {
	return &ptypes.ControllerReplica{
		Address: &ptypes.ReplicaAddress{
			Address: r.Address,
		},
		Mode: ptypes.ReplicaModeToGRPCReplicaMode(r.Mode),
	}
}

func (cs *ControllerServer) syncFileInfoListToControllerFormat(list []types.SyncFileInfo) []*ptypes.SyncFileInfo {
	res := []*ptypes.SyncFileInfo{}
	for _, info := range list {
		res = append(res, cs.syncFileInfoToControllerFormat(info))
	}
	return res
}

func (cs *ControllerServer) syncFileInfoToControllerFormat(info types.SyncFileInfo) *ptypes.SyncFileInfo {
	return &ptypes.SyncFileInfo{
		FromFileName: info.FromFileName,
		ToFileName:   info.ToFileName,
		ActualSize:   info.ActualSize,
	}
}

func (cs *ControllerServer) getVolume() *ptypes.Volume {
	lastExpansionError, lastExpansionFailedAt := cs.c.GetExpansionErrorInfo()
	return &ptypes.Volume{
		Name:                      cs.c.Name,
		Size:                      cs.c.Size(),
		ReplicaCount:              int32(len(cs.c.ListReplicas())),
		Endpoint:                  cs.c.Endpoint(),
		Frontend:                  cs.c.Frontend(),
		FrontendState:             cs.c.FrontendState(),
		IsExpanding:               cs.c.IsExpanding(),
		LastExpansionError:        lastExpansionError,
		LastExpansionFailedAt:     lastExpansionFailedAt,
		UnmapMarkSnapChainRemoved: cs.c.GetUnmapMarkSnapChainRemoved(),
	}
}

func (cs *ControllerServer) getControllerReplica(address string) *ptypes.ControllerReplica {
	for _, r := range cs.c.ListReplicas() {
		if r.Address == address {
			return cs.replicaToControllerReplica(&r)
		}
	}

	return nil
}

func (cs *ControllerServer) listControllerReplica() []*ptypes.ControllerReplica {
	csList := []*ptypes.ControllerReplica{}
	for _, r := range cs.c.ListReplicas() {
		csList = append(csList, cs.replicaToControllerReplica(&r))
	}

	return csList
}

func (cs *ControllerServer) VolumeGet(ctx context.Context, req *empty.Empty) (*ptypes.Volume, error) {
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeStart(ctx context.Context, req *ptypes.VolumeStartRequest) (*ptypes.Volume, error) {
	if err := cs.c.Start(req.Size, req.CurrentSize, req.ReplicaAddresses...); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeShutdown(ctx context.Context, req *empty.Empty) (*ptypes.Volume, error) {
	if err := cs.c.Shutdown(); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeSnapshot(ctx context.Context, req *ptypes.VolumeSnapshotRequest) (*ptypes.VolumeSnapshotReply, error) {
	name, err := cs.c.Snapshot(req.Name, req.Labels)
	if err != nil {
		return nil, err
	}

	return &ptypes.VolumeSnapshotReply{
		Name: name,
	}, nil
}

func (cs *ControllerServer) VolumeRevert(ctx context.Context, req *ptypes.VolumeRevertRequest) (*ptypes.Volume, error) {
	if err := cs.c.Revert(req.Name); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeExpand(ctx context.Context, req *ptypes.VolumeExpandRequest) (*ptypes.Volume, error) {
	if err := cs.c.Expand(req.Size); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendStart(ctx context.Context, req *ptypes.VolumeFrontendStartRequest) (*ptypes.Volume, error) {
	if err := cs.c.StartFrontend(req.Frontend); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendShutdown(ctx context.Context, req *empty.Empty) (*ptypes.Volume, error) {
	if err := cs.c.ShutdownFrontend(); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *ptypes.VolumeUnmapMarkSnapChainRemovedSetRequest) (*ptypes.Volume, error) {
	if err := cs.c.SetUnmapMarkSnapChainRemoved(req.Enabled); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) ReplicaList(ctx context.Context, req *empty.Empty) (*ptypes.ReplicaListReply, error) {
	return &ptypes.ReplicaListReply{
		Replicas: cs.listControllerReplica(),
	}, nil
}

func (cs *ControllerServer) ReplicaGet(ctx context.Context, req *ptypes.ReplicaAddress) (*ptypes.ControllerReplica, error) {
	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ControllerReplicaCreate(ctx context.Context, req *ptypes.ControllerReplicaCreateRequest) (*ptypes.ControllerReplica, error) {
	if err := cs.c.AddReplica(req.Address, req.SnapshotRequired, ptypes.GRPCReplicaModeToReplicaMode(req.Mode)); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ReplicaDelete(ctx context.Context, req *ptypes.ReplicaAddress) (*empty.Empty, error) {
	if err := cs.c.RemoveReplica(req.Address); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (cs *ControllerServer) ReplicaUpdate(ctx context.Context, req *ptypes.ControllerReplica) (*ptypes.ControllerReplica, error) {
	if err := cs.c.SetReplicaMode(req.Address.Address, types.Mode(req.Mode.String())); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address.Address), nil
}

func (cs *ControllerServer) ReplicaPrepareRebuild(ctx context.Context, req *ptypes.ReplicaAddress) (*ptypes.ReplicaPrepareRebuildReply, error) {
	list, err := cs.c.PrepareRebuildReplica(req.Address, req.InstanceName)
	if err != nil {
		return nil, err
	}

	return &ptypes.ReplicaPrepareRebuildReply{
		Replica:          cs.getControllerReplica(req.Address),
		SyncFileInfoList: cs.syncFileInfoListToControllerFormat(list),
	}, nil
}

func (cs *ControllerServer) ReplicaVerifyRebuild(ctx context.Context, req *ptypes.ReplicaAddress) (*ptypes.ControllerReplica, error) {
	if err := cs.c.VerifyRebuildReplica(req.Address, req.InstanceName); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) JournalList(ctx context.Context, req *ptypes.JournalListRequest) (*empty.Empty, error) {
	//ListJournal flushes operation journal (replica read/write, ping, etc.) accumulated since previous flush
	journal.PrintLimited(int(req.Limit))
	return &empty.Empty{}, nil
}

func (cs *ControllerServer) VersionDetailGet(ctx context.Context, req *empty.Empty) (*ptypes.VersionDetailGetReply, error) {
	version := meta.GetVersion()
	return &ptypes.VersionDetailGetReply{
		Version: &ptypes.VersionOutput{
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

func (cs *ControllerServer) MetricsGet(ctx context.Context, req *empty.Empty) (*ptypes.MetricsGetReply, error) {
	return &ptypes.MetricsGetReply{
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
