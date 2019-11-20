package rpc

import (
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine/pkg/engine/meta"
	journal "github.com/longhorn/sparse-tools/stats"

	"github.com/longhorn/longhorn-engine/pkg/engine/controller"
	controllerpb "github.com/longhorn/longhorn-engine/pkg/engine/controller/rpc/pb"
	"github.com/longhorn/longhorn-engine/pkg/engine/types"
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

func GetControllerGRPCServer(c *controller.Controller) *grpc.Server {
	grpcServer := grpc.NewServer()

	cs := NewControllerServer(c)
	controllerpb.RegisterControllerServiceServer(grpcServer, cs)

	healthpb.RegisterHealthServer(grpcServer, NewControllerHealthCheckServer(cs))
	reflection.Register(grpcServer)

	return grpcServer
}

func (cs *ControllerServer) replicaToControllerReplica(r *types.Replica) *controllerpb.ControllerReplica {
	cr := &controllerpb.ControllerReplica{
		Address: &controllerpb.ReplicaAddress{
			Address: r.Address,
		}}

	switch r.Mode {
	case types.WO:
		cr.Mode = controllerpb.ReplicaMode_WO
	case types.RW:
		cr.Mode = controllerpb.ReplicaMode_RW
	case types.ERR:
		cr.Mode = controllerpb.ReplicaMode_ERR
	default:
		return nil
	}

	return cr
}

func (cs *ControllerServer) getVolume() *controllerpb.Volume {
	return &controllerpb.Volume{
		Name:          cs.c.Name,
		ReplicaCount:  int32(len(cs.c.ListReplicas())),
		Endpoint:      cs.c.Endpoint(),
		Frontend:      cs.c.Frontend(),
		FrontendState: cs.c.FrontendState(),
	}
}

func (cs *ControllerServer) getControllerReplica(address string) *controllerpb.ControllerReplica {
	for _, r := range cs.c.ListReplicas() {
		if r.Address == address {
			return cs.replicaToControllerReplica(&r)
		}
	}

	return nil
}

func (cs *ControllerServer) listControllerReplica() []*controllerpb.ControllerReplica {
	csList := []*controllerpb.ControllerReplica{}
	for _, r := range cs.c.ListReplicas() {
		csList = append(csList, cs.replicaToControllerReplica(&r))
	}

	return csList
}

func (cs *ControllerServer) VolumeGet(ctx context.Context, req *empty.Empty) (*controllerpb.Volume, error) {
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeStart(ctx context.Context, req *controllerpb.VolumeStartRequest) (*controllerpb.Volume, error) {
	if err := cs.c.Start(req.ReplicaAddresses...); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeShutdown(ctx context.Context, req *empty.Empty) (*controllerpb.Volume, error) {
	if err := cs.c.Shutdown(); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeSnapshot(ctx context.Context, req *controllerpb.VolumeSnapshotRequest) (*controllerpb.VolumeSnapshotReply, error) {
	name, err := cs.c.Snapshot(req.Name, req.Labels)
	if err != nil {
		return nil, err
	}

	return &controllerpb.VolumeSnapshotReply{
		Name: name,
	}, nil
}

func (cs *ControllerServer) VolumeRevert(ctx context.Context, req *controllerpb.VolumeRevertRequest) (*controllerpb.Volume, error) {
	if err := cs.c.Revert(req.Name); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeExpand(ctx context.Context, req *controllerpb.VolumeExpandRequest) (*controllerpb.Volume, error) {
	if err := cs.c.Expand(req.Size); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendStart(ctx context.Context, req *controllerpb.VolumeFrontendStartRequest) (*controllerpb.Volume, error) {
	if err := cs.c.StartFrontend(req.Frontend); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendShutdown(ctx context.Context, req *empty.Empty) (*controllerpb.Volume, error) {
	if err := cs.c.ShutdownFrontend(); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) ReplicaList(ctx context.Context, req *empty.Empty) (*controllerpb.ReplicaListReply, error) {
	return &controllerpb.ReplicaListReply{
		Replicas: cs.listControllerReplica(),
	}, nil
}

func (cs *ControllerServer) ReplicaGet(ctx context.Context, req *controllerpb.ReplicaAddress) (*controllerpb.ControllerReplica, error) {
	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ReplicaCreate(ctx context.Context, req *controllerpb.ReplicaAddress) (*controllerpb.ControllerReplica, error) {
	if err := cs.c.AddReplica(req.Address); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ReplicaDelete(ctx context.Context, req *controllerpb.ReplicaAddress) (*empty.Empty, error) {
	if err := cs.c.RemoveReplica(req.Address); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (cs *ControllerServer) ReplicaUpdate(ctx context.Context, req *controllerpb.ControllerReplica) (*controllerpb.ControllerReplica, error) {
	if err := cs.c.SetReplicaMode(req.Address.Address, types.Mode(req.Mode.String())); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address.Address), nil
}

func (cs *ControllerServer) ReplicaPrepareRebuild(ctx context.Context, req *controllerpb.ReplicaAddress) (*controllerpb.ReplicaPrepareRebuildReply, error) {
	disks, err := cs.c.PrepareRebuildReplica(req.Address)
	if err != nil {
		return nil, err
	}

	return &controllerpb.ReplicaPrepareRebuildReply{
		Replica: cs.getControllerReplica(req.Address),
		Disks:   disks,
	}, nil
}

func (cs *ControllerServer) ReplicaVerifyRebuild(ctx context.Context, req *controllerpb.ReplicaAddress) (*controllerpb.ControllerReplica, error) {
	if err := cs.c.VerifyRebuildReplica(req.Address); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) JournalList(ctx context.Context, req *controllerpb.JournalListRequest) (*empty.Empty, error) {
	//ListJournal flushes operation journal (replica read/write, ping, etc.) accumulated since previous flush
	journal.PrintLimited(int(req.Limit))
	return &empty.Empty{}, nil
}

func (cs *ControllerServer) VersionDetailGet(ctx context.Context, req *empty.Empty) (*controllerpb.VersionDetailGetReply, error) {
	version := meta.GetVersion()
	return &controllerpb.VersionDetailGetReply{
		Version: &controllerpb.VersionOutput{
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

func (cs *ControllerServer) MetricGet(req *empty.Empty, srv controllerpb.ControllerService_MetricGetServer) error {
	cnt := 0
	for {
		latestMetics := cs.c.GetLatestMetics()
		metric := &controllerpb.Metric{}
		if latestMetics.IOPS.Read != 0 {
			metric.ReadBandwidth = latestMetics.Bandwidth.Read
			metric.ReadLatency = latestMetics.TotalLatency.Read / latestMetics.IOPS.Read
		}
		if latestMetics.IOPS.Write != 0 {
			metric.WriteBandwidth = latestMetics.Bandwidth.Write
			metric.WriteLatency = latestMetics.TotalLatency.Write / latestMetics.IOPS.Write
		}
		metric.IOPS = latestMetics.IOPS.Read + latestMetics.IOPS.Write

		if err := srv.Send(&controllerpb.MetricGetReply{
			Metric: metric,
		}); err != nil {
			logrus.Errorf("failed to send metric in gRPC streaming: %v", err)
			cnt++
		} else {
			cnt = 0
		}

		if cnt >= GRPCRetryCount {
			break
		}

		time.Sleep(1 * time.Second)
	}

	return nil
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
				logrus.Errorf("Failed to send health check result %v for gRPC controller server: %v",
					healthpb.HealthCheckResponse_SERVING, err)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.Errorf("Failed to send health check result %v for gRPC controller server: %v",
					healthpb.HealthCheckResponse_NOT_SERVING, err)
			}

		}
		time.Sleep(time.Second)
	}
}

func (cs *ControllerServer) BackupReplicaMappingCreate(ctx context.Context,
	req *controllerpb.BackupReplicaMapping) (*empty.Empty, error) {
	if err := cs.c.BackupReplicaMappingCreate(req.Backup, req.ReplicaAddress); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (cs *ControllerServer) BackupReplicaMappingGet(ctx context.Context, req *empty.Empty) (*controllerpb.BackupReplicaMap, error) {
	return &controllerpb.BackupReplicaMap{
		BackupReplicaMap: cs.c.BackupReplicaMappingGet(),
	}, nil
}

func (cs *ControllerServer) BackupReplicaMappingDelete(ctx context.Context,
	req *controllerpb.BackupReplicaMappingDeleteRequest) (*empty.Empty, error) {
	if err := cs.c.BackupReplicaMappingDelete(req.Backup); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}
