package rpc

import (
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	journal "github.com/longhorn/sparse-tools/stats"

	"github.com/longhorn/longhorn-engine/controller"
	"github.com/longhorn/longhorn-engine/types"
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

func (cs *ControllerServer) replicaToControllerReplica(r *types.Replica) *ControllerReplica {
	cr := &ControllerReplica{
		Address: &ReplicaAddress{
			Address: r.Address,
		}}

	switch r.Mode {
	case types.WO:
		cr.Mode = ReplicaMode_WO
	case types.RW:
		cr.Mode = ReplicaMode_RW
	case types.ERR:
		cr.Mode = ReplicaMode_ERR
	default:
		return nil
	}

	return cr
}

func (cs *ControllerServer) getVolume() *Volume {
	return &Volume{
		Name:          cs.c.Name,
		ReplicaCount:  int32(len(cs.c.ListReplicas())),
		Endpoint:      cs.c.Endpoint(),
		Frontend:      cs.c.Frontend(),
		FrontendState: cs.c.FrontendState(),
		IsRestoring:   cs.c.IsRestoring(),
		LastRestored:  cs.c.LastRestored(),
	}
}

func (cs *ControllerServer) getControllerReplica(address string) *ControllerReplica {
	for _, r := range cs.c.ListReplicas() {
		if r.Address == address {
			return cs.replicaToControllerReplica(&r)
		}
	}

	return nil
}

func (cs *ControllerServer) listControllerReplica() []*ControllerReplica {
	csList := []*ControllerReplica{}
	for _, r := range cs.c.ListReplicas() {
		csList = append(csList, cs.replicaToControllerReplica(&r))
	}

	return csList
}

func (cs *ControllerServer) VolumeGet(ctx context.Context, req *empty.Empty) (*Volume, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VolumeGet not implemented")
}

func (cs *ControllerServer) VolumeStart(ctx context.Context, req *VolumeStartRequest) (*Volume, error) {
	if err := cs.c.Start(req.ReplicaAddresses...); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeShutdown(ctx context.Context, req *empty.Empty) (*Volume, error) {
	if err := cs.c.Shutdown(); err != nil {
		return nil, err
	}
	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeSnapshot(ctx context.Context, req *VolumeSnapshotRequest) (*VolumeSnapshotReply, error) {
	name, err := cs.c.Snapshot(req.Name, req.Labels)
	if err != nil {
		return nil, err
	}

	return &VolumeSnapshotReply{
		Name: name,
	}, nil
}

func (cs *ControllerServer) VolumeRevert(ctx context.Context, req *VolumeRevertRequest) (*Volume, error) {
	if err := cs.c.Revert(req.Name); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFrontendStart(ctx context.Context, req *VolumeFrontendStartRequest) (*Volume, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VolumeFrontendStart not implemented")
}
func (cs *ControllerServer) VolumeFrontendShutdown(ctx context.Context, req *empty.Empty) (*Volume, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VolumeFrontendShutdown not implemented")
}

func (cs *ControllerServer) VolumePrepareRestore(ctx context.Context, req *VolumePrepareRestoreRequest) (*Volume, error) {
	if err := cs.c.PrepareRestore(req.LastRestored); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) VolumeFinishRestore(ctx context.Context, req *VolumeFinishRestoreRequest) (*Volume, error) {
	if err := cs.c.FinishRestore(req.CurrentRestored); err != nil {
		return nil, err
	}

	return cs.getVolume(), nil
}

func (cs *ControllerServer) ReplicaList(ctx context.Context, req *empty.Empty) (*ReplicaListReply, error) {
	return &ReplicaListReply{
		Replicas: cs.listControllerReplica(),
	}, nil
}

func (cs *ControllerServer) ReplicaGet(ctx context.Context, req *ReplicaAddress) (*ControllerReplica, error) {
	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ReplicaCreate(ctx context.Context, req *ReplicaAddress) (*ControllerReplica, error) {
	if err := cs.c.AddReplica(req.Address); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) ReplicaDelete(ctx context.Context, req *ReplicaAddress) (*empty.Empty, error) {
	if err := cs.c.RemoveReplica(req.Address); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (cs *ControllerServer) ReplicaUpdate(ctx context.Context, req *ControllerReplica) (*ControllerReplica, error) {
	if err := cs.c.SetReplicaMode(req.Address.Address, types.Mode(req.Mode.String())); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address.Address), nil
}

func (cs *ControllerServer) ReplicaPrepareRebuild(ctx context.Context, req *ReplicaAddress) (*ReplicaPrepareRebuildReply, error) {
	disks, err := cs.c.PrepareRebuildReplica(req.Address)
	if err != nil {
		return nil, err
	}

	return &ReplicaPrepareRebuildReply{
		Replica: cs.getControllerReplica(req.Address),
		Disks:   disks,
	}, nil
}

func (cs *ControllerServer) ReplicaVerifyRebuild(ctx context.Context, req *ReplicaAddress) (*ControllerReplica, error) {
	if err := cs.c.VerifyRebuildReplica(req.Address); err != nil {
		return nil, err
	}

	return cs.getControllerReplica(req.Address), nil
}

func (cs *ControllerServer) JournalList(ctx context.Context, req *JournalListRequest) (*empty.Empty, error) {
	//ListJournal flushes operation journal (replica read/write, ping, etc.) accumulated since previous flush
	journal.PrintLimited(int(req.Limit))
	return &empty.Empty{}, nil
}

func (cs *ControllerServer) PortUpdate(ctx context.Context, req *PortUpdateRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PortUpdate not implemented")
}
func (cs *ControllerServer) VersionDetailGet(ctx context.Context, req *empty.Empty) (*VersionDetailGetReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VersionDetailGet not implemented")
}
func (cs *ControllerServer) MetricGet(req *empty.Empty, srv ControllerService_MetricGetServer) error {
	return status.Errorf(codes.Unimplemented, "method MetricGet not implemented")
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
