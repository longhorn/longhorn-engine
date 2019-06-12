package rpc

import (
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/longhorn/longhorn-engine/controller"
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
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaList not implemented")
}
func (cs *ControllerServer) ReplicaGet(ctx context.Context, req *ReplicaAddress) (*ControllerReplica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaGet not implemented")
}
func (cs *ControllerServer) ReplicaCreate(ctx context.Context, req *ReplicaAddress) (*ControllerReplica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaCreate not implemented")
}
func (cs *ControllerServer) ReplicaDelete(ctx context.Context, req *ReplicaAddress) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaDelete not implemented")
}
func (cs *ControllerServer) ReplicaUpdate(ctx context.Context, req *ControllerReplica) (*ControllerReplica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaUpdate not implemented")
}
func (cs *ControllerServer) ReplicaPrepareRebuild(ctx context.Context, req *ReplicaAddress) (*ReplicaPrepareRebuildReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaPrepareRebuild not implemented")
}
func (cs *ControllerServer) ReplicaVerifyRebuild(ctx context.Context, req *ReplicaAddress) (*ControllerReplica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaVerifyRebuild not implemented")
}
func (cs *ControllerServer) JournalList(ctx context.Context, req *JournalListRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JournalList not implemented")
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
