package rpc

import (
	"fmt"
	"strconv"

	"github.com/longhorn/go-common-libs/generated/profilerpb"
	"github.com/longhorn/go-common-libs/profiler"
	"github.com/longhorn/types/pkg/generated/enginerpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/longhorn-engine/pkg/interceptor"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

type ReplicaServer struct {
	enginerpc.UnimplementedReplicaServiceServer
	s *replica.Server
}

type ReplicaHealthCheckServer struct {
	rs *ReplicaServer
}

func NewReplicaServer(volumeName, instanceName string, s *replica.Server) *grpc.Server {
	rs := &ReplicaServer{s: s}
	server := grpc.NewServer(interceptor.WithIdentityValidationReplicaServerInterceptor(volumeName, instanceName))
	enginerpc.RegisterReplicaServiceServer(server, rs)
	healthpb.RegisterHealthServer(server, NewReplicaHealthCheckServer(rs))
	reflection.Register(server)
	profilerpb.RegisterProfilerServer(server, profiler.NewServer(volumeName))
	return server
}

func NewReplicaHealthCheckServer(rs *ReplicaServer) *ReplicaHealthCheckServer {
	return &ReplicaHealthCheckServer{
		rs: rs,
	}
}

func (rs *ReplicaServer) listReplicaDisks() map[string]*enginerpc.DiskInfo {
	disks := map[string]*enginerpc.DiskInfo{}
	r := rs.s.Replica()
	if r != nil {
		ds := r.ListDisks()
		for name, info := range ds {
			disks[name] = &enginerpc.DiskInfo{
				Name:        info.Name,
				Parent:      info.Parent,
				Children:    info.Children,
				Removed:     info.Removed,
				UserCreated: info.UserCreated,
				Created:     info.Created,
				Size:        info.Size,
				Labels:      info.Labels,
			}
		}
	}
	return disks
}

func (rs *ReplicaServer) getReplica() (replica *enginerpc.Replica) {
	state, info := rs.s.Status()
	replica = &enginerpc.Replica{
		Dirty:       info.Dirty,
		Rebuilding:  info.Rebuilding,
		Head:        info.Head,
		Parent:      info.Parent,
		Size:        strconv.FormatInt(info.Size, 10),
		SectorSize:  info.SectorSize,
		BackingFile: info.BackingFilePath,
		State:       string(state),
		Disks:       rs.listReplicaDisks(),
	}
	r := rs.s.Replica()
	if r != nil {
		chain, _ := r.DisplayChain()
		replica.Chain = chain
		replica.RemainSnapshots = int32(r.GetRemainSnapshotCounts())
		replica.SnapshotCountUsage = int32(r.GetSnapshotCountUsage())
		replica.SnapshotSizeUsage = r.GetSnapshotSizeUsage()
		if !r.IsRevCounterDisabled() {
			replica.RevisionCounter = r.GetRevisionCounter()
			replica.RevisionCounterDisabled = false
		} else {
			replica.RevisionCounterDisabled = true
		}

		modifyTime, headFileSize := r.GetReplicaStat()
		replica.LastModifyTime = modifyTime
		replica.HeadFileSize = headFileSize

		replica.UnmapMarkDiskChainRemoved = r.GetUnmapMarkDiskChainRemoved()
	}
	return replica
}

func (rs *ReplicaServer) ReplicaCreate(ctx context.Context, req *enginerpc.ReplicaCreateRequest) (*enginerpc.ReplicaCreateResponse, error) {
	size := int64(0)
	if req.Size != "" {
		var err error
		size, err = strconv.ParseInt(req.Size, 10, 0)
		if err != nil {
			return nil, err
		}
	}

	if err := rs.s.Create(size); err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaCreateResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaDelete(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, rs.s.Delete()
}

func (rs *ReplicaServer) ReplicaGet(ctx context.Context, req *emptypb.Empty) (*enginerpc.ReplicaGetResponse, error) {
	return &enginerpc.ReplicaGetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaOpen(ctx context.Context, req *emptypb.Empty) (*enginerpc.ReplicaOpenResponse, error) {
	if err := rs.s.Open(); err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaOpenResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaClose(ctx context.Context, req *emptypb.Empty) (*enginerpc.ReplicaCloseResponse, error) {
	if err := rs.s.Close(); err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaCloseResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaReload(ctx context.Context, req *emptypb.Empty) (*enginerpc.ReplicaReloadResponse, error) {
	if err := rs.s.Reload(); err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaReloadResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaRevert(ctx context.Context, req *enginerpc.ReplicaRevertRequest) (*enginerpc.ReplicaRevertResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("cannot accept empty snapshot name")
	}
	if req.Created == "" {
		return nil, fmt.Errorf("need to specific created time")
	}

	if err := rs.s.Revert(req.Name, req.Created); err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaRevertResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaSnapshot(ctx context.Context, req *enginerpc.ReplicaSnapshotRequest) (*enginerpc.ReplicaSnapshotResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("cannot accept empty snapshot name")
	}
	if req.Created == "" {
		return nil, fmt.Errorf("need to specific created time")
	}

	if err := rs.s.Snapshot(req.Name, req.UserCreated, req.Created, req.Labels); err != nil {
		return nil, err
	}

	return &enginerpc.ReplicaSnapshotResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaExpand(ctx context.Context, req *enginerpc.ReplicaExpandRequest) (*enginerpc.ReplicaExpandResponse, error) {
	if err := rs.s.Expand(req.Size); err != nil {
		errWithCode, ok := err.(*types.Error)
		if !ok {
			errWithCode = types.NewError(types.ErrorCodeFunctionFailedWithoutRollback,
				err.Error(), "")
		}
		return nil, status.Errorf(codes.Internal, errWithCode.ToJSONString())
	}

	return &enginerpc.ReplicaExpandResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) DiskRemove(ctx context.Context, req *enginerpc.DiskRemoveRequest) (*enginerpc.DiskRemoveResponse, error) {
	if err := rs.s.RemoveDiffDisk(req.Name, req.Force); err != nil {
		return nil, err
	}

	return &enginerpc.DiskRemoveResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) DiskReplace(ctx context.Context, req *enginerpc.DiskReplaceRequest) (*enginerpc.DiskReplaceResponse, error) {
	if err := rs.s.ReplaceDisk(req.Target, req.Source); err != nil {
		return nil, err
	}

	return &enginerpc.DiskReplaceResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) DiskPrepareRemove(ctx context.Context, req *enginerpc.DiskPrepareRemoveRequest) (*enginerpc.DiskPrepareRemoveResponse, error) {
	operations, err := rs.s.PrepareRemoveDisk(req.Name)
	if err != nil {
		return nil, err
	}

	resp := &enginerpc.DiskPrepareRemoveResponse{}
	for _, op := range operations {
		resp.Operations = append(resp.Operations, &enginerpc.PrepareRemoveAction{
			Action: op.Action,
			Source: op.Source,
			Target: op.Target,
		})
	}
	return resp, err
}

func (rs *ReplicaServer) DiskMarkAsRemoved(ctx context.Context, req *enginerpc.DiskMarkAsRemovedRequest) (*enginerpc.DiskMarkAsRemovedResponse, error) {
	if err := rs.s.MarkDiskAsRemoved(req.Name); err != nil {
		return nil, err
	}

	return &enginerpc.DiskMarkAsRemovedResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) RebuildingSet(ctx context.Context, req *enginerpc.RebuildingSetRequest) (*enginerpc.RebuildingSetResponse, error) {
	if err := rs.s.SetRebuilding(req.Rebuilding); err != nil {
		return nil, err
	}

	return &enginerpc.RebuildingSetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) RevisionCounterSet(ctx context.Context, req *enginerpc.RevisionCounterSetRequest) (*enginerpc.RevisionCounterSetResponse, error) {
	if err := rs.s.SetRevisionCounter(req.Counter); err != nil {
		return nil, err
	}
	return &enginerpc.RevisionCounterSetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) UnmapMarkDiskChainRemovedSet(ctx context.Context, req *enginerpc.UnmapMarkDiskChainRemovedSetRequest) (*enginerpc.UnmapMarkDiskChainRemovedSetResponse, error) {
	rs.s.SetUnmapMarkDiskChainRemoved(req.Enabled)
	return &enginerpc.UnmapMarkDiskChainRemovedSetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) SnapshotMaxCountSet(ctx context.Context, req *enginerpc.SnapshotMaxCountSetRequest) (*enginerpc.SnapshotMaxCountSetResponse, error) {
	rs.s.SetSnapshotMaxCount(int(req.Count))
	return &enginerpc.SnapshotMaxCountSetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) SnapshotMaxSizeSet(ctx context.Context, req *enginerpc.SnapshotMaxSizeSetRequest) (*enginerpc.SnapshotMaxSizeSetResponse, error) {
	rs.s.SetSnapshotMaxSize(req.Size)
	return &enginerpc.SnapshotMaxSizeSetResponse{Replica: rs.getReplica()}, nil
}

func (hc *ReplicaHealthCheckServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.rs.s != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (hc *ReplicaHealthCheckServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	if hc.rs.s != nil {
		return ws.Send(&healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		})
	}

	return ws.Send(&healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	})
}
