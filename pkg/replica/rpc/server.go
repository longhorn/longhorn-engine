package rpc

import (
	"fmt"
	"strconv"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

type ReplicaServer struct {
	s *replica.Server
}

type ReplicaHealthCheckServer struct {
	rs *ReplicaServer
}

func NewReplicaServer(volumeName, instanceName string, s *replica.Server) *grpc.Server {
	rs := &ReplicaServer{s: s}
	server := grpc.NewServer(ptypes.WithIdentityValidationReplicaServerInterceptor(volumeName, instanceName))
	ptypes.RegisterReplicaServiceServer(server, rs)
	healthpb.RegisterHealthServer(server, NewReplicaHealthCheckServer(rs))
	reflection.Register(server)
	return server
}

func NewReplicaHealthCheckServer(rs *ReplicaServer) *ReplicaHealthCheckServer {
	return &ReplicaHealthCheckServer{
		rs: rs,
	}
}

func (rs *ReplicaServer) listReplicaDisks() map[string]*ptypes.DiskInfo {
	disks := map[string]*ptypes.DiskInfo{}
	r := rs.s.Replica()
	if r != nil {
		ds := r.ListDisks()
		for name, info := range ds {
			disks[name] = &ptypes.DiskInfo{
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

func (rs *ReplicaServer) getReplica() (replica *ptypes.Replica) {
	state, info := rs.s.Status()
	replica = &ptypes.Replica{
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

func (rs *ReplicaServer) ReplicaCreate(ctx context.Context, req *ptypes.ReplicaCreateRequest) (*ptypes.ReplicaCreateResponse, error) {
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

	return &ptypes.ReplicaCreateResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaDelete(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, rs.s.Delete()
}

func (rs *ReplicaServer) ReplicaGet(ctx context.Context, req *empty.Empty) (*ptypes.ReplicaGetResponse, error) {
	return &ptypes.ReplicaGetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaOpen(ctx context.Context, req *empty.Empty) (*ptypes.ReplicaOpenResponse, error) {
	if err := rs.s.Open(); err != nil {
		return nil, err
	}

	return &ptypes.ReplicaOpenResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaClose(ctx context.Context, req *empty.Empty) (*ptypes.ReplicaCloseResponse, error) {
	if err := rs.s.Close(); err != nil {
		return nil, err
	}

	return &ptypes.ReplicaCloseResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaReload(ctx context.Context, req *empty.Empty) (*ptypes.ReplicaReloadResponse, error) {
	if err := rs.s.Reload(); err != nil {
		return nil, err
	}

	return &ptypes.ReplicaReloadResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaRevert(ctx context.Context, req *ptypes.ReplicaRevertRequest) (*ptypes.ReplicaRevertResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("cannot accept empty snapshot name")
	}
	if req.Created == "" {
		return nil, fmt.Errorf("need to specific created time")
	}

	if err := rs.s.Revert(req.Name, req.Created); err != nil {
		return nil, err
	}

	return &ptypes.ReplicaRevertResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaSnapshot(ctx context.Context, req *ptypes.ReplicaSnapshotRequest) (*ptypes.ReplicaSnapshotResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("cannot accept empty snapshot name")
	}
	if req.Created == "" {
		return nil, fmt.Errorf("need to specific created time")
	}

	if err := rs.s.Snapshot(req.Name, req.UserCreated, req.Created, req.Labels); err != nil {
		return nil, err
	}

	return &ptypes.ReplicaSnapshotResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) ReplicaExpand(ctx context.Context, req *ptypes.ReplicaExpandRequest) (*ptypes.ReplicaExpandResponse, error) {
	if err := rs.s.Expand(req.Size); err != nil {
		errWithCode, ok := err.(*types.Error)
		if !ok {
			errWithCode = types.NewError(types.ErrorCodeFunctionFailedWithoutRollback,
				err.Error(), "")
		}
		return nil, status.Errorf(codes.Internal, errWithCode.ToJSONString())
	}

	return &ptypes.ReplicaExpandResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) DiskRemove(ctx context.Context, req *ptypes.DiskRemoveRequest) (*ptypes.DiskRemoveResponse, error) {
	if err := rs.s.RemoveDiffDisk(req.Name, req.Force); err != nil {
		return nil, err
	}

	return &ptypes.DiskRemoveResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) DiskReplace(ctx context.Context, req *ptypes.DiskReplaceRequest) (*ptypes.DiskReplaceResponse, error) {
	if err := rs.s.ReplaceDisk(req.Target, req.Source); err != nil {
		return nil, err
	}

	return &ptypes.DiskReplaceResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) DiskPrepareRemove(ctx context.Context, req *ptypes.DiskPrepareRemoveRequest) (*ptypes.DiskPrepareRemoveResponse, error) {
	operations, err := rs.s.PrepareRemoveDisk(req.Name)
	if err != nil {
		return nil, err
	}

	resp := &ptypes.DiskPrepareRemoveResponse{}
	for _, op := range operations {
		resp.Operations = append(resp.Operations, &ptypes.PrepareRemoveAction{
			Action: op.Action,
			Source: op.Source,
			Target: op.Target,
		})
	}
	return resp, err
}

func (rs *ReplicaServer) DiskMarkAsRemoved(ctx context.Context, req *ptypes.DiskMarkAsRemovedRequest) (*ptypes.DiskMarkAsRemovedResponse, error) {
	if err := rs.s.MarkDiskAsRemoved(req.Name); err != nil {
		return nil, err
	}

	return &ptypes.DiskMarkAsRemovedResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) RebuildingSet(ctx context.Context, req *ptypes.RebuildingSetRequest) (*ptypes.RebuildingSetResponse, error) {
	if err := rs.s.SetRebuilding(req.Rebuilding); err != nil {
		return nil, err
	}

	return &ptypes.RebuildingSetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) RevisionCounterSet(ctx context.Context, req *ptypes.RevisionCounterSetRequest) (*ptypes.RevisionCounterSetResponse, error) {
	if err := rs.s.SetRevisionCounter(req.Counter); err != nil {
		return nil, err
	}
	return &ptypes.RevisionCounterSetResponse{Replica: rs.getReplica()}, nil
}

func (rs *ReplicaServer) UnmapMarkDiskChainRemovedSet(ctx context.Context, req *ptypes.UnmapMarkDiskChainRemovedSetRequest) (*ptypes.UnmapMarkDiskChainRemovedSetResponse, error) {
	rs.s.SetUnmapMarkDiskChainRemoved(req.Enabled)
	return &ptypes.UnmapMarkDiskChainRemovedSetResponse{Replica: rs.getReplica()}, nil
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
