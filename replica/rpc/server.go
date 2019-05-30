package rpc

import (
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/longhorn-engine/replica"
)

type ReplicaServer struct {
	s *replica.Server
}

func NewReplicaServer(s *replica.Server) *ReplicaServer {
	return &ReplicaServer{
		s: s,
	}
}

func (rs *ReplicaServer) ReplicaCreate(ctx context.Context, req *ReplicaCreateRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaCreate not implemented")
}
func (rs *ReplicaServer) ReplicaDelete(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaDelete not implemented")
}
func (rs *ReplicaServer) ReplicaGet(ctx context.Context, req *empty.Empty) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaGet not implemented")
}
func (rs *ReplicaServer) ReplicaOpen(ctx context.Context, req *empty.Empty) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaOpen not implemented")
}
func (rs *ReplicaServer) ReplicaClose(ctx context.Context, req *empty.Empty) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaClose not implemented")
}
func (rs *ReplicaServer) ReplicaReload(ctx context.Context, req *empty.Empty) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaReload not implemented")
}
func (rs *ReplicaServer) ReplicaRevert(ctx context.Context, req *ReplicaRevertRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaRevert not implemented")
}
func (rs *ReplicaServer) ReplicaSnapshot(ctx context.Context, req *ReplicaSnapshotRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaSnapshot not implemented")
}
func (rs *ReplicaServer) DiskRemove(ctx context.Context, req *DiskRemoveRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DiskRemove not implemented")
}
func (rs *ReplicaServer) DiskReplace(ctx context.Context, req *DiskReplaceRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DiskReplace not implemented")
}
func (rs *ReplicaServer) DiskPrepareRemove(ctx context.Context, req *DiskPrepareRemoveRequest) (*DiskPrepareRemoveReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DiskPrepareRemove not implemented")
}
func (rs *ReplicaServer) DiskMarkAsRemoved(ctx context.Context, req *DiskMarkAsRemovedRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DiskMarkAsRemoved not implemented")
}
func (rs *ReplicaServer) RebuildingSet(ctx context.Context, req *RebuildingSetRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RebuildingSet not implemented")
}
func (rs *ReplicaServer) RevisionCounterSet(ctx context.Context, req *RevisionCounterSetRequest) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RevisionCounterSet not implemented")
}
