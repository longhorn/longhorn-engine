package rpc

import (
	"fmt"
	"strconv"

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

func (rs *ReplicaServer) listReplicaDisks() map[string]*DiskInfo {
	disks := map[string]*DiskInfo{}
	r := rs.s.Replica()
	if r != nil {
		ds := r.ListDisks()
		for name, info := range ds {
			disks[name] = &DiskInfo{
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

func (rs *ReplicaServer) getReplica() (replica *Replica) {
	state, info := rs.s.Status()
	replica = &Replica{
		Dirty:       info.Dirty,
		Rebuilding:  info.Rebuilding,
		Head:        info.Head,
		Parent:      info.Parent,
		Size:        strconv.FormatInt(info.Size, 10),
		SectorSize:  info.SectorSize,
		BackingFile: info.BackingFileName,
		State:       string(state),
		Disks:       rs.listReplicaDisks(),
	}
	r := rs.s.Replica()
	if r != nil {
		chain, _ := r.DisplayChain()
		replica.Chain = chain
		replica.RemainSnapshots = int32(r.GetRemainSnapshotCounts())
		replica.RevisionCounter = r.GetRevisionCounter()
	}
	return replica
}

func (rs *ReplicaServer) ReplicaCreate(ctx context.Context, req *ReplicaCreateRequest) (*Replica, error) {
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

	return rs.getReplica(), nil
}

func (rs *ReplicaServer) ReplicaDelete(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaDelete not implemented")
}
func (rs *ReplicaServer) ReplicaGet(ctx context.Context, req *empty.Empty) (*Replica, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicaGet not implemented")
}

func (rs *ReplicaServer) ReplicaOpen(ctx context.Context, req *empty.Empty) (*Replica, error) {
	if err := rs.s.Open(); err != nil {
		return nil, err
	}

	return rs.getReplica(), nil
}

func (rs *ReplicaServer) ReplicaClose(ctx context.Context, req *empty.Empty) (*Replica, error) {
	if err := rs.s.Close(); err != nil {
		return nil, err
	}

	return rs.getReplica(), nil
}

func (rs *ReplicaServer) ReplicaReload(ctx context.Context, req *empty.Empty) (*Replica, error) {
	if err := rs.s.Reload(); err != nil {
		return nil, err
	}

	return rs.getReplica(), nil
}

func (rs *ReplicaServer) ReplicaRevert(ctx context.Context, req *ReplicaRevertRequest) (*Replica, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("Cannot accept empty snapshot name")
	}
	if req.Created == "" {
		return nil, fmt.Errorf("Need to specific created time")
	}

	if err := rs.s.Revert(req.Name, req.Created); err != nil {
		return nil, err
	}

	return rs.getReplica(), nil
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
