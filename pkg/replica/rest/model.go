package rest

import (
	"strconv"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

type Replica struct {
	client.Resource
	Dirty                   bool                        `json:"dirty"`
	Rebuilding              bool                        `json:"rebuilding"`
	Head                    string                      `json:"head"`
	Parent                  string                      `json:"parent"`
	Size                    string                      `json:"size"`
	SectorSize              int64                       `json:"sectorSize,string"`
	BackingFile             string                      `json:"backingFile"`
	State                   string                      `json:"state"`
	Chain                   []string                    `json:"chain"`
	Disks                   map[string]replica.DiskInfo `json:"disks"`
	RemainSnapshots         int                         `json:"remainsnapshots"`
	RevisionCounter         int64                       `json:"revisioncounter,string"`
	RevisionCounterDisabled bool                        `json:"revisioncounterdisabled"`
}

func NewReplica(context *api.ApiContext, state types.ReplicaState, info replica.Info, rep *replica.Replica) *Replica {
	r := &Replica{
		Resource: client.Resource{
			Type:    "replica",
			Id:      "1",
			Actions: map[string]string{},
		},
	}

	r.State = string(state)

	actions := map[string]bool{}

	switch state {
	case types.ReplicaStateInitial:
	case types.ReplicaStateOpen:
	case types.ReplicaStateClosed:
	case types.ReplicaStateDirty:
	case types.ReplicaStateRebuilding:
	case types.ReplicaStateError:
	}

	for action := range actions {
		r.Actions[action] = context.UrlBuilder.ActionLink(r.Resource, action)
	}

	r.Dirty = info.Dirty
	r.Rebuilding = info.Rebuilding
	r.Head = info.Head
	r.Parent = info.Parent
	r.SectorSize = info.SectorSize
	r.Size = strconv.FormatInt(info.Size, 10)
	r.BackingFile = info.BackingFilePath

	if rep != nil {
		r.Chain, _ = rep.DisplayChain()
		r.Disks = rep.ListDisks()
		r.RemainSnapshots = rep.GetRemainSnapshotCounts()
		if !rep.IsRevCounterDisabled() {
			r.RevisionCounter = rep.GetRevisionCounter()
			r.RevisionCounterDisabled = false
		} else {
			r.RevisionCounterDisabled = true
		}
	}

	return r
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	replica := schemas.AddType("replica", Replica{})

	replica.ResourceMethods = []string{"GET", "DELETE"}
	replica.ResourceActions = map[string]client.Action{}

	return schemas
}

type Server struct {
	s *replica.Server
}

func NewServer(s *replica.Server) *Server {
	return &Server{
		s: s,
	}
}
