package rest

import (
	"strconv"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn/replica"
)

type Replica struct {
	client.Resource
	Dirty           bool                       `json:"dirty"`
	Rebuilding      bool                       `json:"rebuilding"`
	Head            string                     `json:"head"`
	Parent          string                     `json:"parent"`
	Size            string                     `json:"size"`
	SectorSize      int64                      `json:"sectorSize"`
	State           string                     `json:"state"`
	Chain           []string                   `json:"chain"`
	Disks           []string                   `json:"disks"`
	DiskChildrenMap map[string]map[string]bool `json:"diskchildrenmap"`
	RemainSnapshots int                        `json:"remainsnapshots"`
	RevisionCounter int64                      `json:"revisioncounter"`
}

type CreateInput struct {
	client.Resource
	Size string `json:"size"`
}

type RevertInput struct {
	client.Resource
	Name string `json:"name"`
}

type RebuildingInput struct {
	client.Resource
	Rebuilding bool `json:"rebuilding"`
}

type SnapshotInput struct {
	client.Resource
	Name string `json:"Name"`
}

type RemoveDiskInput struct {
	client.Resource
	Name string `json:"name"`
}

type PrepareRemoveDiskInput struct {
	client.Resource
	Name string `json:"name"`
}

type PrepareRemoveDiskOutput struct {
	client.Resource
	Operations []replica.PrepareRemoveAction `json:"operations"`
}

type RevisionCounter struct {
	client.Resource
	Counter int64 `json:"counter"`
}

func NewReplica(context *api.ApiContext, state replica.State, info replica.Info, rep *replica.Replica) *Replica {
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
	case replica.Initial:
		actions["create"] = true
	case replica.Open:
		actions["close"] = true
		actions["setrebuilding"] = true
		actions["snapshot"] = true
		actions["reload"] = true
		actions["removedisk"] = true
		actions["revert"] = true
		actions["prepareremovedisk"] = true
		actions["setrevisioncounter"] = true
	case replica.Closed:
		actions["open"] = true
		actions["removedisk"] = true
		actions["revert"] = true
		actions["prepareremovedisk"] = true
	case replica.Dirty:
		actions["setrebuilding"] = true
		actions["close"] = true
		actions["snapshot"] = true
		actions["reload"] = true
		actions["removedisk"] = true
		actions["revert"] = true
		actions["prepareremovedisk"] = true
	case replica.Rebuilding:
		actions["snapshot"] = true
		actions["setrebuilding"] = true
		actions["close"] = true
		actions["reload"] = true
		actions["setrevisioncounter"] = true
	case replica.Error:
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

	if rep != nil {
		r.Chain, _ = rep.DisplayChain()
		r.Disks = rep.ListDisks()
		r.DiskChildrenMap = rep.ShowDiskChildrenMap()
		r.RemainSnapshots = rep.GetRemainSnapshotCounts()
		r.RevisionCounter = rep.GetRevisionCounter()
	}

	return r
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("createInput", CreateInput{})
	schemas.AddType("rebuildingInput", RebuildingInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("removediskInput", RemoveDiskInput{})
	schemas.AddType("revertInput", RevertInput{})
	schemas.AddType("prepareRemoveDiskInput", PrepareRemoveDiskInput{})
	schemas.AddType("prepareRemoveDiskOutput", PrepareRemoveDiskOutput{})
	schemas.AddType("revisionCounter", RevisionCounter{})
	replica := schemas.AddType("replica", Replica{})

	replica.ResourceMethods = []string{"GET", "DELETE"}
	replica.ResourceActions = map[string]client.Action{
		"close": {
			Output: "replica",
		},
		"open": {
			Output: "replica",
		},
		"reload": {
			Output: "replica",
		},
		"snapshot": {
			Input:  "snapshotInput",
			Output: "replica",
		},
		"removedisk": {
			Input:  "removediskInput",
			Output: "replica",
		},
		"setrebuilding": {
			Input:  "rebuildingInput",
			Output: "replica",
		},
		"create": {
			Input:  "createInput",
			Output: "replica",
		},
		"revert": {
			Input:  "revertInput",
			Output: "replica",
		},
		"prepareremovedisk": {
			Input:  "prepareRemoveDiskInput",
			Output: "prepareRemoveDiskOutput",
		},
		"setrevisioncounter": {
			Input: "revisionCounter",
		},
	}

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
