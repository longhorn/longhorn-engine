package rest

import (
	"strconv"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-engine/replica"
)

type Replica struct {
	client.Resource
	Dirty           bool                        `json:"dirty"`
	Rebuilding      bool                        `json:"rebuilding"`
	Head            string                      `json:"head"`
	Parent          string                      `json:"parent"`
	Size            string                      `json:"size"`
	SectorSize      int64                       `json:"sectorSize,string"`
	BackingFile     string                      `json:"backingFile"`
	State           string                      `json:"state"`
	Chain           []string                    `json:"chain"`
	Disks           map[string]replica.DiskInfo `json:"disks"`
	RemainSnapshots int                         `json:"remainsnapshots"`
	RevisionCounter int64                       `json:"revisioncounter,string"`
}

type RevertInput struct {
	client.Resource
	Name    string `json:"name"`
	Created string `json:"created"`
}

type RebuildingInput struct {
	client.Resource
	Rebuilding bool `json:"rebuilding"`
}

type SnapshotInput struct {
	client.Resource
	Name        string            `json:"name"`
	UserCreated bool              `json:"usercreated"`
	Created     string            `json:"created"`
	Labels      map[string]string `json:"labels"`
}

type RemoveDiskInput struct {
	client.Resource
	Name  string `json:"name"`
	Force bool   `json:"force"`
}

type ReplaceDiskInput struct {
	client.Resource
	Target string `json:"target"`
	Source string `json:"source"`
}

type MarkDiskAsRemovedInput struct {
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
	Counter int64 `json:"counter,string"`
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
	case replica.Open:
		actions["setrebuilding"] = true
		actions["snapshot"] = true
		actions["removedisk"] = true
		actions["replacedisk"] = true
		actions["revert"] = true
		actions["markdiskasremoved"] = true
		actions["prepareremovedisk"] = true
		actions["setrevisioncounter"] = true
	case replica.Closed:
		actions["removedisk"] = true
		actions["replacedisk"] = true
		actions["revert"] = true
		actions["markdiskasremoved"] = true
		actions["prepareremovedisk"] = true
	case replica.Dirty:
		actions["setrebuilding"] = true
		actions["snapshot"] = true
		actions["removedisk"] = true
		actions["replacedisk"] = true
		actions["revert"] = true
		actions["markdiskasremoved"] = true
		actions["prepareremovedisk"] = true
	case replica.Rebuilding:
		actions["snapshot"] = true
		actions["setrebuilding"] = true
		actions["removedisk"] = true
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
	r.BackingFile = info.BackingFileName

	if rep != nil {
		r.Chain, _ = rep.DisplayChain()
		r.Disks = rep.ListDisks()
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
	schemas.AddType("rebuildingInput", RebuildingInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("removediskInput", RemoveDiskInput{})
	schemas.AddType("markDiskAsRemovedInput", MarkDiskAsRemovedInput{})
	schemas.AddType("revertInput", RevertInput{})
	schemas.AddType("prepareRemoveDiskInput", PrepareRemoveDiskInput{})
	schemas.AddType("prepareRemoveDiskOutput", PrepareRemoveDiskOutput{})
	schemas.AddType("revisionCounter", RevisionCounter{})
	schemas.AddType("replacediskInput", ReplaceDiskInput{})
	replica := schemas.AddType("replica", Replica{})

	replica.ResourceMethods = []string{"GET", "DELETE"}
	replica.ResourceActions = map[string]client.Action{
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
		"replacedisk": {
			Input:  "replacediskInput",
			Output: "replica",
		},
		"markdiskasremoved": {
			Input:  "markDiskAsRemovedInput",
			Output: "replica",
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
