package rest

import (
	"strconv"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn/replica"
)

const (
	StateOpen   = "open"
	StateClosed = "closed"
)

type Replica struct {
	client.Resource
	Dirty      bool     `json:"dirty"`
	Head       string   `json:"head"`
	Parent     string   `json:"parent"`
	Size       string   `json:"size"`
	SectorSize int64    `json:"sectorSize"`
	State      string   `json:"state"`
	Chain      []string `json:"chain"`
}

type OpenInput struct {
	client.Resource
	Size string `json:"size"`
}

type SnapshotInput struct {
	client.Resource
	Name string `json:"Name"`
}

type RemoveDiskInput struct {
	client.Resource
	Name string `json:"name"`
}

func NewReplica(context *api.ApiContext, rep *replica.Replica) *Replica {
	r := &Replica{
		Resource: client.Resource{
			Type:    "replica",
			Id:      "1",
			Actions: map[string]string{},
		},
	}

	if rep == nil {
		r.State = StateClosed
		r.Actions["open"] = context.UrlBuilder.ActionLink(r.Resource, "open")
	} else {
		r.State = StateOpen
		info := rep.Info()
		r.Dirty = info.Dirty
		r.Head = info.Head
		r.Parent = info.Parent
		r.SectorSize = info.SectorSize
		r.Size = strconv.FormatInt(info.Size, 10)
		r.Chain, _ = rep.Chain()
		r.Actions["reload"] = context.UrlBuilder.ActionLink(r.Resource, "reload")
		r.Actions["snapshot"] = context.UrlBuilder.ActionLink(r.Resource, "snapshot")
		r.Actions["close"] = context.UrlBuilder.ActionLink(r.Resource, "close")
		r.Actions["removedisk"] = context.UrlBuilder.ActionLink(r.Resource, "removedisk")
	}

	return r
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("openInput", OpenInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("removediskInput", RemoveDiskInput{})
	replica := schemas.AddType("replica", Replica{})

	replica.ResourceActions = map[string]client.Action{
		"close": client.Action{
			Output: "replica",
		},
		"open": client.Action{
			Input:  "openInput",
			Output: "replica",
		},
		"reload": client.Action{
			Output: "replica",
		},
		"snapshot": client.Action{
			Input:  "snapshotInput",
			Output: "replica",
		},
		"removedisk": client.Action{
			Input:  "removediskInput",
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
