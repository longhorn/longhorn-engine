package rest

import (
	"encoding/base64"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn/controller"
	"github.com/rancher/longhorn/types"
)

type Replica struct {
	client.Resource
	Address string `json:"address"`
	Mode    string `json:"mode"`
}

type Volume struct {
	client.Resource
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
}

type VolumeCollection struct {
	client.Collection
	Data []Volume `json:"data"`
}

type ReplicaCollection struct {
	client.Collection
	Data []Replica `json:"data"`
}

type DiskCollection struct {
	client.Collection
	Data []string `json:"data"`
}

type StartInput struct {
	client.Resource
	Replicas []string `json:"replicas"`
}

type SnapshotOutput struct {
	client.Resource
}

type SnapshotInput struct {
	client.Resource
	Name string `json:"name"`
}

type RevertInput struct {
	client.Resource
	Name string `json:"name"`
}

type JournalInput struct {
	client.Resource
	Limit int `json:"limit"`
}

type PrepareRebuildOutput struct {
	client.Resource
	Disks []string `json:"disks"`
}

func NewVolume(context *api.ApiContext, name string, replicas int) *Volume {
	v := &Volume{
		Resource: client.Resource{
			Id:      EncodeID(name),
			Type:    "volume",
			Actions: map[string]string{},
		},
		Name:         name,
		ReplicaCount: replicas,
	}

	if replicas == 0 {
		v.Actions["start"] = context.UrlBuilder.ActionLink(v.Resource, "start")
	} else {
		v.Actions["shutdown"] = context.UrlBuilder.ActionLink(v.Resource, "shutdown")
		v.Actions["snapshot"] = context.UrlBuilder.ActionLink(v.Resource, "snapshot")
		v.Actions["revert"] = context.UrlBuilder.ActionLink(v.Resource, "revert")
	}
	return v
}

func NewReplica(context *api.ApiContext, address string, mode types.Mode) *Replica {
	r := &Replica{
		Resource: client.Resource{
			Id:      EncodeID(address),
			Type:    "replica",
			Actions: map[string]string{},
		},
		Address: address,
		Mode:    string(mode),
	}
	r.Actions["preparerebuild"] = context.UrlBuilder.ActionLink(r.Resource, "preparerebuild")
	r.Actions["verifyrebuild"] = context.UrlBuilder.ActionLink(r.Resource, "verifyrebuild")
	return r
}

func DencodeID(id string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func EncodeID(id string) string {
	return base64.StdEncoding.EncodeToString([]byte(id))
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("startInput", StartInput{})
	schemas.AddType("snapshotOutput", SnapshotOutput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("revertInput", RevertInput{})
	schemas.AddType("journalInput", JournalInput{})
	schemas.AddType("prepareRebuildOutput", PrepareRebuildOutput{})

	replica := schemas.AddType("replica", Replica{})
	replica.CollectionMethods = []string{"GET", "POST"}
	replica.ResourceMethods = []string{"GET", "PUT"}
	replica.ResourceActions = map[string]client.Action{
		"preparerebuild": {
			Output: "prepareRebuildOutput",
		},
	}

	f := replica.ResourceFields["address"]
	f.Create = true
	replica.ResourceFields["address"] = f

	f = replica.ResourceFields["mode"]
	f.Update = true
	replica.ResourceFields["mode"] = f

	volumes := schemas.AddType("volume", Volume{})
	volumes.ResourceActions = map[string]client.Action{
		"revert": {
			Input:  "revertInput",
			Output: "volume",
		},
		"start": {
			Input:  "startInput",
			Output: "volume",
		},
		"shutdown": {
			Output: "volume",
		},
		"snapshot": {
			Input:  "snapshotInput",
			Output: "snapshotOutput",
		},
	}

	return schemas
}

type Server struct {
	c *controller.Controller
}

func NewServer(c *controller.Controller) *Server {
	return &Server{
		c: c,
	}
}
