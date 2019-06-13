package rest

import (
	"encoding/base64"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-engine/controller"
)

type Volume struct {
	client.Resource
	Name          string `json:"name"`
	ReplicaCount  int    `json:"replicaCount"`
	Endpoint      string `json:"endpoint"`
	Frontend      string `json:"frontend"`
	FrontendState string `json:"frontendState"`
	IsRestoring   bool   `json:"isRestoring"`
	LastRestored  string `json:"lastRestored"`
}

type VolumeCollection struct {
	client.Collection
	Data []Volume `json:"data"`
}

type DiskCollection struct {
	client.Collection
	Data []string `json:"data"`
}

type JournalInput struct {
	client.Resource
	Limit int `json:"limit"`
}

type PortInput struct {
	client.Resource
	Port int `json:"port"`
}

type StartFrontendInput struct {
	client.Resource
	Frontend string `json:"frontend"`
}

func NewVolume(context *api.ApiContext, name, endpoint, frontend, frontendState string, replicas int, isRestoring bool, lastRestored string) *Volume {
	v := &Volume{
		Resource: client.Resource{
			Id:      EncodeID(name),
			Type:    "volume",
			Actions: map[string]string{},
		},
		Name:          name,
		ReplicaCount:  replicas,
		Endpoint:      endpoint,
		Frontend:      frontend,
		FrontendState: frontendState,
		IsRestoring:   isRestoring,
		LastRestored:  lastRestored,
	}

	if replicas != 0 {
		v.Actions["startfrontend"] = context.UrlBuilder.ActionLink(v.Resource, "startfrontend")
		v.Actions["shutdownfrontend"] = context.UrlBuilder.ActionLink(v.Resource, "shutdownfrontend")
	}
	return v
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
	schemas.AddType("journalInput", JournalInput{})
	schemas.AddType("portInput", PortInput{})
	schemas.AddType("startFrontendInput", StartFrontendInput{})

	volumes := schemas.AddType("volume", Volume{})
	volumes.ResourceActions = map[string]client.Action{
		"startfrontend": {
			Input:  "startFrontendInput",
			Output: "volume",
		},
		"shutdownfrontend": {
			Output: "volume",
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
