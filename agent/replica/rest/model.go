package rest

import (
	"github.com/rancher/go-rancher/client"
)

type BackupTarget struct {
	client.Resource
	UUID      string    `json:"uuid,omitempty"`
	Name      string    `json:"name,omitempty"`
	Type      string    `json:"type,omitempty"`
	NFSConfig NFSConfig `json:"nfsConfig,omitempty"`
}

type NFSConfig struct {
	Server       string `json:"server"`
	Share        string `json:"share"`
	MountOptions string `json:"mountOptions"`
}

func newSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})

	target := schemas.AddType("backupTarget", BackupTarget{})
	target.CollectionMethods = []string{"POST", "GET"}

	schemas.AddType("nfsConfig", NFSConfig{})

	return schemas
}
