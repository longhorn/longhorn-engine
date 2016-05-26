package rest

import (
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	rrest "github.com/rancher/longhorn/agent/replica/rest"
)

type snapshot struct {
	client.Resource
	Name string `json:"name"`
}

type snapshotCollection struct {
	client.Collection
	Data []snapshot `json:"data"`
}

type status struct {
	client.Resource
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

type backupInput struct {
	UUID         string             `json:"uuid,omitempty"`
	BackupTarget rrest.BackupTarget `json:"backupTarget,omitempty"`
}

type locationInput struct {
	UUID         string             `json:"uuid,omitempty"`
	Location     string             `json:"location,omitempty"`
	BackupTarget rrest.BackupTarget `json:"backupTarget,omitempty"`
}

type revertInput struct {
	Name string `json:"name,omitempty"`
}

type volume struct {
	client.Resource
	Name string `json:"name,omitempty"`
}

func newSnapshot(context *api.ApiContext, name string) *snapshot {
	snapshot := &snapshot{
		Resource: client.Resource{
			Id:      name,
			Type:    "snapshot",
			Actions: map[string]string{},
		},
		Name: name,
	}

	return snapshot
}

func newStatus(id string, state, message, resourceType string) *status {
	return &status{
		Resource: client.Resource{Id: id, Type: resourceType},
		State:    state,
		Message:  message,
	}
}

func newSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})

	schemas.AddType("revertToSnapshotInput", revertInput{})
	schemas.AddType("locationInput", locationInput{})
	schemas.AddType("backupInput", backupInput{})

	volume := schemas.AddType("volume", volume{})
	volume.CollectionMethods = []string{"GET"}
	volume.ResourceMethods = []string{"GET"}
	volume.ResourceActions = map[string]client.Action{
		"reverttosnapshot": client.Action{
			Input:  "revertToSnapshotInput",
			Output: "volume",
		},
		"restoreFromBackup": client.Action{
			Input:  "locationInput",
			Output: "restoreStatus",
		},
	}

	snapshot := schemas.AddType("snapshot", snapshot{})
	snapshot.CollectionMethods = []string{"GET", "POST"}
	snapshot.ResourceMethods = []string{"GET", "PUT", "DELETE"}
	snapshot.ResourceActions = map[string]client.Action{
		"backup": client.Action{
			Input:  "backupInput",
			Output: "backupStatus",
		},
		"removeBackup": client.Action{
			Input: "locationInput",
		},
	}

	restoreStatus := schemas.AddType("restorestatus", status{})
	restoreStatus.ResourceMethods = []string{"GET"}

	backupStatus := schemas.AddType("backupstatus", status{})
	backupStatus.ResourceMethods = []string{"GET"}

	return schemas
}
