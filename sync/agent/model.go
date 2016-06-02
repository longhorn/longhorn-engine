package agent

import (
	"time"

	"github.com/rancher/go-rancher/client"
)

type Process struct {
	client.Resource
	ProcessType string    `json:"processType"`
	SrcFile     string    `json:"srcFile"`
	DestFile    string    `json:"destfile"`
	Host        string    `json:"host"`
	Port        int       `json:"port"`
	ExitCode    int       `json:"exitCode"`
	Output      string    `json:"output"`
	Created     time.Time `json:"created"`
}

type ProcessCollection struct {
	client.Collection
	Data []Process `json:"data"`
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	process := schemas.AddType("process", Process{})
	process.CollectionMethods = []string{"GET", "POST"}

	for _, name := range []string{"file", "host", "port"} {
		f := process.ResourceFields[name]
		f.Create = true
		process.ResourceFields[name] = f
	}

	return schemas
}
