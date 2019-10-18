package rest

import (
	"encoding/base64"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

type Volume struct {
	client.Resource
	Name string `json:"name"`
}

type ReadInput struct {
	client.Resource
	Offset int64 `json:"offset,string"`
	Length int64 `json:"length,string"`
}

type ReadOutput struct {
	client.Resource
	Data string `json:"data"`
}

type WriteInput struct {
	client.Resource
	Offset int64  `json:"offset"`
	Length int    `json:"length"`
	Data   string `json:"data"`
}

type WriteOutput struct {
	client.Resource
}

func NewVolume(context *api.ApiContext, name string) *Volume {
	v := &Volume{
		Resource: client.Resource{
			Id:      EncodeID(name),
			Type:    "volume",
			Actions: map[string]string{},
		},
		Name: name,
	}

	v.Actions["readat"] = context.UrlBuilder.ActionLink(v.Resource, "readat")
	v.Actions["writeat"] = context.UrlBuilder.ActionLink(v.Resource, "writeat")
	return v
}

func DecodeID(id string) (string, error) {
	b, err := DecodeData(id)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func EncodeID(id string) string {
	return EncodeData([]byte(id))
}
func DecodeData(data string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func EncodeData(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("readInput", ReadInput{})
	schemas.AddType("readOutput", ReadOutput{})
	schemas.AddType("writeInput", WriteInput{})
	schemas.AddType("writeOutput", WriteOutput{})

	volumes := schemas.AddType("volume", Volume{})
	volumes.ResourceActions = map[string]client.Action{
		"readat": {
			Input:  "readInput",
			Output: "readOutput",
		},
		"writeat": {
			Input:  "writeInput",
			Output: "writeOutput",
		},
	}

	return schemas
}

type Server struct {
	d *Device
}

func NewServer(d *Device) *Server {
	return &Server{
		d: d,
	}
}
