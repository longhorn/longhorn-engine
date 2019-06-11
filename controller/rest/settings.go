package rest

import (
	"net/http"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-engine/meta"
)

func (s *Server) UpdatePort(rw http.ResponseWriter, req *http.Request) error {
	var input PortInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	return s.c.UpdatePort(input.Port)
}

func (s *Server) GetVersionDetails(rw http.ResponseWriter, req *http.Request) error {
	version := meta.GetVersion()
	apiContext := api.GetApiContext(req)
	apiContext.Write(&Version{
		client.Resource{
			Id:   "details",
			Type: "version",
		},
		version,
	})
	return nil
}
