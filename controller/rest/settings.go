package rest

import (
	"net/http"

	"github.com/rancher/go-rancher/api"
)

func (s *Server) UpdatePort(rw http.ResponseWriter, req *http.Request) error {
	var input PortInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	return s.c.UpdatePort(input.Port)
}
