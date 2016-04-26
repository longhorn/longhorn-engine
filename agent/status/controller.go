package status

import (
	"net/http"

	"github.com/rancher/longhorn/client"
)

type ControllerStatus struct {
	controller *client.ControllerClient
}

func NewControllerStatus() *ControllerStatus {

	controllerClient := client.NewControllerClient("http://localhost:9501/v1")
	return &ControllerStatus{
		controller: controllerClient,
	}
}

func (s *ControllerStatus) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	_, err := s.controller.GetVolume()
	if err != nil {
		writeError(rw, err)
		return
	}

	writeOK(rw)
}
