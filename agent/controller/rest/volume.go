package rest

import (
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) ListVolumes(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Listing volumes")

	apiContext := api.GetApiContext(req)

	apiContext.Write(&client.GenericCollection{
		Data: []interface{}{
			s.getVolume(apiContext),
		},
	})
	return nil
}

func (s *Server) GetVolume(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Getting volumes")

	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	if id != "1" {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	v := s.getVolume(apiContext)
	apiContext.Write(v)
	return nil
}

func (s *Server) RevertToSnapshot(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Reverting to snapshot")

	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	if id != "1" {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input revertInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	snap, err := s.getSnapshot(apiContext, input.Name)
	if err != nil {
		return err
	}

	if snap == nil {
		rw.WriteHeader(http.StatusBadRequest)
		return nil
	}

	_, err = s.controllerClient.RevertVolume(snap.Name)
	if err != nil {
		return err
	}

	return s.GetVolume(rw, req)
}

func (s *Server) getVolume(context *api.ApiContext) *volume {
	return &volume{
		Resource: client.Resource{Id: "1"},
		Name:     "volume",
	}
}
