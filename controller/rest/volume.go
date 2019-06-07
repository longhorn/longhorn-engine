package rest

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/yasker/go-websocket-toolbox/broadcaster"
)

func (s *Server) ListVolumes(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	list, err := s.volumeList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(list)
	return nil
}

func (s *Server) volumeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	return &client.GenericCollection{
		Data: []interface{}{
			s.listVolumes(apiContext)[0],
		},
	}, nil
}

func (s *Server) processEventVolumeList(e *broadcaster.Event, r *http.Request) (interface{}, error) {
	apiContext := api.GetApiContext(r)
	list, err := s.volumeList(apiContext)
	if err != nil {
		return nil, err
	}
	return apiContext.PopulateCollection(list)
}

func (s *Server) GetVolume(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	apiContext.Write(v)
	return nil
}

func (s *Server) RevertVolume(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input RevertInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	if err := s.c.Revert(input.Name); err != nil {
		return err
	}

	return s.GetVolume(rw, req)
}

func (s *Server) listVolumes(context *api.ApiContext) []*Volume {
	return []*Volume{
		NewVolume(context, s.c.Name, s.c.Endpoint(), s.c.Frontend(), s.c.FrontendState(), len(s.c.ListReplicas()), s.c.IsRestoring(), s.c.LastRestored()),
	}
}

func (s *Server) getVolume(context *api.ApiContext, id string) *Volume {
	for _, v := range s.listVolumes(context) {
		if v.Id == id {
			return v
		}
	}
	return nil
}

func (s *Server) StartFrontend(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input StartFrontendInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	if err := s.c.StartFrontend(input.Frontend); err != nil {
		return err
	}

	return s.GetVolume(rw, req)
}

func (s *Server) ShutdownFrontend(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	if err := s.c.ShutdownFrontend(); err != nil {
		return err
	}

	return s.GetVolume(rw, req)
}

func (s *Server) PrepareRestoreVolume(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input PrepareRestoreInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	if err := s.c.PrepareRestore(input.LastRestored); err != nil {
		return err
	}

	return nil
}

func (s *Server) FinishRestoreVolume(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input FinishRestoreInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	if err := s.c.FinishRestore(input.CurrentRestored); err != nil {
		return err
	}

	return nil
}
