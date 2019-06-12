package rest

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/yasker/go-websocket-toolbox/broadcaster"
)

func (s *Server) ListReplicas(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	list, err := s.replicaList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(list)
	return nil
}

func (s *Server) replicaList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	resp := &client.GenericCollection{}
	for _, r := range s.c.ListReplicas() {
		resp.Data = append(resp.Data, NewReplica(apiContext, r.Address, r.Mode))
	}

	resp.ResourceType = "replica"
	resp.CreateTypes = map[string]string{
		"replica": apiContext.UrlBuilder.Collection("replica"),
	}
	return resp, nil
}

func (s *Server) processEventReplicaList(e *broadcaster.Event, r *http.Request) (interface{}, error) {
	apiContext := api.GetApiContext(r)
	list, err := s.replicaList(apiContext)
	if err != nil {
		return nil, err
	}
	return apiContext.PopulateCollection(list)
}

func (s *Server) GetReplica(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	vars := mux.Vars(req)
	id, err := DencodeID(vars["id"])
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	apiContext.Write(s.getReplica(apiContext, id))
	return nil
}

func (s *Server) getReplica(context *api.ApiContext, id string) *Replica {
	for _, r := range s.c.ListReplicas() {
		if r.Address == id {
			return NewReplica(context, r.Address, r.Mode)
		}
	}
	return nil
}
