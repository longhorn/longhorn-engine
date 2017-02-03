package rest

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn/types"
)

func (s *Server) ListReplicas(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	resp := client.GenericCollection{}
	for _, r := range s.c.ListReplicas() {
		resp.Data = append(resp.Data, NewReplica(apiContext, r.Address, r.Mode))
	}

	resp.ResourceType = "replica"
	resp.CreateTypes = map[string]string{
		"replica": apiContext.UrlBuilder.Collection("replica"),
	}

	apiContext.Write(&resp)
	return nil
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

func (s *Server) CreateReplica(rw http.ResponseWriter, req *http.Request) error {
	var replica Replica
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&replica); err != nil {
		return err
	}

	if err := s.c.AddReplica(replica.Address); err != nil {
		return err
	}

	apiContext.Write(s.getReplica(apiContext, replica.Address))
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

func (s *Server) DeleteReplica(rw http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	id, err := DencodeID(vars["id"])
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	return s.c.RemoveReplica(id)
}

func (s *Server) UpdateReplica(rw http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	id, err := DencodeID(vars["id"])
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var replica Replica
	apiContext := api.GetApiContext(req)
	apiContext.Read(&replica)

	if err := s.c.SetReplicaMode(id, types.Mode(replica.Mode)); err != nil {
		return err
	}

	return s.GetReplica(rw, req)
}

func (s *Server) PrepareRebuildReplica(rw http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	id, err := DencodeID(vars["id"])
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	disks, err := s.c.PrepareRebuildReplica(id)
	if err != nil {
		return err
	}

	apiContext := api.GetApiContext(req)
	resp := &PrepareRebuildOutput{
		Resource: client.Resource{
			Id:   id,
			Type: "prepareRebuildOutput",
		},
		Disks: disks,
	}

	apiContext.Write(&resp)
	return nil
}

func (s *Server) VerifyRebuildReplica(rw http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	id, err := DencodeID(vars["id"])
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	if err := s.c.VerifyRebuildReplica(id); err != nil {
		return err
	}

	return s.GetReplica(rw, req)
}
