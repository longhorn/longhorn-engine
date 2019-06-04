package rest

import (
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"io"
	"net/http"
)

func (s *Server) ListReplicas(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	resp := client.GenericCollection{}
	resp.Data = append(resp.Data, s.Replica(apiContext))

	apiContext.Write(&resp)
	return nil
}

func (s *Server) Replica(apiContext *api.ApiContext) *Replica {
	state, info := s.s.Status()
	return NewReplica(apiContext, state, info, s.s.Replica())
}

func (s *Server) GetReplica(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	r := s.Replica(apiContext)
	if mux.Vars(req)["id"] == r.Id {
		apiContext.Write(r)
	} else {
		rw.WriteHeader(http.StatusNotFound)
	}
	return nil
}

func (s *Server) doOp(req *http.Request, err error) error {
	if err != nil {
		return err
	}

	apiContext := api.GetApiContext(req)
	apiContext.Write(s.Replica(apiContext))
	return nil
}

func (s *Server) SetRebuilding(rw http.ResponseWriter, req *http.Request) error {
	var input RebuildingInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}

	return s.doOp(req, s.s.SetRebuilding(input.Rebuilding))
}

func (s *Server) MarkDiskAsRemoved(rw http.ResponseWriter, req *http.Request) error {
	var input MarkDiskAsRemovedInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	return s.doOp(req, s.s.MarkDiskAsRemoved(input.Name))
}

func (s *Server) PrepareRemoveDisk(rw http.ResponseWriter, req *http.Request) error {
	var input PrepareRemoveDiskInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}
	operations, err := s.s.PrepareRemoveDisk(input.Name)
	if err != nil {
		return err
	}
	apiContext.Write(&PrepareRemoveDiskOutput{
		Resource: client.Resource{
			Id:   input.Name,
			Type: "prepareRemoveDiskOutput",
		},
		Operations: operations,
	})
	return nil
}

func (s *Server) DeleteReplica(rw http.ResponseWriter, req *http.Request) error {
	return s.doOp(req, s.s.Delete())
}

func (s *Server) SetRevisionCounter(rw http.ResponseWriter, req *http.Request) error {
	var input RevisionCounter
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}
	return s.doOp(req, s.s.SetRevisionCounter(input.Counter))
}
