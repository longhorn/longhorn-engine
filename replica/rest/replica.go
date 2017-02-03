package rest

import (
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn/util"
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

func (s *Server) Create(rw http.ResponseWriter, req *http.Request) error {
	var input CreateInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}

	size := int64(0)
	if input.Size != "" {
		var err error
		size, err = strconv.ParseInt(input.Size, 10, 0)
		if err != nil {
			return err
		}
	}

	return s.doOp(req, s.s.Create(size))
}

func (s *Server) OpenReplica(rw http.ResponseWriter, req *http.Request) error {
	return s.doOp(req, s.s.Open())
}

func (s *Server) RemoveDisk(rw http.ResponseWriter, req *http.Request) error {
	var input RemoveDiskInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	return s.doOp(req, s.s.RemoveDiffDisk(input.Name))
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

func (s *Server) SnapshotReplica(rw http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}

	name := input.Name
	if name == "" {
		name = util.UUID()
	}

	return s.doOp(req, s.s.Snapshot(name))
}

func (s *Server) RevertReplica(rw http.ResponseWriter, req *http.Request) error {
	var input RevertInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}

	return s.doOp(req, s.s.Revert(input.Name))
}

func (s *Server) ReloadReplica(rw http.ResponseWriter, req *http.Request) error {
	return s.doOp(req, s.s.Reload())
}

func (s *Server) CloseReplica(rw http.ResponseWriter, req *http.Request) error {
	return s.doOp(req, s.s.Close())
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
