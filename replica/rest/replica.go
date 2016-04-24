package rest

import (
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/satori/go.uuid"
)

func (s *Server) ListReplicas(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	resp := client.GenericCollection{}
	resp.Data = append(resp.Data, s.getReplica(apiContext))

	apiContext.Write(&resp)
	return nil
}

func (s *Server) getReplica(apiContext *api.ApiContext) *Replica {
	return NewReplica(apiContext, s.s.Replica())
}

func (s *Server) GetReplica(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	r := s.getReplica(apiContext)
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
	apiContext.Write(s.getReplica(apiContext))
	return nil
}

func (s *Server) OpenReplica(rw http.ResponseWriter, req *http.Request) error {
	var input OpenInput
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

	return s.doOp(req, s.s.Open(size))
}

func (s *Server) RemoveDisk(rw http.ResponseWriter, req *http.Request) error {
	var input RemoveDiskInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	return s.doOp(req, s.s.RemoveDiffDisk(input.Name))
}

func (s *Server) SnapshotReplica(rw http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil && err != io.EOF {
		return err
	}

	name := input.Name
	if name == "" {
		name = uuid.NewV4().String()
	}

	return s.doOp(req, s.s.Snapshot(name))
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
