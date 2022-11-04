package rest

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) ListVolumes(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	apiContext.Write(&client.GenericCollection{
		Data: []interface{}{
			s.listVolumes(apiContext)[0],
		},
	})
	return nil
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

func (s *Server) ReadAt(rw http.ResponseWriter, req *http.Request) error {
	var input ReadInput

	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	buf := make([]byte, input.Length)
	_, err := s.d.backend.ReadAt(buf, input.Offset)
	if err != nil {
		log.Errorln("read failed: ", err.Error())
		return errors.Wrap(err, "read failed")
	}

	data := EncodeData(buf)
	apiContext.Write(&ReadOutput{
		Resource: client.Resource{
			Type: "readOutput",
		},
		Data: data,
	})
	return nil
}

func (s *Server) WriteAt(rw http.ResponseWriter, req *http.Request) error {
	var input WriteInput

	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	v := s.getVolume(apiContext, id)
	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	buf, err := DecodeData(input.Data)
	if err != nil {
		return err
	}
	if len(buf) != input.Length {
		return fmt.Errorf("inconsistent length in request")
	}

	if _, err := s.d.backend.WriteAt(buf, input.Offset); err != nil {
		log.Errorln("write failed: ", err.Error())
		return err
	}
	apiContext.Write(&WriteOutput{
		Resource: client.Resource{
			Type: "writeOutput",
		},
	})
	return nil
}

func (s *Server) listVolumes(context *api.ApiContext) []*Volume {
	return []*Volume{
		NewVolume(context, s.d.Name),
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
