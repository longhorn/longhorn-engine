package rest

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn/controller/rest"
)

func checkAction(s *Server, t func(http.ResponseWriter, *http.Request) error) func(http.ResponseWriter, *http.Request) error {
	return func(rw http.ResponseWriter, req *http.Request) error {
		replica := s.Replica(api.GetApiContext(req))
		if replica.Actions[req.URL.Query().Get("action")] == "" {
			rw.WriteHeader(http.StatusNotFound)
			return nil
		}
		return t(rw, req)
	}
}

func NewRouter(s *Server) *mux.Router {
	schemas := NewSchema()
	router := mux.NewRouter().StrictSlash(true)
	f := rest.HandleError

	// API framework routes
	router.Methods("GET").Path("/").Handler(api.VersionsHandler(schemas, "v1"))
	router.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	router.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))
	router.Methods("GET").Path("/v1").Handler(api.VersionHandler(schemas, "v1"))

	// Replicas
	router.Methods("GET").Path("/v1/replicas").Handler(f(schemas, s.ListReplicas))
	router.Methods("GET").Path("/v1/replicas/{id}").Handler(f(schemas, s.GetReplica))
	router.Methods("DELETE").Path("/v1/replicas/{id}").Handler(f(schemas, s.DeleteReplica))

	// Actions
	actions := map[string]func(http.ResponseWriter, *http.Request) error{
		"reload":        s.ReloadReplica,
		"snapshot":      s.SnapshotReplica,
		"open":          s.OpenReplica,
		"close":         s.CloseReplica,
		"removedisk":    s.RemoveDisk,
		"setrebuilding": s.SetRebuilding,
		"create":        s.Create,
	}

	for name, action := range actions {
		router.Methods("POST").Path("/v1/replicas/{id}").Queries("action", name).Handler(f(schemas, checkAction(s, action)))
	}

	return router
}
