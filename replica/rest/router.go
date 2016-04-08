package rest

import (
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn/controller/rest"
)

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
	router.Methods("POST").Path("/v1/replicas/{id}").Queries("action", "reload").Handler(f(schemas, s.ReloadReplica))
	router.Methods("POST").Path("/v1/replicas/{id}").Queries("action", "snapshot").Handler(f(schemas, s.SnapshotReplica))
	router.Methods("POST").Path("/v1/replicas/{id}").Queries("action", "open").Handler(f(schemas, s.OpenReplica))
	router.Methods("POST").Path("/v1/replicas/{id}").Queries("action", "close").Handler(f(schemas, s.CloseReplica))
	router.Methods("POST").Path("/v1/replicas/{id}").Queries("action", "removedisk").Handler(f(schemas, s.RemoveDisk))
	router.Methods("DELETE").Path("/v1/replicas/{id}").Handler(f(schemas, s.DeleteReplica))

	return router
}
