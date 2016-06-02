package rest

import (
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn/replica/rest"
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

	// Volumes
	router.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.ListVolumes))
	router.Methods("GET").Path("/v1/volumes/{id}").Handler(f(schemas, s.GetVolume))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "start").Handler(f(schemas, s.StartVolume))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "shutdown").Handler(f(schemas, s.ShutdownVolume))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "snapshot").Handler(f(schemas, s.SnapshotVolume))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "revert").Handler(f(schemas, s.RevertVolume))

	// Replicas
	router.Methods("GET").Path("/v1/replicas").Handler(f(schemas, s.ListReplicas))
	router.Methods("GET").Path("/v1/replicas/{id}").Handler(f(schemas, s.GetReplica))
	router.Methods("POST").Path("/v1/replicas").Handler(f(schemas, s.CreateReplica))
	router.Methods("DELETE").Path("/v1/replicas/{id}").Handler(f(schemas, s.DeleteReplica))
	router.Methods("PUT").Path("/v1/replicas/{id}").Handler(f(schemas, s.UpdateReplica))

	// Stats
	router.Methods("POST").Path("/v1/stats").Handler(f(schemas, s.ListStats))

	return router
}
