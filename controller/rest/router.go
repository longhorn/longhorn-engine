package rest

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/yasker/go-websocket-toolbox/handler"

	"github.com/longhorn/longhorn-engine/types"

	// add pprof endpoint
	_ "net/http/pprof"
)

const (
	StreamTypeVolume  = "volumes"
	StreamTypeMetrics = "metrics"
)

func HandleError(s *client.Schemas, t func(http.ResponseWriter, *http.Request) error) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if err := t(rw, req); err != nil {
			apiContext := api.GetApiContext(req)
			apiContext.WriteErr(err)
		}
	}))
}

func NewRouter(s *Server) *mux.Router {
	schemas := NewSchema()
	router := mux.NewRouter().StrictSlash(true)
	f := HandleError

	// API framework routes
	router.Methods("GET").Path("/").Handler(api.VersionsHandler(schemas, "v1"))
	router.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	router.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))
	router.Methods("GET").Path("/v1").Handler(api.VersionHandler(schemas, "v1"))

	// Volumes
	router.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.ListVolumes))
	router.Methods("GET").Path("/v1/volumes/{id}").Handler(f(schemas, s.GetVolume))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "startfrontend").Handler(f(schemas, s.StartFrontend))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "shutdownfrontend").Handler(f(schemas, s.ShutdownFrontend))

	// Journal
	router.Methods("POST").Path("/v1/journal").Handler(f(schemas, s.ListJournal))

	// Settings
	router.Methods("POST").Path("/v1/settings/updateport").Handler(f(schemas, s.UpdatePort))

	// Version
	router.Methods("GET").Path("/v1/version/details").Handler(f(schemas, s.GetVersionDetails))

	// WebSockets
	volumeListStream := handler.NewStreamHandlerFunc(StreamTypeVolume, s.processEventVolumeList, s.c.Broadcaster, types.EventTypeVolume, types.EventTypeReplica)
	router.Path("/v1/ws/volumes").Handler(f(schemas, volumeListStream))

	metricsStream := handler.NewStreamHandlerFunc(StreamTypeMetrics, s.processEventMetrics, s.c.Broadcaster, types.EventTypeMetrics)
	router.Path("/v1/ws/metrics").Handler(f(schemas, metricsStream))

	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

	return router
}
