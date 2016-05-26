package rest

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	lclient "github.com/rancher/longhorn/controller/client"
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
	schemas := newSchema()
	router := mux.NewRouter().StrictSlash(true)
	f := HandleError

	// API framework routes
	router.Methods("GET").Path("/").Handler(api.VersionsHandler(schemas, "v1"))
	router.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	router.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))
	router.Methods("GET").Path("/v1").Handler(api.VersionHandler(schemas, "v1"))

	// Volume(s)
	router.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.ListVolumes))
	router.Methods("GET").Path("/v1/volumes/{id}").Handler(f(schemas, s.GetVolume))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "reverttosnapshot").Handler(f(schemas, s.RevertToSnapshot))
	router.Methods("POST").Path("/v1/volumes/{id}").Queries("action", "restorefrombackup").Handler(f(schemas, s.RestoreFromBackup))

	// Snapshots
	router.Methods("GET").Path("/v1/snapshots").Handler(f(schemas, s.ListSnapshots))
	router.Methods("GET").Path("/v1/snapshots/{id}").Handler(f(schemas, s.GetSnapshot))
	router.Methods("POST").Path("/v1/snapshots").Handler(f(schemas, s.CreateSnapshot))
	router.Methods("DELETE").Path("/v1/snapshots/{id}").Handler(f(schemas, s.DeleteSnapshot))
	router.Methods("POST").Path("/v1/snapshots/{id}").Queries("action", "backup").Handler(f(schemas, s.CreateBackup))
	router.Methods("POST").Path("/v1/snapshots/{id}").Queries("action", "removebackup").Handler(f(schemas, s.RemoveBackup))

	// Restore status
	router.Methods("GET").Path("/v1/backupstatuses/{id}").Handler(f(schemas, s.GetBackupStatus))
	router.Methods("GET").Path("/v1/restorestatuses/{id}").Handler(f(schemas, s.GetRestoreStatus))

	return router
}

type Server struct {
	controllerClient *lclient.ControllerClient
}

func NewServer() *Server {
	contollerClient := lclient.NewControllerClient("http://localhost:9501")
	return &Server{
		controllerClient: contollerClient,
	}
}
