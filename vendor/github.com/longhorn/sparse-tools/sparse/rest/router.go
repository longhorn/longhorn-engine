package rest

import "github.com/gorilla/mux"

// NewRouter creates and configures a mux router
func NewRouter(server *SyncServer) *mux.Router {

	// API framework routes
	router := mux.NewRouter().StrictSlash(true)

	// Application
	router.HandleFunc("/v1-ssync/open", server.open).Methods("GET")
	router.HandleFunc("/v1-ssync/close", server.close).Methods("POST")
	router.HandleFunc("/v1-ssync/sendHole", server.sendHole).Methods("POST")
	router.HandleFunc("/v1-ssync/writeData", server.writeData).Methods("POST")
	router.HandleFunc("/v1-ssync/getChecksum", server.getChecksum).Methods("GET")
	router.HandleFunc("/v1-ssync/getRecordedMetadata", server.getRecordedMetadata).Methods("GET")

	return router
}
