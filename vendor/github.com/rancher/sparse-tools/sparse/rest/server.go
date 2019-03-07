package rest

import (
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/rancher/sparse-tools/sparse"
)

type SyncServer struct {
	filePath string
	fileIo   sparse.FileIoProcessor
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(port string, filePath string, timeout int) {
	Server(port, filePath)
}

func Server(port string, filePath string) error {
	log.Infof("Creating Ssync service")
	router := NewRouter(&SyncServer{filePath: filePath})
	return http.ListenAndServe(":"+port, router)
}
