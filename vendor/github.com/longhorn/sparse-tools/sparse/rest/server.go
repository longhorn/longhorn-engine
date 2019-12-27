package rest

import (
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

type SyncServer struct {
	filePath    string
	fileIo      sparse.FileIoProcessor
	syncFileOps SyncFileOperations

	srv *http.Server
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(port string, filePath string, timeout int) {
	Server(port, filePath, &SyncFileStub{})
}

func Server(port string, filePath string, syncFileOps SyncFileOperations) error {
	log.Infof("Creating Ssync service")
	srv := &http.Server{
		Addr: ":" + port,
	}
	syncServer := &SyncServer{
		filePath:    filePath,
		syncFileOps: syncFileOps,
		srv:         srv,
	}
	srv.Handler = NewRouter(syncServer)

	return srv.ListenAndServe()
}
