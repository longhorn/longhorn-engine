package rest

import (
	"context"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

type SyncServer struct {
	filePath    string
	fileIo      sparse.FileIoProcessor
	syncFileOps SyncFileOperations
	ctx         context.Context
	cancelFunc  context.CancelFunc

	srv *http.Server
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(ctx context.Context, port string, filePath string, timeout int) {
	Server(ctx, port, filePath, &SyncFileStub{})
}

func Server(ctx context.Context, port string, filePath string, syncFileOps SyncFileOperations) error {
	log.Infof("Creating Ssync service")
	ctx, cancelFunc := context.WithCancel(ctx)
	srv := &http.Server{
		Addr: ":" + port,
	}
	syncServer := &SyncServer{
		filePath:    filePath,
		syncFileOps: syncFileOps,
		ctx:         ctx,
		cancelFunc:  cancelFunc,
		srv:         srv,
	}
	srv.Handler = NewRouter(syncServer)

	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	return srv.ListenAndServe()
}
