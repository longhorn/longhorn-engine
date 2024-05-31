package rest

import (
	"context"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

var (
	defaultIdleTimeout     = 90 * time.Second
	defaultHTTPIdleTimeout = 60 * time.Second
)

type IdleTimer struct {
	sync.Mutex
	activeConns map[net.Conn]bool
	timeout     time.Duration
	timer       *time.Timer
}

func NewIdleTimer(idleTimeout time.Duration) *IdleTimer {
	return &IdleTimer{
		activeConns: make(map[net.Conn]bool),
		timeout:     idleTimeout,
		timer:       time.NewTimer(idleTimeout),
	}
}

func (it *IdleTimer) ConnState(conn net.Conn, state http.ConnState) {
	it.Lock()
	defer it.Unlock()

	switch state {
	case http.StateNew, http.StateActive, http.StateHijacked:
		it.activeConns[conn] = true
		it.timer.Stop()
	case http.StateIdle, http.StateClosed:
		delete(it.activeConns, conn)
		if len(it.activeConns) == 0 {
			it.timer.Reset(it.timeout)
		}
	}
}

func (t *IdleTimer) Done() <-chan time.Time {
	return t.timer.C
}

type DataSyncServer interface {
	open(writer http.ResponseWriter, request *http.Request)
	close(writer http.ResponseWriter, request *http.Request)
	sendHole(writer http.ResponseWriter, request *http.Request)
	writeData(writer http.ResponseWriter, request *http.Request)
	getChecksum(writer http.ResponseWriter, request *http.Request)
	getRecordedMetadata(writer http.ResponseWriter, request *http.Request)
}

type SyncServer struct {
	filePath    string
	fileIo      sparse.FileIoProcessor
	syncFileOps SyncFileOperations
	ctx         context.Context
	cancelFunc  context.CancelFunc

	fileAlreadyExists bool

	srv *http.Server
}

func Server(ctx context.Context, port string, filePath string, syncFileOps SyncFileOperations) error {
	log.Infof("Creating Ssync service")
	ctx, cancelFunc := context.WithCancel(ctx)
	srv := &http.Server{
		Addr:        ":" + port,
		IdleTimeout: defaultHTTPIdleTimeout,
	}

	// if server has no connection for a period of time, it will shutdown itself to prevent receiver from getting stuck.
	idleTimer := NewIdleTimer(defaultIdleTimeout)
	srv.ConnState = idleTimer.ConnState

	fileAlreadyExists := true
	if _, err := os.Stat(filePath); err != nil && errors.Is(err, os.ErrNotExist) {
		log.Infof("file %v does not exist", filePath)
		fileAlreadyExists = false
	}

	syncServer := &SyncServer{
		filePath:          filePath,
		syncFileOps:       syncFileOps,
		ctx:               ctx,
		cancelFunc:        cancelFunc,
		srv:               srv,
		fileAlreadyExists: fileAlreadyExists,
	}
	srv.Handler = NewRouter(syncServer)

	go func() {
		select {
		case <-ctx.Done():
			srv.Close()
		case <-idleTimer.Done():
			log.Errorf("Shutting down the server since it is idle for %v", idleTimer.timeout)
			srv.Close()
		}
	}()

	return srv.ListenAndServe()
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(ctx context.Context, port string, filePath string, timeout int) error {
	return Server(ctx, port, filePath, &SyncFileStub{})
}
