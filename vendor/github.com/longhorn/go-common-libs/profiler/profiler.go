package profiler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/longhorn/types/pkg/generated/profilerrpc"

	"github.com/longhorn/go-common-libs/utils"
)

// ClientContext is the context with the ProfilerClient and the connection
//
//   - conn: the connection for the gRPC server
//   - service: the ProfilerClient
type ClientContext struct {
	conn    *grpc.ClientConn
	service profilerrpc.ProfilerClient
}

// Close closes the connection for the gRPC server
func (c ClientContext) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Server is the gRPC server to provide the basic operations
// like (show/enable/disable) for the profiler
//
//   - name: the name of the server
//   - errMsg: the error message current server has (if enabled failed, it will have the error message)
//   - server: the http server for the profiler
//   - listener: the network listener for the profiler server
//   - lock: the RW lock for the server
type Server struct {
	profilerrpc.UnimplementedProfilerServer

	name   string
	errMsg string

	server *http.Server
	// Keep the listener separately because DisableProfiler may run before
	// http.Server.Serve has tracked it.
	listener net.Listener
	lock     sync.RWMutex
}

// Client is the gRPC client to interactive with the ProfilerServer
//   - Name: the name of the client
//   - serviceURL: the URL of the gRPC server
//   - ProfilerClientContext: the context with the ProfilerClient and the connection
type Client struct {
	Name       string
	serviceURL string
	ClientContext
}

// NewServer returns the gRPC service server for the profiler
func NewServer(name string) *Server {
	return &Server{
		name:   name,
		server: nil,
	}
}

// NewClient returns the gRPC client to interactive with the gRPC server for the profiler
func NewClient(address, name string, dialOpts ...grpc.DialOption) (*Client, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	for _, opt := range dialOpts {
		if opt != nil {
			logrus.Debugf("Add dial option: %v", opt)
			opts = append(opts, opt)
		}
	}
	getContext := func(serviceURL string) (ClientContext, error) {
		connection, err := grpc.NewClient(serviceURL, opts...)
		if err != nil {
			return ClientContext{}, fmt.Errorf("cannot connect to ProfilerServer %v", serviceURL)
		}

		return ClientContext{
			conn:    connection,
			service: profilerrpc.NewProfilerClient(connection),
		}, nil
	}

	serviceURL := utils.GetGRPCAddress(address)
	ctx, err := getContext(serviceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create ProfilerClient for %v: %v", serviceURL, err)
	}

	return &Client{
		Name:          name,
		serviceURL:    serviceURL,
		ClientContext: ctx,
	}, nil

}

// ProfilerOP is the gRPC function to provide the entry point for the profiler operations
func (s *Server) ProfilerOP(_ context.Context, req *profilerrpc.ProfilerOPRequest) (*profilerrpc.ProfilerOPResponse, error) {
	logrus.Infof("Profiler operation: %v, port: %v", req.RequestOp, req.PortNumber)
	var err error
	switch req.RequestOp {
	case profilerrpc.Op_SHOW:
		reply := &profilerrpc.ProfilerOPResponse{}
		reply.ProfilerAddr, err = s.ShowProfiler()
		return reply, err
	case profilerrpc.Op_ENABLE:
		reply := &profilerrpc.ProfilerOPResponse{}
		reply.ProfilerAddr, err = s.EnableProfiler(req.PortNumber)
		return reply, err
	case profilerrpc.Op_DISABLE:
		reply := &profilerrpc.ProfilerOPResponse{}
		reply.ProfilerAddr, err = s.DisableProfiler()
		return reply, err
	default:
		return nil, errors.New("invalid operation")
	}
}

// ShowProfiler returns the address of the profiler
func (s *Server) ShowProfiler() (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	logrus.Info("Preparing to show the profiler address")
	if s.server != nil {
		return s.server.Addr, nil
	}
	if s.errMsg != "" {
		return s.errMsg, nil
	}
	return "", nil
}

// EnableProfiler enables the profiler with specific port number.
// It will fail if the profiler is already enabled or the port number is invalid(0).
// For the profiler server enabling failed, it will return the error message.
// And keep the above error message, then user can get the error message by op `show`.
// The normal(success) case will return the address of the profiler server.
func (s *Server) EnableProfiler(portNumber int32) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	logrus.Info("Preparing to enable the profiler")

	if s.server != nil {
		return "", fmt.Errorf("profiler server is already running at %v", s.server.Addr)
	}

	profilerPort := int(portNumber)
	if profilerPort == 0 {
		s.errMsg = "enable profiler failed: invalid port number"
		return s.errMsg, fmt.Errorf("invalid port number: %v", portNumber)
	}

	profilerAddr := fmt.Sprintf(":%d", profilerPort)
	newServer := &http.Server{
		Addr:              profilerAddr,
		ReadHeaderTimeout: 10 * time.Second,
	}
	// Listen synchronously so success means the port is already bound; no
	// readiness polling is needed before Serve starts accepting.
	listener, err := net.Listen("tcp", profilerAddr)
	if err != nil {
		s.errMsg = err.Error()
		return s.errMsg, fmt.Errorf("failed to start profiler server(%v): %w", profilerAddr, err)
	}

	s.server = newServer
	s.listener = listener
	s.errMsg = ""
	go func() {
		err := newServer.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logrus.WithError(err).Warnf("Get error when start profiler server %v", newServer.Addr)
			if closeErr := newServer.Close(); closeErr != nil {
				logrus.WithError(closeErr).Warnf("Failed to close the profiler server %v", newServer.Addr)
			}
			s.lock.Lock()
			if s.server == newServer {
				s.server = nil
				s.listener = nil
				s.errMsg = err.Error()
			}
			s.lock.Unlock()
		}
		logrus.Infof("Profiler server (%v) is closed", newServer.Addr)
	}()

	return s.server.Addr, nil
}

// DisableProfiler disables the profilerrpc.
// It will return the error when fails to shut down the profiler server.
func (s *Server) DisableProfiler() (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	logrus.Info("Preparing to disable the profiler")
	if s.server == nil {
		return "", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.disableProfilerLocked(ctx)
}

// disableProfilerLocked disables the profiler while the caller holds s.lock.
func (s *Server) disableProfilerLocked(ctx context.Context) (string, error) {
	server := s.server
	if shutdownErr := server.Shutdown(ctx); shutdownErr != nil {
		logrus.WithError(shutdownErr).Warnf("Failed to shutdown the profiler %v", server.Addr)
		// Shutdown has stopped accepting requests even when it times out.
		// Force-close remaining connections and the retained listener before
		// clearing the profiler state so a failed disable cannot leak the port.
		if closeErr := server.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			logrus.WithError(closeErr).Warnf("Failed to close the profiler server %v", server.Addr)
		}
		if s.listener != nil {
			if closeErr := s.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				logrus.WithError(closeErr).Warnf("Failed to close the profiler listener %v", server.Addr)
			}
		}
		s.server = nil
		s.listener = nil
		s.errMsg = shutdownErr.Error()
		return "", shutdownErr
	}
	if s.listener != nil {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logrus.WithError(err).Warnf("Failed to close the profiler listener %v", server.Addr)
			return "", err
		}
	}

	s.server = nil
	s.listener = nil
	return "", nil
}

// ProfilerOP will call the doProfilerOP for the ProfilerServer.
// It will convert the hunam readable op to internal usage and is the entry point for the profiler operations.
func (c *Client) ProfilerOP(op string, portNumber int32) (string, error) {
	opValue, exists := profilerrpc.Op_value[op]
	if !exists {
		return "", fmt.Errorf("invalid operation: %v", op)
	}
	return c.doProfilerOP(profilerrpc.Op(opValue), portNumber)
}

// doProfilerOP is the internal function to call the ProfilerOP for the ProfilerServer.
func (c *Client) doProfilerOP(op profilerrpc.Op, portNumber int32) (string, error) {
	controllerServiceClient := c.getProfilerServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), utils.GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ProfilerOP(ctx, &profilerrpc.ProfilerOPRequest{
		RequestOp:  op,
		PortNumber: portNumber,
	})
	if err != nil {
		return "", err
	}
	return reply.ProfilerAddr, nil
}

// getProfilerServiceClient returns the ProfilerClient (internal function)
func (c *Client) getProfilerServiceClient() profilerrpc.ProfilerClient {
	return c.service
}
