package remote

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	cockroacherrors "github.com/cockroachdb/errors"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	enginerpc "github.com/longhorn/types/pkg/generated/enginerpc"
)

type fakeReplicaService struct {
	enginerpc.UnimplementedReplicaServiceServer
	failOpen  bool
	openCalls atomic.Int64
}

func (f *fakeReplicaService) ReplicaGet(ctx context.Context, e *emptypb.Empty) (*enginerpc.ReplicaGetResponse, error) {
	return &enginerpc.ReplicaGetResponse{
		Replica: &enginerpc.Replica{State: string(types.ReplicaStateClosed)},
	}, nil
}

func (f *fakeReplicaService) ReplicaOpen(ctx context.Context, req *enginerpc.ReplicaOpenRequest) (*enginerpc.ReplicaOpenResponse, error) {
	f.openCalls.Add(1)
	if f.failOpen {
		return nil, status.Error(codes.Internal, "simulated open failure")
	}
	return &enginerpc.ReplicaOpenResponse{}, nil
}

// listenControlDataPair binds control on port P and data on port P+1, as
// derived by util.ParseAddresses.
func listenControlDataPair(t *testing.T) (control, data net.Listener) {
	t.Helper()
	for i := 0; i < 100; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()

		dataL, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err != nil {
			continue
		}
		ctrlL, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			_ = dataL.Close()
			continue
		}
		return ctrlL, dataL
	}
	t.Fatal("failed to allocate control/data port pair")
	return nil, nil
}

// trackedConn records whether the underlying connection was closed.
type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.closed.Store(true)
	return err
}

// trackedTimeouts counts how many dataconn loops started and performed their
// exit cleanup (Decrement), without relying on whole-process goroutine counts.
type trackedTimeouts struct {
	types.SharedTimeouts
	started atomic.Int64
	exited  atomic.Int64
}

func (s *trackedTimeouts) Increment() {
	s.SharedTimeouts.Increment()
	s.started.Add(1)
}

func (s *trackedTimeouts) Decrement() {
	s.SharedTimeouts.Decrement()
	s.exited.Add(1)
}

func unclosedCount(conns []*trackedConn) int {
	n := 0
	for _, c := range conns {
		if !c.closed.Load() {
			n++
		}
	}
	return n
}

// TestFactoryCreateClosesConnectionsOnConnectFailure verifies that when a
// later connect() in the loop fails, the connections already established are
// closed. Ref: longhorn/longhorn#14231
func TestFactoryCreateClosesConnectionsOnConnectFailure(t *testing.T) {
	controlListener, dataL := listenControlDataPair(t)
	defer func() { _ = controlListener.Close() }()
	defer func() { _ = dataL.Close() }()

	srv := grpc.NewServer()
	enginerpc.RegisterReplicaServiceServer(srv, &fakeReplicaService{})
	go func() { _ = srv.Serve(controlListener) }()
	defer srv.Stop()

	first, peer := net.Pipe()
	defer func() { _ = first.Close() }()
	defer func() { _ = peer.Close() }()

	tracked := &trackedConn{Conn: first}
	dialErr := errors.New("injected second connection failure")
	calls := 0
	rf := &Factory{
		connectFn: func(_ types.DataServerProtocol, _ string) (net.Conn, error) {
			calls++
			if calls == 1 {
				return tracked, nil
			}
			return nil, dialErr
		},
	}

	backend, err := rf.Create("test-vol", controlListener.Addr().String(), types.DataServerProtocolTCP,
		util.NewSharedTimeouts(15*time.Second, time.Minute), false, 0)

	if !errors.Is(err, dialErr) {
		t.Fatalf("expected injected dial error, got %v", err)
	}
	if backend != nil {
		t.Fatal("expected nil backend")
	}
	if calls != 2 {
		t.Fatalf("expected 2 connection attempts, got %d", calls)
	}
	if !tracked.closed.Load() {
		t.Fatal("first connection was not closed")
	}
}

// TestFactoryCreateCleansUpClientOnOpenFailure verifies that when the remote
// replica fails to open, Factory.Create() closes every data connection it
// created and that every dataconn loop performed its exit cleanup. Ref:
// longhorn/longhorn#14231
func TestFactoryCreateCleansUpClientOnOpenFailure(t *testing.T) {
	controlListener, dataL := listenControlDataPair(t)
	defer func() { _ = controlListener.Close() }()
	defer func() { _ = dataL.Close() }()

	server := &fakeReplicaService{failOpen: true}
	srv := grpc.NewServer()
	enginerpc.RegisterReplicaServiceServer(srv, server)
	go func() { _ = srv.Serve(controlListener) }()
	defer srv.Stop()

	const iterations = 10
	var connections []*trackedConn
	rf := &Factory{
		connectFn: func(protocol types.DataServerProtocol, address string) (net.Conn, error) {
			conn, err := connect(protocol, address)
			if err != nil {
				return nil, err
			}
			tracked := &trackedConn{Conn: conn}
			connections = append(connections, tracked)
			return tracked, nil
		},
	}
	timeouts := &trackedTimeouts{
		SharedTimeouts: util.NewSharedTimeouts(15*time.Second, time.Minute),
	}

	for i := 0; i < iterations; i++ {
		backend, err := rf.Create("test-vol", controlListener.Addr().String(), types.DataServerProtocolTCP, timeouts, false, 0)
		// The failure must originate from the simulated ReplicaOpen, not from
		// ReplicaGet nor from the connect phase. Create() wraps the RPC error
		// with cockroachdb/errors, so unwrap to the cause before checking the status.
		cause := cockroacherrors.Cause(err)
		if status.Code(cause) != codes.Internal || status.Convert(cause).Message() != "simulated open failure" {
			t.Fatalf("iteration %d: expected simulated ReplicaOpen failure, got %v", i, err)
		}
		if backend != nil {
			t.Fatalf("iteration %d: expected nil backend", i)
		}
	}

	// Every data connection actually established must have been closed.
	want := iterations * NumberOfConnections
	if len(connections) != want {
		t.Fatalf("expected %d connections, got %d", want, len(connections))
	}
	for i, conn := range connections {
		if !conn.closed.Load() {
			t.Errorf("connection %d not closed", i)
		}
	}
	// The target path really executed on every iteration.
	if got := server.openCalls.Load(); got != iterations {
		t.Fatalf("expected ReplicaOpen called %d times, got %d", iterations, got)
	}

	// Asserting started == exited == iterations only proves every loop ran its
	// exit cleanup (Decrement); it does not claim the sibling sender/receiver
	// goroutines have all returned.
	deadline := time.Now().Add(10 * time.Second)
	for {
		started, exited := timeouts.started.Load(), timeouts.exited.Load()
		if started == iterations && exited == iterations {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loop cleanup incomplete: started=%d exited=%d, unclosed connections=%d",
				started, exited, unclosedCount(connections))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
