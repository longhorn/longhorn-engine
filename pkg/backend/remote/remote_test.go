package remote

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	enginerpc "github.com/longhorn/types/pkg/generated/enginerpc"
)

type fakeReplicaService struct {
	enginerpc.UnimplementedReplicaServiceServer
	failOpen bool
}

func (f *fakeReplicaService) ReplicaGet(ctx context.Context, e *emptypb.Empty) (*enginerpc.ReplicaGetResponse, error) {
	return &enginerpc.ReplicaGetResponse{
		Replica: &enginerpc.Replica{State: string(types.ReplicaStateClosed)},
	}, nil
}

func (f *fakeReplicaService) ReplicaOpen(ctx context.Context, req *enginerpc.ReplicaOpenRequest) (*enginerpc.ReplicaOpenResponse, error) {
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
		l.Close()

		dataL, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err != nil {
			continue
		}
		ctrlL, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			dataL.Close()
			continue
		}
		return ctrlL, dataL
	}
	t.Fatal("failed to allocate control/data port pair")
	return nil, nil
}

func fdCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("fd counting unavailable: %v", err)
	}
	return len(entries)
}

// TestFactoryCreateNoLeakOnOpenFailure verifies that Factory.Create() does
// not leak connections or goroutines when the remote replica cannot be
// opened. Before the fix, each failed Create() leaked the dataconn client:
// NumberOfConnections TCP connections and 5 goroutines.
// Ref: longhorn/longhorn#14231
func TestFactoryCreateNoLeakOnOpenFailure(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fd counting requires /proc")
	}

	controlL, dataL := listenControlDataPair(t)
	defer controlL.Close()
	defer dataL.Close()

	srv := grpc.NewServer()
	enginerpc.RegisterReplicaServiceServer(srv, &fakeReplicaService{failOpen: true})
	go srv.Serve(controlL)
	defer srv.Stop()

	rf := &Factory{}
	sharedTimeouts := util.NewSharedTimeouts(15*time.Second, 1*time.Minute)

	const iterations = 10
	fdBefore := fdCount(t)
	gorBefore := runtime.NumGoroutine()

	for i := 0; i < iterations; i++ {
		if _, err := rf.Create("test-vol", controlL.Addr().String(), types.DataServerProtocolTCP, sharedTimeouts, false, 0); err == nil {
			t.Fatalf("expected Create() to fail on iteration %d", i)
		}
	}

	// Give goroutines terminated by the cleanup a moment to exit before
	// counting.
	time.Sleep(2 * time.Second)

	fdLeaked := fdCount(t) - fdBefore
	gorLeaked := runtime.NumGoroutine() - gorBefore

	t.Logf("after %d failed Create() calls: leaked fds = %d, leaked goroutines = %d",
		iterations, fdLeaked, gorLeaked)

	// Allow small tolerance for unrelated background activity, but far below
	// the pre-fix leak of 2 fds + 5 goroutines per iteration.
	if fdLeaked > iterations {
		t.Errorf("expected no significant fd leak, got %d leaked fds", fdLeaked)
	}
	if gorLeaked > iterations {
		t.Errorf("expected no significant goroutine leak, got %d leaked goroutines", gorLeaked)
	}
}
