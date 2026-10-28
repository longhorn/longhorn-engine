package sync

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
)

func TestGetReplicaStatus(t *testing.T) {
	interval, maxDuration := replicaStatusRetryInterval, replicaStatusMaxRetryableDuration
	replicaStatusRetryInterval, replicaStatusMaxRetryableDuration = time.Millisecond, 500*time.Millisecond
	defer func() { replicaStatusRetryInterval, replicaStatusMaxRetryableDuration = interval, maxDuration }()

	unavailable := errors.Wrap(status.Error(codes.Unavailable, "connection reset by peer"), "failed to get snapshot purge status")
	deadline := errors.Wrap(status.Error(codes.DeadlineExceeded, "context deadline exceeded"), "failed to get snapshot purge status")

	testCases := map[string]struct {
		errs         []error
		delay        time.Duration
		wantErr      error
		wantAttempts int
	}{
		"success":                      {errs: nil, wantAttempts: 1},
		"one unavailable":              {errs: []error{unavailable}, wantAttempts: 2},
		"unavailable until last retry": {errs: []error{unavailable, unavailable}, wantAttempts: 3},
		"always unavailable":           {errs: []error{unavailable, unavailable, unavailable, unavailable}, wantErr: unavailable, wantAttempts: 3},
		"deadline exceeded":            {errs: []error{deadline}, wantErr: deadline, wantAttempts: 1},
		"slow unavailable":             {errs: []error{unavailable}, delay: time.Second, wantErr: unavailable, wantAttempts: 1},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			attempts := 0
			var clients []*replicaClient.ReplicaClient
			resp, err := getReplicaStatus("tcp://10.0.0.1:10000", "volume", func(c *replicaClient.ReplicaClient) (string, error) {
				attempts++
				clients = append(clients, c)
				time.Sleep(tc.delay)
				if attempts <= len(tc.errs) {
					return "", tc.errs[attempts-1]
				}
				return "ok", nil
			})
			if attempts != tc.wantAttempts {
				t.Errorf("attempts = %d, want %d", attempts, tc.wantAttempts)
			}
			if tc.wantErr != nil {
				if err != tc.wantErr {
					t.Errorf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil || resp != "ok" {
				t.Errorf("resp, err = %q, %v, want \"ok\", nil", resp, err)
			}
			for i := 1; i < len(clients); i++ {
				if clients[i] == clients[i-1] {
					t.Errorf("attempt %d reused the client of attempt %d", i+1, i)
				}
			}
		})
	}
}

func TestGetReplicaStatusConnectionRefused(t *testing.T) {
	interval := replicaStatusRetryInterval
	replicaStatusRetryInterval = time.Millisecond
	defer func() { replicaStatusRetryInterval = interval }()

	// Without a listener on the sync agent port the status request fails with codes.Unavailable.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := replicaAddress(l)
	_ = l.Close()

	attempts := 0
	_, err = getReplicaStatus(addr, "volume", func(c *replicaClient.ReplicaClient) (any, error) {
		attempts++
		return c.SnapshotPurgeStatus()
	})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("err = %v, want code Unavailable", err)
	}
	if attempts != replicaStatusMaxAttempts {
		t.Errorf("attempts = %d, want %d", attempts, replicaStatusMaxAttempts)
	}
}

// replicaAddress returns the address of a replica whose sync agent listens on l, which is the replica port + 2.
func replicaAddress(l net.Listener) string {
	return fmt.Sprintf("tcp://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port-2)
}

// resetListener closes the first n accepted connections with a TCP reset before the gRPC handshake.
type resetListener struct {
	net.Listener
	n int
}

func (l *resetListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil || l.n == 0 {
			return conn, err
		}
		l.n--
		_ = conn.(*net.TCPConn).SetLinger(0)
		_ = conn.Close()
	}
}

type purgeStatusServer struct {
	enginerpc.UnimplementedSyncAgentServiceServer
	delay time.Duration
}

func (s purgeStatusServer) SnapshotPurgeStatus(context.Context, *emptypb.Empty) (*enginerpc.SnapshotPurgeStatusResponse, error) {
	time.Sleep(s.delay)
	return &enginerpc.SnapshotPurgeStatusResponse{State: "complete"}, nil
}

// startSyncAgent starts a sync agent that resets its first n connections and returns the replica's address.
func startSyncAgent(t *testing.T, n int) string {
	return startSlowSyncAgent(t, n, 0)
}

// startSlowSyncAgent is startSyncAgent with a sync agent that takes delay to answer.
func startSlowSyncAgent(t *testing.T, n int, delay time.Duration) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	enginerpc.RegisterSyncAgentServiceServer(server, purgeStatusServer{delay: delay})
	go func() {
		_ = server.Serve(&resetListener{Listener: l, n: n})
	}()
	t.Cleanup(server.Stop)
	return replicaAddress(l)
}

func TestGetReplicaStatusConnectionReset(t *testing.T) {
	interval := replicaStatusRetryInterval
	replicaStatusRetryInterval = time.Millisecond
	defer func() { replicaStatusRetryInterval = interval }()

	t.Run("new client per attempt", func(t *testing.T) {
		var clients []*replicaClient.ReplicaClient
		resp, err := getReplicaStatus(startSyncAgent(t, 1), "volume", func(c *replicaClient.ReplicaClient) (*enginerpc.SnapshotPurgeStatusResponse, error) {
			clients = append(clients, c)
			return c.SnapshotPurgeStatus()
		})
		if err != nil || resp.State != "complete" {
			t.Errorf("resp, err = %v, %v, want state complete", resp, err)
		}
		if len(clients) != 2 || clients[0] == clients[1] {
			t.Errorf("attempts = %d, want 2 with different clients", len(clients))
		}
	})

	// The client of a reset connection fails until its reconnect backoff ends, so a retry on it would not help.
	t.Run("same client", func(t *testing.T) {
		c, err := replicaClient.NewReplicaClient(startSyncAgent(t, 1), "volume", "")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = c.Close()
		}()
		for attempt := 1; attempt <= 2; attempt++ {
			if _, err := c.SnapshotPurgeStatus(); status.Code(err) != codes.Unavailable {
				t.Errorf("attempt %d: err = %v, want code Unavailable", attempt, err)
			}
		}
	})
}

func TestGetReplicaStatuses(t *testing.T) {
	interval := replicaStatusRetryInterval
	replicaStatusRetryInterval = time.Millisecond
	defer func() { replicaStatusRetryInterval = interval }()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := replicaAddress(l)
	_ = l.Close()

	const delay = 500 * time.Millisecond
	slow := []string{startSlowSyncAgent(t, 0, delay), startSlowSyncAgent(t, 1, delay), startSlowSyncAgent(t, 0, delay)}

	start := time.Now()
	results := getReplicaStatuses(append(slow, refused), "volume", (*replicaClient.ReplicaClient).SnapshotPurgeStatus)
	// Asked one after the other, the slow replicas alone would take 3 * delay.
	if elapsed := time.Since(start); elapsed >= 2*delay {
		t.Errorf("took %v, want less than %v", elapsed, 2*delay)
	}

	if len(results) != 4 {
		t.Fatalf("got %d results, want 4", len(results))
	}
	for _, address := range slow {
		if result := results[address]; result.err != nil || result.resp.State != "complete" {
			t.Errorf("%v: resp, err = %v, %v, want state complete", address, result.resp, result.err)
		}
	}
	if result := results[refused]; status.Code(result.err) != codes.Unavailable {
		t.Errorf("%v: err = %v, want code Unavailable", refused, result.err)
	}
}
