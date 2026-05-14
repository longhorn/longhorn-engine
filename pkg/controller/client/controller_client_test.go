package client

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

type fakeClientConn struct{}

func (f *fakeClientConn) Invoke(_ context.Context, method string, _, reply any, _ ...grpc.CallOption) error {
	if method != enginerpc.ControllerService_ControllerReplicaCreate_FullMethodName {
		return errors.New("unexpected grpc method: " + method)
	}

	controllerReplica, ok := reply.(*enginerpc.ControllerReplica)
	if !ok {
		return errors.New("unexpected reply type")
	}

	// Simulate the malformed success response seen during rebuild storms:
	// the RPC succeeds, but the replica payload omits its address.
	*controllerReplica = enginerpc.ControllerReplica{}
	return nil
}

func (f *fakeClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("streaming is not implemented in fakeClientConn")
}

func TestGetControllerReplicaInfoRejectsMalformedResponses(t *testing.T) {
	testCases := map[string]*enginerpc.ControllerReplica{
		"nil replica":     nil,
		"missing address": {},
	}

	for name, replica := range testCases {
		t.Run(name, func(t *testing.T) {
			info, err := GetControllerReplicaInfo(replica)
			if err == nil {
				t.Fatalf("GetControllerReplicaInfo() returned nil error, got info=%v", info)
			}
			if info != nil {
				t.Fatalf("GetControllerReplicaInfo() returned info=%v, want nil", info)
			}
		})
	}
}

func TestReplicaCreateRejectsMalformedReplicaResponse(t *testing.T) {
	controllerClient := &ControllerClient{
		serviceURL: "tcp://engine.example",
		ControllerServiceContext: ControllerServiceContext{
			service: enginerpc.NewControllerServiceClient(&fakeClientConn{}),
		},
	}

	replica, err := controllerClient.ReplicaCreate("tcp://replica.example", true, types.WO)
	if err == nil {
		t.Fatalf("ReplicaCreate() returned nil error, got replica=%v", replica)
	}
	if replica != nil {
		t.Fatalf("ReplicaCreate() returned replica=%v, want nil", replica)
	}
	if !strings.Contains(err.Error(), "failed to decode created replica") {
		t.Fatalf("ReplicaCreate() error = %q, want it to mention decode failure", err)
	}
}
