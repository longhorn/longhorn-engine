package controller

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

type backendSizeResult struct {
	size int64
	err  error
}

type backendClassificationTestBackend struct {
	startupTestBackend
	sizeResults []backendSizeResult
	sizeCalls   int
	sectorErr   error
	state       string
	stateErr    error
}

func (b *backendClassificationTestBackend) Size() (int64, error) {
	result := b.sizeResults[len(b.sizeResults)-1]
	if b.sizeCalls < len(b.sizeResults) {
		result = b.sizeResults[b.sizeCalls]
	}
	b.sizeCalls++
	return result.size, result.err
}

func (b *backendClassificationTestBackend) SectorSize() (int64, error) {
	return 512, b.sectorErr
}

func (b *backendClassificationTestBackend) GetState() (string, error) {
	if b.state == "" {
		b.state = string(types.ReplicaStateOpen)
	}
	return b.state, b.stateErr
}

type backendClassificationTestFactory struct {
	backends    map[string]*backendClassificationTestBackend
	createError map[string]error
}

func (f *backendClassificationTestFactory) Create(_ string, address string, _ types.DataServerProtocol,
	_ types.SharedTimeouts, _ bool, _ int64) (types.Backend, error) {
	if err := f.createError[address]; err != nil {
		return nil, err
	}
	return f.backends[address], nil
}

func newBackendClassificationTestController(factory types.BackendFactory) *Controller {
	return &Controller{
		factory:                 factory,
		revisionCounterDisabled: true,
		metrics:                 &types.Metrics{},
	}
}

func (s *TestSuite) TestStartClassifiesBackendServiceUnavailable(c *C) {
	unavailable := status.Error(codes.Unavailable, "backend unavailable")
	unknown := errors.New("invalid backend")

	testCases := []struct {
		name          string
		factory       *backendClassificationTestFactory
		expectedError string
	}{
		{
			name: "all create calls unavailable",
			factory: &backendClassificationTestFactory{
				createError: map[string]error{"replica-1": unavailable, "replica-2": unavailable},
			},
			expectedError: ControllerErrorNoBackendServiceUnavailable + ".*",
		},
		{
			name: "all first size calls unavailable",
			factory: &backendClassificationTestFactory{
				backends: map[string]*backendClassificationTestBackend{
					"replica-1": {sizeResults: []backendSizeResult{{err: unavailable}}},
					"replica-2": {sizeResults: []backendSizeResult{{err: unavailable}}},
				},
			},
			expectedError: ControllerErrorNoBackendServiceUnavailable + ".*",
		},
		{
			name: "all sector size calls unavailable",
			factory: &backendClassificationTestFactory{
				backends: map[string]*backendClassificationTestBackend{
					"replica-1": {sizeResults: []backendSizeResult{{size: 4096}}, sectorErr: unavailable},
					"replica-2": {sizeResults: []backendSizeResult{{size: 4096}}, sectorErr: unavailable},
				},
			},
			expectedError: ControllerErrorNoBackendServiceUnavailable + ".*",
		},
		{
			name: "all state calls unavailable",
			factory: &backendClassificationTestFactory{
				backends: map[string]*backendClassificationTestBackend{
					"replica-1": {sizeResults: []backendSizeResult{{size: 4096}}, stateErr: unavailable},
					"replica-2": {sizeResults: []backendSizeResult{{size: 4096}}, stateErr: unavailable},
				},
			},
			expectedError: ControllerErrorNoBackendServiceUnavailable + ".*",
		},
		{
			name: "all second size calls unavailable",
			factory: &backendClassificationTestFactory{
				backends: map[string]*backendClassificationTestBackend{
					"replica-1": {sizeResults: []backendSizeResult{{size: 4096}, {err: unavailable}}},
					"replica-2": {sizeResults: []backendSizeResult{{size: 4096}, {err: unavailable}}},
				},
			},
			expectedError: ControllerErrorNoBackendServiceUnavailable + ".*",
		},
		{
			name: "mixed unavailable and non-connectivity failure",
			factory: &backendClassificationTestFactory{
				createError: map[string]error{"replica-1": unavailable},
				backends: map[string]*backendClassificationTestBackend{
					"replica-2": {sizeResults: []backendSizeResult{{err: unknown}}},
				},
			},
			expectedError: ControllerErrorNoBackendReplicaError + ".*",
		},
		{
			name: "mixed unavailable and invalid replica state",
			factory: &backendClassificationTestFactory{
				createError: map[string]error{"replica-1": unavailable},
				backends: map[string]*backendClassificationTestBackend{
					"replica-2": {sizeResults: []backendSizeResult{{size: 4096}}, state: string(types.ReplicaStateClosed)},
				},
			},
			expectedError: ControllerErrorNoBackendReplicaError + ".*",
		},
	}

	for _, tc := range testCases {
		c.Logf("test case: %s", tc.name)
		controller := newBackendClassificationTestController(tc.factory)
		err := controller.Start(4096, 4096, "replica-1", "replica-2")
		c.Assert(err, ErrorMatches, tc.expectedError, Commentf(tc.name))
	}
}
