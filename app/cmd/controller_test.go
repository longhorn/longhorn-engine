package cmd

import (
	"errors"
	"syscall"
	"testing"

	"github.com/longhorn/longhorn-engine/pkg/controller"
)

func TestGetControllerStartExitCode(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "backend service unavailable",
			err:      errors.New(controller.ControllerErrorNoBackendServiceUnavailable),
			expected: int(syscall.EHOSTUNREACH),
		},
		{
			name:     "backend replica error",
			err:      errors.New(controller.ControllerErrorNoBackendReplicaError),
			expected: int(syscall.ENODATA),
		},
		{
			name:     "other error",
			err:      errors.New("failed to start controller"),
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := getControllerStartExitCode(tc.err)
			if actual != tc.expected {
				t.Fatalf("expected exit code %d, got %d", tc.expected, actual)
			}
		})
	}
}
