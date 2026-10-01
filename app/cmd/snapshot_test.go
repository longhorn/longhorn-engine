package cmd

import (
	"context"
	"testing"

	"github.com/urfave/cli/v3"
)

func newTestCommand(t *testing.T, args []string) *cli.Command {
	t.Helper()

	var parsedCmd *cli.Command
	cmd := &cli.Command{
		Name: "test",
		Action: func(ctx context.Context, c *cli.Command) error {
			parsedCmd = c
			return nil
		},
	}

	if err := cmd.Run(context.Background(), args); err != nil {
		t.Fatalf("failed to run test command: %v", err)
	}

	if parsedCmd == nil {
		t.Fatal("failed to capture parsed command")
	}

	return parsedCmd
}

func TestRevertSnapshotWithNoArgs(t *testing.T) {
	cmd := newTestCommand(t, []string{"test"})

	// Call revertSnapshot with no arguments - should return error, not panic
	err := revertSnapshot(cmd)

	// Should return an error about missing snapshot name, not panic
	if err == nil {
		t.Fatal("Expected an error when no arguments provided, but got nil")
	}

	expectedError := "snapshot name is required"
	if err.Error() != expectedError {
		t.Fatalf("Expected error message '%s', but got '%s'", expectedError, err.Error())
	}
}

func TestRevertSnapshotWithEmptyStringArg(t *testing.T) {
	cmd := newTestCommand(t, []string{"test", ""})

	// Call revertSnapshot with empty string argument
	err := revertSnapshot(cmd)

	// Should return an error about missing parameter
	if err == nil {
		t.Fatal("Expected an error when empty string argument provided, but got nil")
	}

	expectedError := "missing parameter for snapshot"
	if err.Error() != expectedError {
		t.Fatalf("Expected error message '%s', but got '%s'", expectedError, err.Error())
	}
}
