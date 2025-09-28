package cmd

import (
	"flag"
	"testing"

	"github.com/urfave/cli"
)

func TestRevertSnapshotWithNoArgs(t *testing.T) {
	// Create a CLI context with no arguments
	app := cli.NewApp()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	ctx := cli.NewContext(app, flagSet, nil)

	// Call revertSnapshot with no arguments - should return error, not panic
	err := revertSnapshot(ctx)

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
	// Create a CLI context with an empty string argument
	app := cli.NewApp()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	err := flagSet.Parse([]string{""}) // Empty string argument
	if err != nil {
		t.Fatalf("Failed to parse flags: %v", err)
	}
	ctx := cli.NewContext(app, flagSet, nil)

	// Call revertSnapshot with empty string argument
	err = revertSnapshot(ctx)

	// Should return an error about missing parameter
	if err == nil {
		t.Fatal("Expected an error when empty string argument provided, but got nil")
	}

	expectedError := "missing parameter for snapshot"
	if err.Error() != expectedError {
		t.Fatalf("Expected error message '%s', but got '%s'", expectedError, err.Error())
	}
}
