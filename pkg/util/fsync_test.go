package util

import (
	"testing"
)

func TestFsyncDir(t *testing.T) {
	if err := FsyncDir(t.TempDir()); err != nil {
		t.Fatalf("FsyncDir: %v", err)
	}
}
