package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateNewSnapshotMetafile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "volume-snap-restore.img.meta")
	created := "2026-10-06T00:00:00Z"

	if err := CreateNewSnapshotMetafile(file, created); err != nil {
		t.Fatalf("CreateNewSnapshotMetafile: %v", err)
	}

	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read meta file: %v", err)
	}
	var meta struct {
		Parent  string
		Created string
	}
	if err := json.Unmarshal(content, &meta); err != nil {
		t.Fatalf("unmarshal meta file %q: %v", content, err)
	}
	if meta.Parent != "" {
		t.Errorf("Parent = %q, want empty", meta.Parent)
	}
	if meta.Created != created {
		t.Errorf("Created = %q, want %q", meta.Created, created)
	}
	if _, err := os.Stat(file + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temporary file left behind: %v", err)
	}
}
