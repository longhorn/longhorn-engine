package diskutil

import (
	"fmt"
	"strings"
	"testing"
)

func TestGenerateExpansionSnapshotName(t *testing.T) {
	size := int64(115343360)

	name1 := GenerateExpansionSnapshotName(size)
	name2 := GenerateExpansionSnapshotName(size)

	// Must start with "expand-<size>-"
	prefix := fmt.Sprintf("expand-%d-", size)
	for _, name := range []string{name1, name2} {
		if !strings.HasPrefix(name, prefix) {
			t.Errorf("expected prefix %q, got %q", prefix, name)
		}
	}
	// Two calls with same size must produce different names
	if name1 == name2 {
		t.Errorf("expected unique names for same size, but both are %q", name1)
	}
}
