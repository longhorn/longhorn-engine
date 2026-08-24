package diskutil

import (
	"fmt"
	"strings"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestGenerateExpansionSnapshotName(c *C) {
	const size = int64(4096)

	// All replicas of a volume share the same volume name, so the generated
	// name must be deterministic for a given volume name and size.
	name := GenerateExpansionSnapshotName("vol-a", size)
	c.Assert(name, Equals, fmt.Sprintf(expansionSnapshotInfix, size)+"-vol-a")
	c.Assert(name, Equals, GenerateExpansionSnapshotName("vol-a", size))

	// The name keeps the size-based infix as a prefix so the snapshot remains
	// identifiable as an expansion snapshot.
	c.Assert(strings.HasPrefix(name, fmt.Sprintf(expansionSnapshotInfix, size)+"-"), Equals, true)

	// Different volumes expanded to the same size must get different names so
	// they do not collide on a Snapshot CR within the same namespace.
	c.Assert(GenerateExpansionSnapshotName("vol-a", size),
		Not(Equals), GenerateExpansionSnapshotName("vol-b", size))

	// An empty volume name falls back to the size-only form so the result stays
	// a valid DNS-1123 name (no trailing dash).
	c.Assert(GenerateExpansionSnapshotName("", size), Equals, fmt.Sprintf(expansionSnapshotInfix, size))
}

func (s *TestSuite) TestGenerateExpansionSnapshotLabels(c *C) {
	const size = int64(8192)

	labels := GenerateExpansionSnapshotLabels(size)
	c.Assert(labels[replicaExpansionLabelKey], Equals, "8192")
}
