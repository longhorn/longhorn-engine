//go:build linux

package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// dataSync flushes the file's data and the metadata required to read it
// back (e.g. an extended size) using fdatasync(2). It is cheaper than a
// full fsync because it skips unrelated inode metadata (mtime/atime)
// that the WAL does not depend on.
func dataSync(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
