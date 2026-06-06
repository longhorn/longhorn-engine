package util

import (
	"os"

	"github.com/cockroachdb/errors"
)

// FsyncDir fsyncs a directory so that file create / rename / unlink
// entries within it are durable. POSIX requires this after os.Rename to
// guarantee crash recovery sees the new name.
func FsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "failed to open dir %v for fsync", dir)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return errors.Wrapf(err, "failed to fsync dir %v", dir)
	}
	return nil
}
