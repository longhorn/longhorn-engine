//go:build !linux

package wal

import "os"

// dataSync falls back to a full fsync on platforms that do not expose
// fdatasync(2).
func dataSync(f *os.File) error {
	return f.Sync()
}
