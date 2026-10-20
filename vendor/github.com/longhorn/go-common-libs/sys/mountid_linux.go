package sys

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Mocked in tests.
var statxMountIDFunc = statxMountID

// getMountID returns the ID of the mount containing path, as listed in the
// first field of mountinfo.
func getMountID(path string) (string, error) {
	id, ok, err := statxMountIDFunc(path)
	if err != nil || ok {
		return id, err
	}
	return fdinfoMountID(path)
}

// statxMountID returns the ID of the mount containing path and whether statx
// reported it, which it does since Linux 5.8 unless blocked by seccomp.
func statxMountID(path string) (string, bool, error) {
	var stx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, 0, unix.STATX_MNT_ID, &stx); err != nil {
		// statx is missing before Linux 4.11, and may be blocked by seccomp.
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
			return "", false, nil
		}
		return "", false, err
	}
	if stx.Mask&unix.STATX_MNT_ID == 0 {
		return "", false, nil
	}
	return strconv.FormatUint(stx.Mnt_id, 10), true, nil
}

// fdinfoMountID returns the ID of the mount containing path from the fdinfo of
// a file descriptor for it, which reports it since Linux 3.15.
func fdinfoMountID(path string) (string, error) {
	// O_PATH needs no permission on the file itself. Unlike statx, it only
	// triggers an automount on path itself with O_DIRECTORY, which is enough
	// as automount points are directories.
	fd, err := unix.Open(path, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOTDIR) {
		fd, err = unix.Open(path, unix.O_PATH|unix.O_CLOEXEC, 0)
	}
	if err != nil {
		return "", fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer unix.Close(fd) // nolint: errcheck // O_PATH descriptor.

	// /proc/thread-self is resolved when opened, and reading fdinfo fails once
	// that thread has exited, which it does if a goroutine locked to it exits,
	// so keep this goroutine on it until done.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	fdinfo := fmt.Sprintf("/proc/thread-self/fdinfo/%d", fd)
	content, err := os.ReadFile(fdinfo)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", fdinfo, err)
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		if id, ok := strings.CutPrefix(line, "mnt_id:"); ok {
			return strings.TrimSpace(id), nil
		}
	}
	return "", fmt.Errorf("no mnt_id in %s", fdinfo)
}
