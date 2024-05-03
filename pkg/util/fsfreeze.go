package util

import (
	"path/filepath"
	"strings"
	"time"

	"k8s.io/mount-utils"

	lhexec "github.com/longhorn/go-common-libs/exec"
	"github.com/longhorn/go-common-libs/types"
)

const (
	binaryFsfreeze          = "fsfreeze"
	notFrozenErrorSubstring = "Invalid argument"
	freezePointDirectory    = "/var/lib/longhorn/freeze" // We expect this to be INSIDE the container namespace.

	// If the block device is functioning and the filesystem is frozen, fsfreeze -u immediately returns successfully.
	// If the block device is NOT functioning, fsfreeze does not return until I/O errors occur (which can take a long
	// time). In certain situations (e.g. when it is executed during an instance-manager shutdown that has already
	// stopped the associated replica so that I/Os will eventually time out), waiting can impede the shutdown sequence.
	unfreezeTimeout = 5 * time.Second
)

// GetFreezePointFromEndpoint returns the absolute path to the canonical location we will try to mount a filesystem to
// before freezing it.
func GetFreezePointFromEndpoint(endpoint string) string {
	return filepath.Join(freezePointDirectory, filepath.Base(endpoint))
}

// AttemptFreezeFilesystem attempts to freeze the filesystem mounted at freezePoint.
func AttemptFreezeFilesystem(freezePoint string, exec lhexec.ExecuteInterface) error {
	if exec == nil {
		exec = lhexec.NewExecutor()
	}

	// fsfreeze cannot be cancelled. Once it is started, we must wait for it to complete. If we do not, unfreeze will
	// wait for it anyway.
	_, err := exec.Execute([]string{}, binaryFsfreeze, []string{"-f", freezePoint}, types.ExecuteNoTimeout)
	if err != nil {
		return err
	}
	return nil
}

// UnfreezeFilesystem attempts to unfreeze the filesystem mounted at freezePoint. It returns true if it
// successfully unfreezes a filesystem, false if there is no need to unfreeze a filesystem, and an error otherwise.
func UnfreezeFilesystem(freezePoint string, exec lhexec.ExecuteInterface) (bool, error) {
	if exec == nil {
		exec = lhexec.NewExecutor()
	}

	_, err := exec.Execute([]string{}, binaryFsfreeze, []string{"-u", freezePoint}, unfreezeTimeout)
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), notFrozenErrorSubstring) {
		return false, nil
	}
	// It the error message is related to a timeout, there is a decent chance the unfreeze will eventually be
	// successful. While we stop waiting for the unfreeze to complete, the unfreeze process itself cannot be killed.
	// This usually indicates the kernel is locked up waiting for I/O errors to be returned for an iSCSI device that can
	// no longer be reached.
	return false, err
}

// UnfreezeAndUnmountFilesystem attempts to unfreeze the filesystem mounted at freezePoint.
func UnfreezeAndUnmountFilesystem(freezePoint string, exec lhexec.ExecuteInterface,
	mounter mount.Interface) (bool, error) {
	if exec == nil {
		exec = lhexec.NewExecutor()
	}
	if mounter == nil {
		mounter = mount.New("")
	}

	unfroze, err := UnfreezeFilesystem(freezePoint, exec)
	if err != nil {
		return unfroze, err
	}
	return unfroze, mount.CleanupMountPoint(freezePoint, mounter, false)
}
