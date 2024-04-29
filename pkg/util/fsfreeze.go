package util

import (
	"io"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	lhexec "github.com/longhorn/go-common-libs/exec"
)

const (
	binaryFsfreeze          = "fsfreeze"
	notFrozenErrorSubstring = "Invalid argument"

	// fsfreeze cannot be cancelled. Once it is started, we must wait for it to complete. If we do not, unfreeze will
	// wait for it anyway.
	freezeTimeout = -1

	// If the block device is functioning and the filesystem is frozen, fsfreeze -u immediately returns successfully.
	// If the block device is NOT functioning, fsfreeze does not return until I/O errors occur (which can take a long
	// time). In certain situations (e.g. when it is executed during an instance-manager shutdown that has already
	// stopped the associated replica so that I/Os will eventually time out), waiting can impede the shutdown sequence.
	unfreezeTimeout = 5 * time.Second
)

func NewDiscardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.Out = io.Discard
	return logger
}

// AttemptFreezeFilesystem attempts to freeze the filesystem mounted at freezePoint. If it fails, it logs, attempts to
// unfreeze the filesystem, and returns false.
func AttemptFreezeFilesystem(freezePoint string, exec lhexec.ExecuteInterface) error {
	if exec == nil {
		exec = lhexec.NewExecutor()
	}
	_, err := exec.Execute([]string{}, binaryFsfreeze, []string{"-f", freezePoint}, freezeTimeout)
	if err != nil {
		return err
	}
	return nil
}

// AttemptUnfreezeFilesystem attempts to unfreeze the filesystem mounted at freezePoint. There isn't really anything we
// can do about it if it fails, so log and return.
// AttemptUnfreezeFilesystem logs to the provided logger to simplify calling code. Pass nil instead to disable this
// behavior. expectSuccess controls the type of event and level AttemptUnfreezeFilesystem logs on.
func AttemptUnfreezeFilesystem(freezePoint string, exec lhexec.ExecuteInterface, expectSuccess bool,
	log logrus.FieldLogger) {
	if exec == nil {
		exec = lhexec.NewExecutor()
	}
	if log == nil {
		log = NewDiscardLogger()
	}

	if expectSuccess {
		log.Infof("Unfreezing filesystem mounted at %v", freezePoint)
	} else {
		log.Debugf("Attempting to unfreeze filesystem mounted at %v", freezePoint)
	}

	_, err := exec.Execute([]string{}, binaryFsfreeze, []string{"-u", freezePoint}, unfreezeTimeout)
	if err != nil {
		if strings.Contains(err.Error(), notFrozenErrorSubstring) {
			log.Debugf("Failed to unfreeze already unfrozen system mounted at %v", freezePoint)
		} else {
			// It the error message is related to a timeout, there is a decent chance the unfreeze will eventually be
			// successful. While we stop waiting for the unfreeze to complete, the unfreeze process itself cannot be
			// killed. This usually indicates the kernel is locked up waiting for I/O errors to be returned for an iSCSI
			// device that can no longer be reached.
			log.WithError(err).Warnf("Failed to unfreeze filesystem mounted at %v", freezePoint)
		}
	} else if !expectSuccess {
		log.Warnf("Unfroze filesystem mounted at %v", freezePoint)
	}
}
