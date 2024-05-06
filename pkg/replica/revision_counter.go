package replica

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

const (
	revisionCounterFile             = "revision.counter"
	revisionFileMode    os.FileMode = 0600
	revisionBlockSize               = 4096
)

func (r *Replica) readRevisionCounter() (int64, error) {
	if r.revisionFile == nil {
		return 0, fmt.Errorf("BUG: revision file wasn't initialized")
	}

	buf := make([]byte, revisionBlockSize)
	_, err := r.revisionFile.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return 0, errors.Wrap(err, "failed to read from revision counter file")
	}
	counter, err := strconv.ParseInt(strings.Trim(string(buf), "\x00"), 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse revision counter file")
	}
	return counter, nil
}

var revisionCounterBuf = sparse.AllocateAligned(revisionBlockSize)

func (r *Replica) writeRevisionCounter(counter int64) error {
	if r.revisionFile == nil {
		return fmt.Errorf("BUG: revision file wasn't initialized")
	}

	counterBytes := []byte(strconv.FormatInt(counter, 10))
	copy(revisionCounterBuf, counterBytes)
	// Need to clear the part of the slice left over from the previous function call.
	// The maximum length of the string representation of an int64 is 20 bytes so only need to clear upto 20 bytes
	for i := len(counterBytes); i < 20; i++ {
		revisionCounterBuf[i] = 0
	}
	_, err := r.revisionFile.WriteAt(revisionCounterBuf, 0)
	if err != nil {
		return errors.Wrap(err, "failed to write to revision counter file")
	}
	return nil
}

func (r *Replica) openRevisionFile(isCreate bool) error {
	var err error
	r.revisionFile, err = sparse.NewDirectFileIoProcessor(r.diskPath(revisionCounterFile), os.O_RDWR, revisionFileMode, isCreate)
	return err
}

func (r *Replica) initRevisionCounter() error {
	if r.readOnly {
		return nil
	}

	r.revisionLock.Lock()
	defer r.revisionLock.Unlock()

	if _, err := os.Stat(r.diskPath(revisionCounterFile)); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// file doesn't exist yet
		if err := r.openRevisionFile(true); err != nil {
			return err
		}
		if err := r.writeRevisionCounter(0); err != nil {
			return err
		}
	} else if err := r.openRevisionFile(false); err != nil {
		return err
	}

	counter, err := r.readRevisionCounter()
	if err != nil {
		return err
	}
	// Don't use r.revisionCache directly
	// r.revisionCache is an internal cache, to avoid read from disk
	// every time when counter needs to be updated.
	// And it's protected by revisionLock
	r.revisionCache = counter
	return nil
}

func (r *Replica) IsRevCounterDisabled() bool {
	return r.revisionCounterDisabled
}

func (r *Replica) GetRevisionCounter() int64 {
	r.revisionLock.Lock()
	defer r.revisionLock.Unlock()

	counter, err := r.readRevisionCounter()
	if err != nil {
		logrus.WithError(err).Error("Failed to get revision counter")
		// -1 will result in the replica to be discarded
		return -1
	}
	r.revisionCache = counter
	return counter
}

func (r *Replica) SetRevisionCounter(counter int64) error {
	r.revisionLock.Lock()
	defer r.revisionLock.Unlock()

	if err := r.writeRevisionCounter(counter); err != nil {
		return err
	}

	r.revisionCache = counter
	return nil
}

func (r *Replica) increaseRevisionCounter() error {
	r.revisionLock.Lock()
	defer r.revisionLock.Unlock()

	if !r.revisionRefreshed {
		counter, err := r.readRevisionCounter()
		if err != nil {
			return err
		}
		logrus.Infof("Reloading the revision counter before processing the first write, the current revision cache is %v, the latest revision counter in file is %v",
			r.revisionCache, counter)
		r.revisionCache = counter
		r.revisionRefreshed = true
	}

	if err := r.writeRevisionCounter(r.revisionCache + 1); err != nil {
		return err
	}

	r.revisionCache++
	return nil
}
