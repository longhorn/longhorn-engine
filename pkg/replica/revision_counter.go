package replica

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"

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

func (r *Replica) initRevisionCounter(ctx context.Context) error {
	if r.readOnly {
		return nil
	}

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
	// To avoid read from disk, apply atomic operations against the internal cache r.revisionCache every time counter needs to be updated
	r.revisionCache.Swap(counter)

	go func() {
		if r.revisionCounterDisabled {
			return
		}
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.revisionCounterReqChan:
				err = r.writeRevisionCounter(r.revisionCache.Load() + 1)
				r.revisionCounterAckChan <- err
				if err != nil {
					close(r.revisionCounterAckChan)
					return
				} else {
					r.revisionCache.Add(1)
				}
			}
		}
	}()

	return nil
}

func (r *Replica) IsRevCounterDisabled() bool {
	return r.revisionCounterDisabled
}

func (r *Replica) GetRevisionCounter() int64 {
	return r.revisionCache.Load()
}

// SetRevisionCounter can be invoked only when there is no pending IO.
// Typically, its caller, the engine process, will hold the lock before calling this function.
// And the engine lock holding means all writable replicas finished their IO.
// In other words, the engine lock holding means there is no pending IO.
func (r *Replica) SetRevisionCounter(counter int64) error {
	if err := r.writeRevisionCounter(counter); err != nil {
		return err
	}
	r.revisionCache.Swap(counter)
	return nil
}
