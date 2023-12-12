package controller

import (
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/types"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type MultiUnmapperAt struct {
	unmappers []types.UnmapperAt
}

type MultiUnmapperError struct {
	errors []error
}

func (m *MultiUnmapperError) Error() string {
	errors := []string{}
	for _, err := range m.errors {
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	switch len(errors) {
	case 0:
		return "Unknown"
	default:
		return strings.Join(errors, "; ")
	}
}

func (m *MultiUnmapperAt) UnmapAt(length uint32, off int64) (int, error) {
	errs := make([]error, len(m.unmappers))
	wg := sync.WaitGroup{}

	lock := &sync.Mutex{}
	unmappedSize := 0

	for i, u := range m.unmappers {
		wg.Add(1)
		go func(index int, u types.UnmapperAt) {
			n, err := u.UnmapAt(length, off)
			if err != nil {
				errs[index] = err
			}

			lock.Lock()
			defer func() {
				lock.Unlock()
				wg.Done()
			}()

			if unmappedSize == 0 {
				unmappedSize = n
			} else if unmappedSize != n {
				// Could log at debug level here, but a difference of 4096 is commonplace.  (Why is it?  TBD).
				// Log anything bigger than that, which is expected to be uncommon.
				if n > unmappedSize+diskutil.VolumeSectorSize || unmappedSize > n+diskutil.VolumeSectorSize {
					logrus.Warnf("One of the MultiUnmapper %v got a very different size %v from others %v", u, n, unmappedSize)
				}
			}
		}(i, u)
	}

	wg.Wait()

	var err error = nil
	for i := range errs {
		if errs[i] != nil {
			err = &MultiUnmapperError{
				errors: errs,
			}
			break
		}
	}

	return unmappedSize, err
}
