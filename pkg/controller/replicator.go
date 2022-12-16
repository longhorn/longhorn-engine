package controller

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

var (
	ErrNoBackend = errors.New("no backend available")
)

type replicator struct {
	backendsAvailable bool
	backends          map[string]backendWrapper
	writerIndex       map[int]string
	readerIndex       map[int]string
	unmapperIndex     map[int]string
	readers           []io.ReaderAt
	writer            io.WriterAt
	unmapper          types.UnmapperAt
	next              int
}

type BackendError struct {
	Errors map[string]error
}

func (b *BackendError) Error() string {
	errors := []string{}
	for address, err := range b.Errors {
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %s", address, err.Error()))
		}
	}

	switch len(errors) {
	case 0:
		return "Unknown"
	case 1:
		return errors[0]
	default:
		return strings.Join(errors, "; ")
	}
}

func (r *replicator) AddBackend(address string, backend types.Backend, mode types.Mode) {
	if _, ok := r.backends[address]; ok {
		return
	}

	logrus.Infof("Adding backend: %s", address)

	if r.backends == nil {
		r.backends = map[string]backendWrapper{}
	}

	r.backends[address] = backendWrapper{
		backend: backend,
		mode:    mode,
	}

	r.buildReaderWriterUnmappers()
}

func (r *replicator) RemoveBackend(address string) {
	backend, ok := r.backends[address]
	if !ok {
		return
	}

	logrus.Infof("Removing backend: %s", address)

	// We cannot wait for it's return because peer may not exists anymore
	// The backend may be nil if the mode is ERR
	if backend.backend != nil {
		// Stop the monitoring goroutine in the Controller
		backend.backend.StopMonitoring()

		go backend.backend.Close()
	}
	delete(r.backends, address)
	r.buildReaderWriterUnmappers()
}

func (r *replicator) ReadAt(buf []byte, off int64) (int, error) {
	var (
		n   int
		err error
	)

	if !r.backendsAvailable {
		return 0, ErrNoBackend
	}

	readersLen := len(r.readers)
	r.next = (r.next + 1) % readersLen
	index := r.next
	retError := &BackendError{
		Errors: map[string]error{},
	}
	for i := 0; i < readersLen; i++ {
		reader := r.readers[index]
		n, err = reader.ReadAt(buf, off)
		if err == nil {
			break
		}
		logrus.WithError(err).Errorf("Failed to read: index %v, off %v, len %v", index, off, len(buf))
		retError.Errors[r.readerIndex[index]] = err
		index = (index + 1) % readersLen
	}
	if len(retError.Errors) != 0 {
		return n, retError
	}
	return n, nil
}

func (r *replicator) WriteAt(p []byte, off int64) (int, error) {
	if !r.backendsAvailable {
		return 0, ErrNoBackend
	}

	n, err := r.writer.WriteAt(p, off)
	if err != nil {
		errors := map[string]error{
			r.writerIndex[0]: err,
		}
		if mErr, ok := err.(*MultiWriterError); ok {
			errors = map[string]error{}
			for index, err := range mErr.Errors {
				if err != nil {
					errors[r.writerIndex[index]] = err
				}
			}
		}
		return n, &BackendError{Errors: errors}
	}
	return n, err
}

func (r *replicator) UnmapAt(length uint32, off int64) (int, error) {
	if !r.backendsAvailable {
		return 0, ErrNoBackend
	}

	n, err := r.unmapper.UnmapAt(length, off)
	if err != nil {
		errs := map[string]error{
			r.unmapperIndex[0]: err,
		}
		if mErr, ok := err.(*MultiWriterError); ok {
			errs = map[string]error{}
			for index, err := range mErr.Errors {
				if err != nil {
					errs[r.unmapperIndex[index]] = err
				}
			}
		}
		return n, &BackendError{Errors: errs}
	}
	return n, err
}

func (r *replicator) buildReaderWriterUnmappers() {
	r.reset(false)

	readers := []io.ReaderAt{}
	writers := []io.WriterAt{}
	unmappers := []types.UnmapperAt{}

	for address, b := range r.backends {
		if b.mode != types.ERR {
			r.writerIndex[len(writers)] = address
			writers = append(writers, b.backend)
			r.unmapperIndex[len(unmappers)] = address
			unmappers = append(unmappers, b.backend)
		}
		if b.mode == types.RW {
			r.readerIndex[len(readers)] = address
			readers = append(readers, b.backend)
		}
	}

	r.writer = &MultiWriterAt{
		writers: writers,
	}
	r.unmapper = &MultiUnmapperAt{
		unmappers: unmappers,
	}
	r.readers = readers

	if len(r.readers) > 0 {
		r.backendsAvailable = true
	}
}

func (r *replicator) SetMode(address string, mode types.Mode) {
	b, ok := r.backends[address]
	if !ok {
		return
	}
	b.mode = mode
	r.backends[address] = b
	if mode == types.ERR {
		b.backend.StopMonitoring()
	}

	r.buildReaderWriterUnmappers()
}

func (r *replicator) Snapshot(name string, userCreated bool, created string, labels map[string]string) error {
	retErrorLock := sync.Mutex{}
	retError := &BackendError{
		Errors: map[string]error{},
	}
	wg := sync.WaitGroup{}

	for addr, backend := range r.backends {
		if backend.mode != types.ERR {
			wg.Add(1)
			go func(address string, backend types.Backend) {
				if err := backend.Snapshot(name, userCreated, created, labels); err != nil {
					retErrorLock.Lock()
					retError.Errors[address] = err
					retErrorLock.Unlock()
				}
				wg.Done()
			}(addr, backend.backend)
		}
	}

	wg.Wait()

	if len(retError.Errors) != 0 {
		return retError
	}
	return nil
}

// Expand tries to handle the expansion for all replicas, as well as the rollback result if the rollback is applied.
// It returns 1 boolean and 2 errors:
//   - The boolean indicates if the expansion succeeds or not.
//     As long as there is one replica expansion success, we will consider the backend expansion as success.
//     Otherwise we need to do manual rollback for these replicas, which make things more complicated.
//   - The 1st one indicates that the related replica is out of sync and it should be marked as ERROR state later;
//   - The 2nd one just records why the replica expansion/rollback fails.
//     The controller doesn't need to mark the related replica as ERROR state.
func (r *replicator) Expand(size int64) (bool, error, error) {
	errorLock := sync.Mutex{}
	errs := &BackendError{
		Errors: map[string]error{},
	}
	outOfSyncErrs := &BackendError{
		Errors: map[string]error{},
	}
	wg := sync.WaitGroup{}
	rwReplicaCount := 0

	for addr, backend := range r.backends {
		if backend.mode != types.ERR {
			wg.Add(1)
			rwReplicaCount++
			go func(address string, backend types.Backend) {
				if err := backend.Expand(size); err != nil {
					errWithCode, ok := err.(*types.Error)
					if !ok {
						errWithCode = types.NewError(types.ErrorCodeResultUnknown,
							err.Error(), "")
					}
					errorLock.Lock()
					errs.Errors[address] = errWithCode
					// - If the error code is not `ErrorCodeFunctionFailedRollbackSucceeded`,
					//   the related replica must be out of sync and it is no longer reusable.
					// - If the error code is `ErrorCodeFunctionFailedRollbackSucceeded`,
					//   it doesn't mean the related replica is in sync. The state of the replica
					//   depends on the final result and we will re-check `outOfSyncErrs` at that time.
					if errWithCode.Code != types.ErrorCodeFunctionFailedRollbackSucceeded {
						outOfSyncErrs.Errors[address] = errWithCode
					}
					errorLock.Unlock()

				}
				wg.Done()
			}(addr, backend.backend)
		}
	}

	wg.Wait()

	if len(errs.Errors) != 0 {
		// Only some of the replica expansion failed,
		// those related replicas were out of sync even if the rollback succeeded.
		if len(errs.Errors) < rwReplicaCount {
			outOfSyncErrs = errs
			logrus.Infof("Only some of the replica expansion failed: %v", outOfSyncErrs)
			return true, outOfSyncErrs, errs
		}
		// All replica expansion failed and some replica rollback failed,
		// only the replicas that failed the rollback were out of sync.
		if len(outOfSyncErrs.Errors) != 0 {
			logrus.Infof("All replica expansion failed and some replica rollback failed: %v", errs)
			return false, outOfSyncErrs, errs
		}
		// All replica expansion failed but all replicas rollback succeeded.
		logrus.Infof("All replica expansion failed but all replicas rollback succeeded: %v", errs)
		return false, nil, errs
	}

	logrus.Info("Succeeded to expand the backend")
	return true, nil, nil
}

func (r *replicator) Close() error {
	var lastErr error
	for _, backend := range r.backends {
		if backend.mode == types.ERR {
			continue
		}

		backend.backend.StopMonitoring()

		if err := backend.backend.Close(); err != nil {
			lastErr = err
		}
	}

	r.reset(true)

	return lastErr
}

func (r *replicator) reset(full bool) {
	r.backendsAvailable = false
	r.writer = nil
	r.unmapper = nil
	r.writerIndex = map[int]string{}
	r.readerIndex = map[int]string{}
	r.unmapperIndex = map[int]string{}

	if full {
		r.backends = nil
	}
}

type backendWrapper struct {
	backend types.Backend
	mode    types.Mode
}

func (r *replicator) RemainSnapshots() (int, error) {
	// addReplica may call here even without any backend
	if len(r.backends) == 0 {
		return 1, nil
	}

	ret := math.MaxInt32
	for _, backend := range r.backends {
		if backend.mode == types.ERR {
			continue
		}
		// ignore error and try next one. We can deal with all
		// error situation later
		if remain, err := backend.backend.RemainSnapshots(); err == nil {
			if remain < ret {
				ret = remain
			}
		}
	}
	if ret == math.MaxInt32 {
		return 0, fmt.Errorf("cannot get valid result for remain snapshot")
	}
	if ret <= 0 {
		return 0, fmt.Errorf("too many snapshots created")
	}

	return ret, nil
}

func (r *replicator) SetRevisionCounter(address string, counter int64) error {
	backend, ok := r.backends[address]
	if !ok {
		return fmt.Errorf("cannot find backend %v", address)
	}

	if err := backend.backend.SetRevisionCounter(counter); err != nil {
		return err
	}

	logrus.Infof("Set backend %s revision counter to %v", address, counter)

	return nil
}

func (r *replicator) GetRevisionCounter(address string) (int64, error) {
	backend, ok := r.backends[address]
	if !ok {
		return -1, fmt.Errorf("cannot find backend %v", address)
	}

	counter, err := backend.backend.GetRevisionCounter()
	if err != nil {
		return 0, err
	}
	logrus.Infof("Got backend %s revision counter %v", address, counter)

	return counter, nil
}

func (r *replicator) SetUnmapMarkSnapChainRemoved(address string, enabled bool) error {
	backend, ok := r.backends[address]
	if !ok {
		return fmt.Errorf("cannot find backend %v", address)
	}

	if err := backend.backend.SetUnmapMarkSnapChainRemoved(enabled); err != nil {
		return err
	}

	logrus.Infof("Set backend %s UnmapMarkSnapChainRemoved to %v", address, enabled)

	return nil
}

func (r *replicator) GetUnmapMarkSnapChainRemoved(address string) (bool, error) {
	backend, ok := r.backends[address]
	if !ok {
		return false, fmt.Errorf("cannot find backend %v", address)
	}

	enabled, err := backend.backend.GetUnmapMarkSnapChainRemoved()
	if err != nil {
		return false, err
	}
	logrus.Infof("Got backend %s UnmapMarkSnapChainRemoved %v", address, enabled)

	return enabled, nil
}
