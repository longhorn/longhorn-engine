package controller

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

var (
	ErrNoBackend = errors.New("No backend available")
)

type replicator struct {
	backendsAvailable bool
	backends          map[string]backendWrapper
	writerIndex       map[int]string
	readerIndex       map[int]string
	readers           []io.ReaderAt
	writer            io.WriterAt
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

func (r *replicator) AddBackend(address string, backend types.Backend) {
	if _, ok := r.backends[address]; ok {
		return
	}

	logrus.Infof("Adding backend: %s", address)

	if r.backends == nil {
		r.backends = map[string]backendWrapper{}
	}

	r.backends[address] = backendWrapper{
		backend: backend,
		mode:    types.WO,
	}

	r.buildReadWriters()
}

func (r *replicator) RemoveBackend(address string) {
	backend, ok := r.backends[address]
	if !ok {
		return
	}

	logrus.Infof("Removing backend: %s", address)

	backend.backend.Close()
	delete(r.backends, address)
	r.buildReadWriters()
}

func (r *replicator) ReadAt(buf []byte, off int64) (int, error) {
	if !r.backendsAvailable {
		return 0, ErrNoBackend
	}

	index := r.next
	r.next++
	if index >= len(r.readers) {
		r.next = 0
		index = 0
	}
	n, err := r.readers[index].ReadAt(buf, off)
	if err != nil {
		logrus.Error("Replicator.ReadAt:", index, err)
		return n, &BackendError{
			Errors: map[string]error{
				r.readerIndex[index]: err,
			},
		}
	}
	return n, err
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

func (r *replicator) buildReadWriters() {
	r.reset(false)

	readers := []io.ReaderAt{}
	writers := []io.WriterAt{}

	for address, b := range r.backends {
		if b.mode != types.ERR {
			r.writerIndex[len(writers)] = address
			writers = append(writers, b.backend)
		}
		if b.mode == types.RW {
			r.readerIndex[len(readers)] = address
			readers = append(readers, b.backend)
		}
	}

	r.writer = &MultiWriterAt{
		writers: writers,
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
	r.buildReadWriters()
}

func (r *replicator) Snapshot(name string) error {
	for _, wrapper := range r.backends {
		if wrapper.mode == types.ERR {
			return errors.New("Can not snapshot while a replica is in ERR")
		}
	}

	var lastErr error
	wg := sync.WaitGroup{}

	for _, backend := range r.backends {
		wg.Add(1)
		go func(backend types.Backend) {
			if err := backend.Snapshot(name); err != nil {
				lastErr = err
			}
			wg.Done()
		}(backend.backend)
	}

	wg.Wait()
	return lastErr
}

func (r *replicator) Close() error {
	var lastErr error
	for _, backend := range r.backends {
		if backend.mode == types.ERR {
			continue
		}
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
	r.writerIndex = map[int]string{}
	r.readerIndex = map[int]string{}

	if full {
		r.backends = nil
	}
}

type backendWrapper struct {
	backend types.Backend
	mode    types.Mode
}
