package controller

import (
	"io"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

type replicator struct {
	io.WriterAt

	backends     map[string]backendWrapper
	readers      []io.ReaderAt
	readerLength int
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

func (r *replicator) ReadAt(buf []byte, off int64) (n int, err error) {
	// Use a poor random algorithm by always selecting 0 :)
	return r.readers[0].ReadAt(buf, off)
}

func (r *replicator) buildReadWriters() {
	readers := []io.ReaderAt{}
	writers := make([]io.WriterAt, 0, len(r.backends))
	for _, b := range r.backends {
		writers = append(writers, b.backend)
		readers = append(readers, b.backend)
	}

	r.WriterAt = &MultiWriterAt{
		writers: writers,
	}
	r.readers = readers
	r.readerLength = len(readers)
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

func (r *replicator) Snapshot() error {
	var lastErr error
	wg := sync.WaitGroup{}

	for _, backend := range r.backends {
		wg.Add(1)
		go func(backend types.Backend) {
			if err := backend.Snapshot(); err != nil {
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

	r.reset()

	return lastErr
}

func (r *replicator) reset() {
	r.WriterAt = nil
	r.backends = nil
}

type backendWrapper struct {
	backend types.Backend
	mode    types.Mode
}
