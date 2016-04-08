package controller

import (
	"io"
	"sync"
)

type MultiWriterAt struct {
	writers []io.WriterAt
}

type MultiWriterError struct {
	Index   int
	Writers []io.WriterAt
	Errors  []error
}

func (m *MultiWriterError) Error() string {
	for _, err := range m.Errors {
		if err != nil {
			return err.Error()
		}
	}
	return "Unknown"
}

func (m *MultiWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	errs := make([]error, len(m.writers))
	errored := false
	wg := sync.WaitGroup{}

	for i, w := range m.writers {
		wg.Add(1)
		go func(index int, w io.WriterAt) {
			_, err := w.WriteAt(p, off)
			if err != nil {
				errored = true
				errs[index] = err
			}
			wg.Done()
		}(i, w)
	}

	wg.Wait()
	if errored {
		return 0, &MultiWriterError{
			Writers: m.writers,
			Errors:  errs,
		}
	}

	return len(p), nil
}
