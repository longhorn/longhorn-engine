package controller

import (
	"io"
	"strings"
	"sync"
	"time"

	"github.com/rancher/sparse-tools/stats"
)

type MultiWriterAt struct {
	writers []io.WriterAt
}

type MultiWriterError struct {
	Writers []io.WriterAt
	Errors  []error
}

func (m *MultiWriterError) Error() string {
	errors := []string{}
	for _, err := range m.Errors {
		if err != nil {
			errors = append(errors, err.Error())
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

func (m *MultiWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	errs := make([]error, len(m.writers))
	errored := false
	wg := sync.WaitGroup{}

	for i, w := range m.writers {
		wg.Add(1)
		go func(index int, w io.WriterAt) {
			timeStart := time.Now()
			_, err := w.WriteAt(p, off)
			if err != nil {
				errored = true
				errs[index] = err
			}
			timeElapsed := time.Now().Sub(timeStart)
			stats.Sample(timeStart, timeElapsed, index, stats.OpWrite, len(p))
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
