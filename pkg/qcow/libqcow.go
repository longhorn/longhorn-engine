package qcow

// #cgo LDFLAGS: -lqcow -lz -pthread
// #include <zlib.h>
// #include <stdlib.h>
// #include <libqcow.h>
import "C"
import (
	"errors"
	"fmt"
	"io"
	"unsafe"
)

type Qcow struct {
	file *C.libqcow_file_t
}

func toError(e *C.libqcow_error_t) error {
	buf := [1024]C.char{}
	defer C.libqcow_error_free(&e)
	if C.libqcow_error_sprint(e, &buf[0], 1023) < 0 {
		return fmt.Errorf("unknown error: %v", e)
	}
	return errors.New(C.GoString(&buf[0]))
}

func Open(path string) (*Qcow, error) {
	var f *C.libqcow_file_t
	var qErr *C.libqcow_error_t
	if C.libqcow_file_initialize(&f, &qErr) != 1 {
		return nil, toError(qErr)
	}

	name := C.CString(path)
	defer C.free(unsafe.Pointer(name))

	if C.libqcow_file_open(f, name, C.LIBQCOW_OPEN_READ, &qErr) != 1 {
		C.libqcow_file_free(&f, nil)
		return nil, toError(qErr)
	}

	return &Qcow{file: f}, nil
}

func (q *Qcow) WriteAt(buf []byte, off int64) (int, error) {
	return 0, errors.New("unsupported operation")
}

func (q *Qcow) ReadAt(buf []byte, off int64) (int, error) {
	var qErr *C.libqcow_error_t
	ret := C.libqcow_file_read_buffer_at_offset(q.file, unsafe.Pointer(&buf[0]),
		C.size_t(len(buf)), C.off64_t(off), &qErr)
	if ret < 0 {
		return 0, toError(qErr)
	}
	if ret == 0 {
		return 0, io.EOF
	}
	return int(ret), nil
}

func (q *Qcow) UnmapAt(length uint32, off int64) (int, error) {
	return 0, errors.New("unsupported operation")
}

func (q *Qcow) Close() error {
	var qErr *C.libqcow_error_t
	if C.libqcow_file_close(q.file, &qErr) != 1 {
		return toError(qErr)
	}
	return nil
}

func (q Qcow) Size() (int64, error) {
	var result C.size64_t
	var qErr *C.libqcow_error_t
	if C.libqcow_file_get_media_size(q.file, &result, &qErr) != 1 {
		return 0, toError(qErr)
	}
	return int64(result), nil
}

func (q *Qcow) Fd() uintptr {
	return 0
}
