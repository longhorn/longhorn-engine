package dataconn

import (
	"net"
	"sync"
	"time"

	"libguestfs.org/libnbd"
)

type nbdClientWrapper struct {
	handles        []*libnbd.Libnbd
	conn           net.Conn
	maxConnections int
	next           int
	indexLock      sync.Mutex
	end            chan struct{}
}

func NewNBDClientWrapper(conn net.Conn, engineToReplicaTimeout time.Duration, nbdEnabled int) *nbdClientWrapper {
	wrapper := &nbdClientWrapper{
		handles:        make([]*libnbd.Libnbd, nbdEnabled),
		conn:           conn,
		maxConnections: nbdEnabled,
		next:           0,
		end:            make(chan struct{}),
	}
	go wrapper.handle()

	return wrapper
}

func (w *nbdClientWrapper) handle() {
	for i := 0; i < w.maxConnections; i++ {
		h, err := libnbd.Create()
		if err != nil {
			panic(err)
		}

		w.handles[i] = h
		uri := "nbd://" + w.conn.RemoteAddr().String()
		err = w.handles[i].ConnectUri(uri)
		if err != nil {
			panic(err)
		}
	}

	for {
		<-w.end
		return
	}
}

func (w *nbdClientWrapper) ReadAt(buf []byte, offset int64) (int, error) {
	w.indexLock.Lock()
	w.next = (w.next + 1) % w.maxConnections
	index := w.next
	w.indexLock.Unlock()
	err := w.handles[index].Pread(buf, uint64(offset), nil)
	if err != nil {
		return 0, err
	}

	return len(buf), nil
}

func (w *nbdClientWrapper) WriteAt(buf []byte, offset int64) (int, error) {
	w.indexLock.Lock()
	w.next = (w.next + 1) % w.maxConnections
	index := w.next
	w.indexLock.Unlock()
	err := w.handles[index].Pwrite(buf, uint64(offset), nil)
	if err != nil {
		return 0, err
	}

	return len(buf), nil
}

func (w *nbdClientWrapper) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), nil

}

func (w *nbdClientWrapper) Close() {
	for i := 0; i < w.maxConnections; i++ {
		w.handles[i].Close()
	}
	w.end <- struct{}{}
}
