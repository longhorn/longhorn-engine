package dataconn

import (
	"net"
	"sync"
	"time"

	"libguestfs.org/libnbd"
)

type NbdClientWrapper struct {
	handles        []*libnbd.Libnbd
	conn           net.Conn
	maxConnections int
	next           int
	indexLock      sync.Mutex
	end            chan struct{}
	handlesAlive   chan struct{}
}

func NewNbdClientWrapper(conn net.Conn, engineToReplicaTimeout time.Duration, nbdEnabled int) *NbdClientWrapper {
	wrapper := &NbdClientWrapper{
		handles:        make([]*libnbd.Libnbd, nbdEnabled),
		conn:           conn,
		maxConnections: nbdEnabled,
		next:           0,
		end:            make(chan struct{}),
		handlesAlive:   make(chan struct{}),
	}
	go wrapper.handle()
	<-wrapper.handlesAlive
	return wrapper
}

func (w *NbdClientWrapper) handle() {
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
	w.handlesAlive <- struct{}{}
	for {
		<-w.end
		return
	}
}

func (w *NbdClientWrapper) ReadAt(buf []byte, offset int64) (int, error) {
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

func (w *NbdClientWrapper) WriteAt(buf []byte, offset int64) (int, error) {
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

func (w *NbdClientWrapper) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), nil

}

func (w *NbdClientWrapper) Close() {
	for i := 0; i < w.maxConnections; i++ {
		w.handles[i].Close()
	}
	w.end <- struct{}{}
}
