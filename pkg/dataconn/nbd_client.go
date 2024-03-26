package dataconn

import (
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"libguestfs.org/libnbd"
)

type NbdClientController struct {
	handles        []*libnbd.Libnbd
	conn           net.Conn
	maxConnections int
	handleCh       chan *libnbd.Libnbd
	end            chan struct{}
	handlesAlive   chan struct{}
}

func NewNbdClientWrapper(conn net.Conn, engineToReplicaTimeout time.Duration, nbdEnabled int) *NbdClientController {
	wrapper := &NbdClientController{
		handles:        make([]*libnbd.Libnbd, nbdEnabled),
		conn:           conn,
		maxConnections: nbdEnabled,
		handleCh:       make(chan *libnbd.Libnbd, nbdEnabled),
		end:            make(chan struct{}),
		handlesAlive:   make(chan struct{}),
	}
	go wrapper.handle()
	<-wrapper.handlesAlive
	return wrapper
}

func (w *NbdClientController) handle() {
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
		w.handleCh <- h
	}
	w.handlesAlive <- struct{}{}
	<-w.end
}

func (w *NbdClientController) ReadAt(buf []byte, offset int64) (int, error) {
	h := <-w.handleCh
	err := h.Pread(buf, uint64(offset), nil)
	w.handleCh <- h
	if err != nil {
		logrus.WithError(err).Error("Failed to read from NBD server: ", w.conn.RemoteAddr().String())
		return 0, err
	}

	return len(buf), nil
}

func (w *NbdClientController) WriteAt(buf []byte, offset int64) (int, error) {
	h := <-w.handleCh
	err := h.Pwrite(buf, uint64(offset), nil)
	w.handleCh <- h
	if err != nil {
		logrus.WithError(err).Error("Failed to write to NBD server: ", w.conn.RemoteAddr().String())
		return 0, err
	}

	return len(buf), nil
}

func (w *NbdClientController) UnmapAt(length uint32, offset int64) (int, error) {
	return int(length), nil

}

func (w *NbdClientController) Close() {
	for i := 0; i < w.maxConnections; i++ {
		w.handles[i].Close()
	}
	w.end <- struct{}{}
}

// type NbdClientWrapper struct {
// 	handles        []*libnbd.Libnbd
// 	conn           net.Conn
// 	maxConnections int
// 	next           int32
// 	end            chan struct{}
// 	handlesAlive   chan struct{}
// }

// func NewNbdClientWrapper(conn net.Conn, engineToReplicaTimeout time.Duration, nbdEnabled int) *NbdClientWrapper {
// 	wrapper := &NbdClientWrapper{
// 		handles:        make([]*libnbd.Libnbd, nbdEnabled),
// 		conn:           conn,
// 		maxConnections: nbdEnabled,
// 		next:           0,
// 		end:            make(chan struct{}),
// 		handlesAlive:   make(chan struct{}),
// 	}
// 	go wrapper.handle()
// 	<-wrapper.handlesAlive
// 	return wrapper
// }

// func (w *NbdClientWrapper) handle() {
// 	for i := 0; i < w.maxConnections; i++ {
// 		h, err := libnbd.Create()
// 		if err != nil {
// 			panic(err)
// 		}

// 		w.handles[i] = h
// 		uri := "nbd://" + w.conn.RemoteAddr().String()
// 		err = w.handles[i].ConnectUri(uri)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// 	w.handlesAlive <- struct{}{}
// 	for {
// 		<-w.end
// 		return
// 	}
// }

// func (w *NbdClientWrapper) ReadAt(buf []byte, offset int64) (int, error) {
// 	index := atomic.AddInt32(&w.next, 1) % int32(w.maxConnections)
// 	err := w.handles[index].Pread(buf, uint64(offset), nil)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return len(buf), nil
// }

// func (w *NbdClientWrapper) WriteAt(buf []byte, offset int64) (int, error) {
// 	index := atomic.AddInt32(&w.next, 1) % int32(w.maxConnections)
// 	err := w.handles[index].Pwrite(buf, uint64(offset), nil)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return len(buf), nil
// }

// func (w *NbdClientWrapper) UnmapAt(length uint32, offset int64) (int, error) {
// 	return int(length), nil

// }

// func (w *NbdClientWrapper) Close() {
// 	for i := 0; i < w.maxConnections; i++ {
// 		w.handles[i].Close()
// 	}
// 	w.end <- struct{}{}
// }
