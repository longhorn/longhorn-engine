package socket

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/engine/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/engine/types"
)

const (
	frontendName = "socket"

	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"
)

func New() *Socket {
	return &Socket{}
}

type Socket struct {
	Volume     string
	Size       int64
	SectorSize int

	isUp         bool
	socketPath   string
	socketServer *dataconn.Server
}

func (t *Socket) FrontendName() string {
	return frontendName
}

func (t *Socket) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	t.Volume = name
	t.Size = size
	t.SectorSize = int(sectorSize)

	if err := t.Shutdown(); err != nil {
		return err
	}

	if err := t.startSocketServer(rw); err != nil {
		return err
	}

	t.isUp = true

	return nil
}

func (t *Socket) Shutdown() error {
	if t.Volume != "" {
		if t.socketServer != nil {
			logrus.Infof("Shutdown TGT socket server for %v", t.Volume)
			t.socketServer.Stop()
			t.socketServer = nil
		}
	}
	t.isUp = false

	return nil
}

func (t *Socket) State() types.State {
	if t.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (t *Socket) Endpoint() string {
	if t.isUp {
		return t.GetSocketPath()
	}
	return ""
}

func (t *Socket) GetSocketPath() string {
	if t.Volume == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-"+t.Volume+".sock")
}

func (t *Socket) startSocketServer(rw types.ReaderWriterAt) error {
	socketPath := t.GetSocketPath()
	if err := os.MkdirAll(filepath.Dir(socketPath), 0700); err != nil {
		return fmt.Errorf("Cannot create directory %v", filepath.Dir(socketPath))
	}
	// Check and remove existing socket
	if st, err := os.Stat(socketPath); err == nil && !st.IsDir() {
		if err := os.Remove(socketPath); err != nil {
			return err
		}
	}

	t.socketPath = socketPath
	go t.startSocketServerListen(rw)
	return nil
}

func (t *Socket) startSocketServerListen(rw types.ReaderWriterAt) error {
	ln, err := net.Listen("unix", t.socketPath)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			logrus.Errorln("Fail to accept socket connection")
			continue
		}
		go t.handleServerConnection(conn, rw)
	}
}

func (t *Socket) handleServerConnection(c net.Conn, rw types.ReaderWriterAt) {
	defer c.Close()

	server := dataconn.NewServer(c, NewDataProcessorWrapper(rw))
	logrus.Infoln("New data socket connnection established")
	if err := server.Handle(); err != nil && err != io.EOF {
		logrus.Errorln("Fail to handle socket server connection due to ", err)
	} else if err == io.EOF {
		logrus.Warnln("Socket server connection closed")
	}
}

type DataProcessorWrapper struct {
	rw types.ReaderWriterAt
}

func NewDataProcessorWrapper(rw types.ReaderWriterAt) DataProcessorWrapper {
	return DataProcessorWrapper{
		rw: rw,
	}
}

func (d DataProcessorWrapper) ReadAt(p []byte, off int64) (n int, err error) {
	return d.rw.ReadAt(p, off)
}

func (d DataProcessorWrapper) WriteAt(p []byte, off int64) (n int, err error) {
	return d.rw.WriteAt(p, off)
}

func (d DataProcessorWrapper) PingResponse() error {
	return nil
}

func (t *Socket) Upgrade(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	return fmt.Errorf("Upgrade is not supported")
}

func (t *Socket) Expand(size int64) error {
	return fmt.Errorf("Expand is not supported")
}
