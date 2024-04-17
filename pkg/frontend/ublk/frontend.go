package ublk

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

const (
	frontendName = "ublk"

	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"
)

func New(frontendQueues int) *Ublk {
	return &Ublk{Queues: frontendQueues}
}

type Ublk struct {
	Volume     string
	Size       int64
	UblkID     int
	Queues     int
	QueueDepth int
	BlockSize  int
	DaemonPId  int

	isUp         bool
	socketPath   string
	socketServer *dataconn.Server
}

func (u *Ublk) FrontendName() string {
	return frontendName
}

func (u *Ublk) Init(name string, size, sectorSize int64) error {
	u.Volume = name
	u.Size = size

	return nil
}

func (u *Ublk) StartUblk() error {

	command := "add"
	args := []string{"-t", "longhorn", "-f", u.socketPath, "-s", strconv.FormatInt(u.Size, 10), "-d", "32", "-q", strconv.Itoa(u.Queues)}

	cmd := exec.Command("ublk", append([]string{command}, args...)...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logrus.Error("Error starting ublk:", err)
		return nil
	}

	logrus.Info("ublk started successfully")

	var jsonOutput map[string]interface{}
	err = json.Unmarshal(output, &jsonOutput)

	if err != nil {
		return err
	}

	u.UblkID = int(jsonOutput["dev_id"].(float64))
	u.DaemonPId = int(jsonOutput["daemon_pid"].(float64))
	u.Queues = int(jsonOutput["nr_hw_queues"].(float64))
	u.QueueDepth = int(jsonOutput["queue_depth"].(float64))
	u.BlockSize = int(jsonOutput["block_size"].(float64))

	u.isUp = true
	return nil
}

func (u *Ublk) Startup(rwu types.ReaderWriterUnmapperAt) error {
	if err := u.startSocketServer(rwu); err != nil {
		return err
	}
	go func() {
		err := u.StartUblk()
		if err != nil {
			logrus.Errorf("Failed to start ublk: %v", err)
		}
	}()

	return nil
}
func (u *Ublk) ShutdownUblk() {
	comm := "ublk"
	args := []string{"del", strconv.Itoa(u.UblkID)}

	cmd := exec.Command(comm, args...)
	logrus.Infof("Running command: %v", cmd.Args)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logrus.Errorf("Error stopping ublk: %v", err)
		return
	}
	logrus.Infof("ublk stopped successfully: %v", string(output))
}

func (u *Ublk) Shutdown() error {
	_, file, no, ok := runtime.Caller(1)
	if ok {
		logrus.Infof("\ncalled from %s#%d\n\n", file, no)
	}
	if u.Volume != "" {
		if u.socketServer != nil {
			logrus.Infof("Shutting down TGT socket server for %v", u.Volume)
			u.socketServer.Stop()
			u.socketServer = nil
		}
	}
	u.isUp = false

	go func() {
		u.ShutdownUblk()
	}()

	return nil
}

func (u *Ublk) State() types.State {
	if u.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (u *Ublk) Endpoint() string {
	if u.isUp {
		return u.GetSocketPath()
	}
	return ""
}

func (u *Ublk) GetSocketPath() string {
	if u.Volume == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-"+u.Volume+".sock")
}

func (u *Ublk) startSocketServer(rwu types.ReaderWriterUnmapperAt) error {
	socketPath := u.GetSocketPath()
	if err := os.MkdirAll(filepath.Dir(socketPath), 0700); err != nil {
		return errors.Wrapf(err, "cannot create directory %v", filepath.Dir(socketPath))
	}

	if st, err := os.Stat(socketPath); err == nil && !st.IsDir() {
		if err := os.Remove(socketPath); err != nil {
			return err
		}
	}

	u.socketPath = socketPath
	go func() {
		err := u.startSocketServerListen(rwu)
		if err != nil {
			logrus.Errorf("Failed to start socket server: %v", err)
		}
	}()
	return nil
}

func (u *Ublk) startSocketServerListen(rwu types.ReaderWriterUnmapperAt) error {
	ln, err := net.Listen("unix", u.socketPath)
	if err != nil {
		return err
	}
	defer func(ln net.Listener) {
		err := ln.Close()
		if err != nil {
			logrus.WithError(err).Error("Failed to close socket listener")
		}
	}(ln)

	for {
		conn, err := ln.Accept()
		if err != nil {
			logrus.WithError(err).Error("Failed to accept socket connection")
			continue
		}
		go u.handleServerConnection(conn, rwu)
	}
}

func (u *Ublk) handleServerConnection(c net.Conn, rwu types.ReaderWriterUnmapperAt) {
	defer func(c net.Conn) {
		err := c.Close()
		if err != nil {
			logrus.WithError(err).Error("Failed to close socket server connection")
		}
	}(c)

	server := dataconn.NewServer(c, NewDataProcessorWrapper(rwu))
	logrus.Info("New data socket connection established")
	if err := server.Handle(); err != nil && err != io.EOF {
		logrus.WithError(err).Errorf("Failed to handle socket server connection")
	} else if err == io.EOF {
		logrus.Warn("Socket server connection closed")
	}
}

type DataProcessorWrapper struct {
	rwu types.ReaderWriterUnmapperAt
}

func NewDataProcessorWrapper(rwu types.ReaderWriterUnmapperAt) DataProcessorWrapper {
	return DataProcessorWrapper{
		rwu: rwu,
	}
}

func (d DataProcessorWrapper) ReadAt(p []byte, off int64) (n int, err error) {
	return d.rwu.ReadAt(p, off)
}

func (d DataProcessorWrapper) WriteAt(p []byte, off int64) (n int, err error) {
	return d.rwu.WriteAt(p, off)
}

func (d DataProcessorWrapper) UnmapAt(length uint32, off int64) (n int, err error) {
	return d.rwu.UnmapAt(length, off)
}

func (d DataProcessorWrapper) PingResponse() error {
	return nil
}

func (u *Ublk) Upgrade(name string, size, sectorSize int64, rwu types.ReaderWriterUnmapperAt) error {
	return fmt.Errorf("upgrade is not supported")
}

func (u *Ublk) Expand(size int64) error {
	return fmt.Errorf("expand is not supported")
}
