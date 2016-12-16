package tgt

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/longhorn/rpc"
	"github.com/rancher/longhorn/types"
	"github.com/rancher/longhorn/util"
)

const (
	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"
)

func New() types.Frontend {
	return &Tgt{}
}

type Tgt struct {
	Volume     string
	Size       int64
	SectorSize int

	isUp         bool
	socketPath   string
	socketServer *rpc.Server
	scsiDevice   *ScsiDevice
}

func (t *Tgt) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	t.Volume = name
	t.Size = size
	t.SectorSize = int(sectorSize)

	if err := t.Shutdown(); err != nil {
		return err
	}

	if err := t.startSocketServer(rw); err != nil {
		return err
	}

	if err := t.startScsiDevice(); err != nil {
		return err
	}

	if err := t.createDev(); err != nil {
		return err
	}

	t.isUp = true

	return nil
}

func (t *Tgt) Shutdown() error {
	if t.Volume != "" {
		dev := t.getDev()
		if err := util.RemoveDevice(dev); err != nil {
			return fmt.Errorf("Fail to remove device %s: %v", dev, err)
		}
		if t.socketServer != nil {
			logrus.Infof("Shutdown TGT socket server for %v", t.Volume)
			t.socketServer.Stop()
			t.socketServer = nil
		}
		if err := StopScsi(t.Volume); err != nil {
			return fmt.Errorf("Fail to stop SCSI device: %v", err)
		}
	}
	t.isUp = false

	return nil
}

func (t *Tgt) State() types.State {
	if t.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (t *Tgt) getSocketPath() string {
	if t.Volume == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-"+t.Volume+".sock")
}

func (t *Tgt) startSocketServer(rw types.ReaderWriterAt) error {
	socketPath := t.getSocketPath()
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

func (t *Tgt) startSocketServerListen(rw types.ReaderWriterAt) error {
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

func (t *Tgt) handleServerConnection(c net.Conn, rw types.ReaderWriterAt) {
	defer c.Close()

	server := rpc.NewServer(c, rw)
	if err := server.Handle(); err != nil && err != io.EOF {
		logrus.Errorln("Fail to handle socket server connection due to ", err)
	} else if err == io.EOF {
		logrus.Warnln("Socket server connection closed")
	}
}

func (t *Tgt) getDev() string {
	return filepath.Join(DevPath, t.Volume)
}

func (t *Tgt) startScsiDevice() error {
	if t.scsiDevice == nil {
		bsOpts := fmt.Sprintf("size=%v", t.Size)
		scsiDev, err := NewScsiDevice(t.Volume, t.socketPath, "longhorn", bsOpts)
		if err != nil {
			return err
		}
		t.scsiDevice = scsiDev
	}
	if err := StartScsi(t.scsiDevice); err != nil {
		return err
	}
	logrus.Infof("SCSI device %s created", t.scsiDevice.Device)
	return nil
}

func (t *Tgt) createDev() error {
	if err := os.MkdirAll(DevPath, 0700); err != nil {
		logrus.Fatalln("Cannot create directory ", DevPath)
	}

	dev := t.getDev()
	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	if err := util.DuplicateDevice(t.scsiDevice.Device, dev); err != nil {
		return err
	}
	logrus.Infof("Device %s is ready", dev)
	return nil
}
