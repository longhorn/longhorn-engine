package nbd

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/pmorjan/kmod"
	nbd "github.com/pojntfx/go-nbd/pkg/server"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

const (
	frontendName = "nbd"

	SocketDirectory = "/var/run"
	DeviceDirectory = "/dev/longhorn/"
)

type Nbd struct {
	Volume     string
	Size       int64
	SectorSize int

	isUp          bool
	socketPath    string
	nbdDevicePath string
	connections   int
	clients       int
}

func New(frontendStreams int) types.Frontend {
	return &Nbd{connections: frontendStreams}
}

func (n *Nbd) FrontendName() string {
	return frontendName
}

func (n *Nbd) Init(name string, size, sectorSize int64) error {
	n.Volume = name
	n.Size = size
	n.SectorSize = int(sectorSize)

	return n.Shutdown()
}

func (n *Nbd) Startup(rwu types.ReaderWriterUnmapperAt) error {
	if err := n.startNbdServer(rwu); err != nil {
		return err
	}
	n.isUp = true

	// Load nbd module
	k, err := kmod.New()
	if err != nil {
		return err
	}
	if err := k.Load("nbd", "", 0); err != nil {
		return err
	}

	// Connect device
	n.nbdDevicePath, err = n.getNextFreeNbdDevice()
	if err != nil {
		return err
	}
	args := strings.Fields(fmt.Sprintf("/usr/sbin/nbd-client -N %s -u %s -b 4096 %s -C %d", n.Volume, n.socketPath, n.nbdDevicePath, n.connections))
	cmd := exec.Command(args[0], args[1:]...)
	if err := cmd.Run(); err != nil {
		return err
	}

	// Create symlink
	devicePath := n.GetDevicePath()
	logrus.Infof("linking %v to %v", devicePath, n.nbdDevicePath)
	if err := os.MkdirAll(DeviceDirectory, os.ModePerm); err != nil {
		return err
	}
	if _, err := os.Lstat(devicePath); err == nil {
		os.Remove(devicePath)
	}
	if err := os.Symlink(n.nbdDevicePath, devicePath); err != nil {
		return err
	}

	return nil
}

func (n *Nbd) getNextFreeNbdDevice() (string, error) {
	var nbdDevicePath string
	for i := 0; i < 16; i++ {
		nbdDevicePath = fmt.Sprintf("/dev/nbd%d", i)
		args := strings.Fields(fmt.Sprintf("/usr/sbin/nbd-client -c %s", nbdDevicePath))
		cmd := exec.Command(args[0], args[1:]...)
		if err := cmd.Run(); err != nil {
			return nbdDevicePath, nil
		}
	}
	return "", fmt.Errorf("no more nbd devices")
}

func (n *Nbd) Shutdown() error {
	if n.Volume == "" || !n.isUp {
		return nil
	}

	// Disconnect device
	args := strings.Fields(fmt.Sprintf("/usr/sbin/nbd-client -d %s", n.nbdDevicePath))
	cmd := exec.Command(args[0], args[1:]...)
	if err := cmd.Run(); err != nil {
		return err
	}

	// Delete symlink
	devicePath := n.GetDevicePath()
	if _, err := os.Lstat(devicePath); err == nil {
		os.Remove(devicePath)
	}

	if n.Volume != "" {
		logrus.Warnf("Shutting down nbd server for %v", n.Volume)
	}
	n.isUp = false

	return nil
}

func (n *Nbd) State() types.State {
	if n.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (n *Nbd) Endpoint() string {
	if n.isUp {
		return n.GetSocketPath()
	}
	return ""
}

func (n *Nbd) GetSocketPath() string {
	if n.Volume == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-nbd-"+n.Volume+".sock")
}

func (n *Nbd) GetDevicePath() string {
	if n.Volume == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(DeviceDirectory, n.Volume)
}

func (n *Nbd) startNbdServer(rwu types.ReaderWriterUnmapperAt) error {
	socketPath := n.GetSocketPath()
	if err := os.MkdirAll(filepath.Dir(socketPath), 0700); err != nil {
		return errors.Wrapf(err, "cannot create directory %v", filepath.Dir(socketPath))
	}
	// Check and remove existing socket
	if st, err := os.Stat(socketPath); err == nil && !st.IsDir() {
		if err := os.Remove(socketPath); err != nil {
			return err
		}
	}

	n.socketPath = socketPath
	go n.startNbdServerListen(rwu)
	return nil
}

func (n *Nbd) startNbdServerListen(rwu types.ReaderWriterUnmapperAt) error {
	ln, err := net.Listen("unix", n.socketPath)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			logrus.WithError(err).Error("Failed to accept nbd connection")
			continue
		}

		n.clients++

		logrus.Infof("%v clients connected", n.clients)

		go n.handleServerConnection(conn, rwu)
	}
}

func (n *Nbd) handleServerConnection(conn net.Conn, rwu types.ReaderWriterUnmapperAt) {
	defer func() {
		conn.Close()

		n.clients--

		if err := recover(); err != nil {
			logrus.Infof("Client disconnected with error: %v", err)
		}

		logrus.Infof("%v clients connected", n.clients)
	}()

	b := NewNbdBackend(rwu, n.Size)

	if err := nbd.Handle(
		conn,
		[]*nbd.Export{
			{
				Name:        "",
				Description: "Longhorn volume",
				Backend:     b,
			},
		},
		&nbd.Options{
			ReadOnly:           false,
			MinimumBlockSize:   4096,
			PreferredBlockSize: 4096,
			MaximumBlockSize:   4096,
		}); err != nil {
		logrus.WithError(err).Errorf("Failed to handle nbd connection")
	}
}

type Backend struct {
	rwu  types.ReaderWriterUnmapperAt
	size int64
}

func NewNbdBackend(rwu types.ReaderWriterUnmapperAt, size int64) *Backend {
	return &Backend{
		rwu:  rwu,
		size: size,
	}
}

func (b *Backend) ReadAt(p []byte, off int64) (n int, err error) {
	return b.rwu.ReadAt(p, off)
}

func (b *Backend) WriteAt(p []byte, off int64) (n int, err error) {
	return b.rwu.WriteAt(p, off)
}

func (b *Backend) Size() (int64, error) {
	return b.size, nil
}

func (b *Backend) Sync() error {
	return nil
}

func (n *Nbd) Upgrade(name string, size, sectorSize int64, rwu types.ReaderWriterUnmapperAt) error {
	return fmt.Errorf("upgrade is not supported")
}

func (n *Nbd) Expand(size int64) error {
	return fmt.Errorf("expand is not supported")
}
