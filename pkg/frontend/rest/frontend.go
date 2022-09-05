package rest

import (
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

const (
	frontendName = "rest"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "rest-frontend"})
)

type Device struct {
	Name       string
	Size       int64
	SectorSize int64

	isUp    bool
	backend types.ReaderWriterUnmapperAt
}

func New() types.Frontend {
	return &Device{}
}

func (d *Device) FrontendName() string {
	return frontendName
}

func (d *Device) Init(name string, size, sectorSize int64) error {
	d.Name = name
	d.Size = size
	d.SectorSize = sectorSize
	return nil
}

func (d *Device) Startup(rwu types.ReaderWriterUnmapperAt) error {
	d.backend = rwu
	if err := d.start(); err != nil {
		return err
	}

	d.isUp = true
	return nil
}

func (d *Device) Shutdown() error {
	return d.stop()
}

func (d *Device) start() error {
	listen := "localhost:9414"
	server := NewServer(d)
	router := http.Handler(NewRouter(server))
	router = handlers.LoggingHandler(os.Stdout, router)
	router = handlers.ProxyHeaders(router)

	log.Infof("Rest Frontend listening on %s", listen)

	go func() {
		http.ListenAndServe(listen, router)
	}()
	return nil
}

func (d *Device) stop() error {
	d.isUp = false
	return nil
}

func (d *Device) State() types.State {
	if d.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func (d *Device) Endpoint() string {
	if d.isUp {
		return "http://localhost:9414"
	}
	return ""
}

func (d *Device) Upgrade(name string, size, sectorSize int64, rwu types.ReaderWriterUnmapperAt) error {
	return fmt.Errorf("upgrade is not supported")
}

func (d *Device) Expand(size int64) error {
	return fmt.Errorf("expand is not supported")
}
