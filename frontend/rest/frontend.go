package rest

import (
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/rancher/longhorn/types"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "rest-frontend"})
)

type Device struct {
	Name       string
	Size       int64
	SectorSize int64

	isUp    bool
	backend types.ReaderWriterAt
}

func New() types.Frontend {
	return &Device{}
}

func (d *Device) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	d.Name = name
	d.backend = rw
	d.Size = size
	d.SectorSize = sectorSize

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
