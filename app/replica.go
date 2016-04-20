package app

import (
	"errors"
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/docker/go-units"
	"github.com/gorilla/handlers"
	"github.com/rancher/longhorn/replica"
	"github.com/rancher/longhorn/replica/rest"
	"github.com/rancher/longhorn/replica/rpc"
	"github.com/rancher/longhorn/util"
)

func ReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "replica",
		UsageText: "longhorn controller DIRECTORY SIZE",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9502",
			},
			cli.BoolFlag{
				Name: "debug",
			},
			cli.StringFlag{
				Name:  "size",
				Usage: "Volume size in bytes or human readable 42kb, 42mb, 42gb",
			},
		},
		Action: func(c *cli.Context) {
			if err := startReplica(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func startReplica(c *cli.Context) error {
	if c.NArg() != 1 {
		return errors.New("directory name is required")
	}

	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	dir := c.Args()[0]
	s := replica.NewServer(dir, 4096)

	address := c.String("listen")
	size := c.String("size")
	if size != "" {
		size, err := units.RAMInBytes(size)
		if err != nil {
			return err
		}

		if err := s.Open(size); err != nil {
			return err
		}
	}

	controlAddress, dataAddress, err := util.ParseAddresses(address)
	if err != nil {
		return err
	}

	resp := make(chan error)

	go func() {
		server := rest.NewServer(s)
		router := http.Handler(rest.NewRouter(server))
		router = handlers.LoggingHandler(os.Stdout, router)
		logrus.Infof("Listening on control %s", controlAddress)
		resp <- http.ListenAndServe(controlAddress, router)
	}()

	go func() {
		rpcServer := rpc.New(dataAddress, s)
		logrus.Infof("Listening on data %s", dataAddress)
		resp <- rpcServer.ListenAndServe()
	}()

	return <-resp
}
