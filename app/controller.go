package app

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/urfave/cli"

	"github.com/rancher/longhorn-engine/backend/dynamic"
	"github.com/rancher/longhorn-engine/backend/file"
	"github.com/rancher/longhorn-engine/backend/remote"
	"github.com/rancher/longhorn-engine/controller"
	"github.com/rancher/longhorn-engine/controller/rest"
	"github.com/rancher/longhorn-engine/types"
	"github.com/rancher/longhorn-engine/util"
)

var (
	frontends = map[string]types.Frontend{}
)

func ControllerCmd() cli.Command {
	return cli.Command{
		Name: "controller",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9501",
			},
			cli.StringFlag{
				Name:  "frontend",
				Value: "",
			},
			cli.StringSliceFlag{
				Name:  "enable-backend",
				Value: (*cli.StringSlice)(&[]string{"tcp"}),
			},
			cli.StringSliceFlag{
				Name: "replica",
			},
			cli.StringFlag{
				Name: "launcher",
			},
		},
		Action: func(c *cli.Context) {
			if err := startController(c); err != nil {
				logrus.Fatalf("Error running controller command: %v.", err)
			}
		},
	}
}

func startController(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("volume name is required")
	}
	name := c.Args()[0]

	if !util.ValidVolumeName(name) {
		return errors.New("invalid target name")
	}

	listen := c.String("listen")
	backends := c.StringSlice("enable-backend")
	replicas := c.StringSlice("replica")
	frontendName := c.String("frontend")
	launcher := c.String("launcher")

	factories := map[string]types.BackendFactory{}
	for _, backend := range backends {
		switch backend {
		case "file":
			factories[backend] = file.New()
		case "tcp":
			factories[backend] = remote.New()
		default:
			logrus.Fatalf("Unsupported backend: %s", backend)
		}
	}

	var frontend types.Frontend
	if frontendName != "" {
		f, ok := frontends[frontendName]
		if !ok {
			return fmt.Errorf("Failed to find frontend: %s", frontendName)
		}
		frontend = f
	}

	control := controller.NewController(name, dynamic.New(factories), frontend, launcher)
	server := rest.NewServer(control)
	router := http.Handler(rest.NewRouter(server))

	router = util.FilteredLoggingHandler(map[string]struct{}{
		"/v1/volumes":  {},
		"/v1/replicas": {},
	}, os.Stdout, router)
	router = handlers.ProxyHeaders(router)

	if len(replicas) > 0 {
		logrus.Infof("Starting with replicas %q", replicas)
		if err := control.Start(replicas...); err != nil {
			log.Fatal(err)
		}
	}

	addShutdown(func() {
		control.Shutdown()
	})

	httpServer := &http.Server{
		Addr:    listen,
		Handler: router,
	}
	control.RestServer = httpServer
	control.StartRestServer()
	return control.WaitForShutdown()
}
