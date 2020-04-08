package cmd

import (
	"errors"
	"fmt"
	"log"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine/pkg/backend/dynamic"
	"github.com/longhorn/longhorn-engine/pkg/backend/file"
	"github.com/longhorn/longhorn-engine/pkg/backend/remote"
	"github.com/longhorn/longhorn-engine/pkg/controller"
	controllerrpc "github.com/longhorn/longhorn-engine/pkg/controller/rpc"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
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
			cli.BoolFlag{
				Name: "upgrade",
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
	isUpgrade := c.Bool("upgrade")

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
		f, ok := controller.Frontends[frontendName]
		if !ok {
			return fmt.Errorf("Failed to find frontend: %s", frontendName)
		}
		frontend = f
	}

	control := controller.NewController(name, dynamic.New(factories), frontend, isUpgrade)

	// need to wait for Shutdown() completion
	control.ShutdownWG.Add(1)
	addShutdown(func() (err error) {
		defer control.ShutdownWG.Done()
		logrus.Debugf("Starting to execute shutdown function for the engine controller of volume %v", name)
		return control.Shutdown()
	})

	if len(replicas) > 0 {
		logrus.Infof("Starting with replicas %q", replicas)
		if err := control.Start(replicas...); err != nil {
			log.Fatal(err)
		}
	}

	control.GRPCAddress = util.GetGRPCAddress(listen)
	control.GRPCServer = controllerrpc.GetControllerGRPCServer(control)

	control.StartGRPCServer()
	return control.WaitForShutdown()
}
