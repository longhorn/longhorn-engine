package main

import (
	"fmt"

	"github.com/docker/go-units"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine-launcher/api"
	"github.com/longhorn/longhorn-engine-launcher/rpc"
)

func EngineCmd() cli.Command {
	return cli.Command{
		Name: "engine",
		Subcommands: []cli.Command{
			EngineCreateCmd(),
			EngineGetCmd(),
			FrontendStartCallbackCmd(),
			FrontendShutdownCallbackCmd(),
		},
	}
}

func EngineCreateCmd() cli.Command {
	return cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "volume-name",
			},
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "binary",
			},
			cli.StringFlag{
				Name: "size",
			},
			cli.StringFlag{
				Name: "listen",
			},
			cli.StringFlag{
				Name: "listen-addr",
			},
			cli.StringFlag{
				Name:  "frontend",
				Usage: "Supports tgt-blockdev or tgt-iscsi, or leave it empty to disable frontend",
			},
			cli.StringSliceFlag{
				Name:  "enable-backend",
				Value: (*cli.StringSlice)(&[]string{"tcp"}),
			},
			cli.StringSliceFlag{
				Name: "replica",
			},
		},
		Action: func(c *cli.Context) {
			if err := createEngine(c); err != nil {
				logrus.Fatalf("Error running engine create command: %v.", err)
			}
		},
	}
}

func createEngine(c *cli.Context) error {
	name := c.String("name")
	volumeName := c.String("volume-name")
	binary := c.String("binary")
	backends := c.StringSlice("enable-backend")
	replicas := c.StringSlice("replica")
	frontend := c.String("frontend")
	if name == "" || volumeName == "" || binary == "" {
		return fmt.Errorf("missing required parameter")
	}

	listen := c.String("listen")
	listenAddr := c.String("listen-addr")
	if listenAddr != "" {
		listenAddr = listenAddr + ":"
	}
	if listen == "" && listenAddr == "" {
		return fmt.Errorf("missing required parameter")
	}

	sizeString := c.String("size")
	if sizeString == "" {
		return fmt.Errorf("Invalid empty size")
	}
	size, err := units.RAMInBytes(sizeString)
	if err != nil {
		return err
	}

	url := c.GlobalString("url")
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to %v: %v", url, err)
	}
	defer conn.Close()

	client := rpc.NewLonghornEngineServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), FrontendTimeout)
	defer cancel()

	obj, err := client.EngineCreate(ctx, &rpc.EngineCreateRequest{
		Spec: &rpc.EngineSpec{
			Name:       name,
			VolumeName: volumeName,
			Binary:     binary,
			Listen:     listen,
			ListenAddr: listenAddr,
			Size:       size,
			Frontend:   frontend,
			Backends:   backends,
			Replicas:   replicas,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start process: %v", err)
	}
	return printJSON(RPCToEngine(obj))
}

func EngineGetCmd() cli.Command {
	return cli.Command{
		Name: "get",
		Action: func(c *cli.Context) {
			if err := getEngine(c); err != nil {
				logrus.Fatalf("Error running engine get command: %v.", err)
			}
		},
	}
}

func getEngine(c *cli.Context) error {
	return fmt.Errorf("not implemented")
}

func FrontendStartCallbackCmd() cli.Command {
	name := "frontend-start-callback"
	return cli.Command{
		Name: name,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := startFrontendCallback(c); err != nil {
				logrus.Fatalf("Error running %v command: %v.", name, err)
			}
		},
	}
}

func startFrontendCallback(c *cli.Context) error {
	id := c.String("id")
	if id == "" {
		return fmt.Errorf("missing parameter id")
	}

	url := c.GlobalString("url")
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to %v: %v", url, err)
	}
	defer conn.Close()

	client := rpc.NewLonghornEngineServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), FrontendTimeout)
	defer cancel()

	if _, err := client.FrontendStartCallback(ctx, &rpc.EngineRequest{
		Name: id,
	}); err != nil {
		return fmt.Errorf("failed to start frontend: %v", err)
	}
	return nil
}

func FrontendShutdownCallbackCmd() cli.Command {
	name := "frontend-shutdown-callback"
	return cli.Command{
		Name: name,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := shutdownFrontendCallback(c); err != nil {
				logrus.Fatalf("Error running %v command: %v.", name, err)
			}
		},
	}
}

func shutdownFrontendCallback(c *cli.Context) error {
	id := c.String("id")
	if id == "" {
		return fmt.Errorf("missing parameter id")
	}

	url := c.GlobalString("url")
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to %v: %v", url, err)
	}
	defer conn.Close()

	client := rpc.NewLonghornEngineServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), FrontendTimeout)
	defer cancel()

	if _, err := client.FrontendShutdownCallback(ctx, &rpc.EngineRequest{
		Name: id,
	}); err != nil {
		return fmt.Errorf("failed to start frontend: %v", err)
	}
	return nil
}

func RPCToEngine(obj *rpc.EngineResponse) *api.Engine {
	return &api.Engine{
		Name:       obj.Spec.Name,
		VolumeName: obj.Spec.VolumeName,
		Binary:     obj.Spec.Binary,
		ListenAddr: obj.Spec.ListenAddr,
		Listen:     obj.Spec.Listen,
		Size:       obj.Spec.Size,
		Frontend:   obj.Spec.Frontend,
		Backends:   obj.Spec.Backends,
		Replicas:   obj.Spec.Replicas,

		Endpoint: obj.Status.Endpoint,
	}
}
