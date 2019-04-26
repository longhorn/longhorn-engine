package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/rancher/longhorn-engine-launcher/rpc"
	"github.com/rancher/longhorn-engine/util"
)

const (
	UpgradeTimeout  = 120 * time.Second
	InfoTimeout     = 10 * time.Second
	FrontendTimeout = 60 * time.Second
)

func StartCmd() cli.Command {
	return cli.Command{
		Name: "start",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "longhorn-binary",
				Value: "/usr/local/bin/longhorn",
			},
			cli.StringFlag{
				Name:  "launcher-listen",
				Value: "localhost:9510",
			},
			cli.StringFlag{
				Name: "size",
			},
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9501",
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
			if err := start(c); err != nil {
				logrus.Fatalf("Error running start command: %v.", err)
			}
		},
	}
}

func UpgradeCmd() cli.Command {
	return cli.Command{
		Name: "upgrade",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "longhorn-binary",
			},
			cli.StringSliceFlag{
				Name: "replica",
			},
		},
		Action: func(c *cli.Context) {
			if err := upgrade(c); err != nil {
				logrus.Fatalf("Error running upgrade command: %v.", err)
			}
		},
	}
}

func InfoCmd() cli.Command {
	return cli.Command{
		Name: "info",
		Action: func(c *cli.Context) {
			if err := info(c); err != nil {
				logrus.Fatalf("Error running endpoint command: %v.", err)
			}
		},
	}
}

func FrontendStartCmd() cli.Command {
	return cli.Command{
		Name: "frontend-start",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := startFrontend(c); err != nil {
				logrus.Fatalf("Error running frontend-start command: %v.", err)
			}
		},
	}
}

func FrontendShutdownCmd() cli.Command {
	return cli.Command{
		Name: "frontend-shutdown",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := shutdownFrontend(c); err != nil {
				logrus.Fatalf("Error running frontend-start command: %v.", err)
			}
		},
	}
}

func FrontendSetCmd() cli.Command {
	return cli.Command{
		Name:  "frontend-set",
		Usage: "Set frontend to tgt-blockdev or tgt-iscsi and enable it. Only valid if no frontend has been set",
		Action: func(c *cli.Context) {
			if err := setFrontend(c); err != nil {
				logrus.Fatalf("Error running frontend-set command: %v.", err)
			}
		},
	}
}

func start(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("volume name is required")
	}
	name := c.Args()[0]

	launcherListen := c.String("launcher-listen")
	longhornBinary := c.String("longhorn-binary")

	listen := c.String("listen")
	backends := c.StringSlice("enable-backend")
	replicas := c.StringSlice("replica")
	frontend := c.String("frontend")

	sizeString := c.String("size")
	if sizeString == "" {
		return fmt.Errorf("Invalid empty size")
	}
	size, err := units.RAMInBytes(sizeString)
	if err != nil {
		return err
	}

	l, err := NewLauncher(launcherListen, longhornBinary, frontend, name, size)
	if err != nil {
		return err
	}
	id := util.UUID()
	controller := NewController(id, longhornBinary, name, listen, l.frontend, backends, replicas)
	if err := l.StartController(controller); err != nil {
		return err
	}
	if err := l.StartRPCServer(); err != nil {
		return err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Receive %v to exit", sig)
		l.Shutdown()
	}()

	return l.WaitForShutdown()
}

func upgrade(c *cli.Context) error {
	url := c.GlobalString("url")
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to %v: %v", url, err)
	}
	defer conn.Close()

	longhornBinary := c.String("longhorn-binary")
	replicas := c.StringSlice("replica")

	if longhornBinary == "" || len(replicas) == 0 {
		return fmt.Errorf("missing required parameters")
	}

	client := rpc.NewLonghornLauncherServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), UpgradeTimeout)
	defer cancel()

	if _, err := client.UpgradeEngine(ctx, &rpc.Engine{
		Binary:   longhornBinary,
		Replicas: replicas,
	}); err != nil {
		return fmt.Errorf("failed to upgrade: %v", err)
	}
	return nil
}

func info(c *cli.Context) error {
	url := c.GlobalString("url")
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to %v: %v", url, err)
	}
	defer conn.Close()

	client := rpc.NewLonghornLauncherServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), InfoTimeout)
	defer cancel()

	endpoint, err := client.GetInfo(ctx, &rpc.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get endpoint: %v", err)
	}

	output, err := json.MarshalIndent(endpoint, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func startFrontend(c *cli.Context) error {
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

	client := rpc.NewLonghornLauncherServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), FrontendTimeout)
	defer cancel()

	if _, err := client.StartFrontend(ctx, &rpc.Identity{
		ID: id,
	}); err != nil {
		return fmt.Errorf("failed to start frontend: %v", err)
	}
	return nil
}

func shutdownFrontend(c *cli.Context) error {
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

	client := rpc.NewLonghornLauncherServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), FrontendTimeout)
	defer cancel()

	if _, err := client.ShutdownFrontend(ctx, &rpc.Identity{
		ID: id,
	}); err != nil {
		return fmt.Errorf("failed to start frontend: %v", err)
	}
	return nil
}

func setFrontend(c *cli.Context) error {
	if c.NArg() == 0 {
		return fmt.Errorf("frontend is required as the first argument")
	}
	frontend := c.Args()[0]

	url := c.GlobalString("url")
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to %v: %v", url, err)
	}
	defer conn.Close()

	client := rpc.NewLonghornLauncherServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), FrontendTimeout)
	defer cancel()

	if _, err := client.SetFrontend(ctx, &rpc.Frontend{
		Frontend: frontend,
	}); err != nil {
		return fmt.Errorf("failed to start frontend: %v", err)
	}
	return nil
}

func main() {
	a := cli.NewApp()
	a.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "url",
			Value: "localhost:9510",
		},
		cli.BoolFlag{
			Name: "debug",
		},
	}
	a.Commands = []cli.Command{
		StartCmd(),
		UpgradeCmd(),
		InfoCmd(),
		FrontendStartCmd(),
		FrontendShutdownCmd(),
		FrontendSetCmd(),
	}
	if err := a.Run(os.Args); err != nil {
		logrus.Fatal("Error when executing command: ", err)
	}
}
