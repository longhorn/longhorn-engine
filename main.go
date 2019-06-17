package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine-launcher/engine"
	"github.com/longhorn/longhorn-engine-launcher/process"
	"github.com/longhorn/longhorn-engine-launcher/rpc"
)

const (
	FrontendTimeout = 60 * time.Second
)

func StartLauncherCmd() cli.Command {
	return cli.Command{
		Name: "daemon",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:8500",
			},
			cli.StringFlag{
				Name:  "port-range",
				Value: "10000-30000",
			},
		},
		Action: func(c *cli.Context) {
			if err := startLauncher(c); err != nil {
				logrus.Fatalf("Error running start command: %v.", err)
			}
		},
	}
}

func startLauncher(c *cli.Context) error {
	listen := c.String("listen")
	portRange := c.String("port-range")

	shutdownCh := make(chan error)
	l, err := process.NewLauncher(portRange, shutdownCh)
	if err != nil {
		return err
	}
	s, err := engine.NewService(l, listen)
	if err != nil {
		return err
	}

	listenAt, err := net.Listen("tcp", listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	rpcService := grpc.NewServer()
	rpc.RegisterLonghornProcessLauncherServiceServer(rpcService, l)
	rpc.RegisterLonghornEngineServiceServer(rpcService, s)
	reflection.Register(rpcService)

	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.Errorf("Stopping due to %v:", err)
		}
		close(shutdownCh)
	}()
	logrus.Infof("Launcher listening to %v", listen)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Receive %v to exit", sig)
		rpcService.Stop()
	}()

	return <-shutdownCh
}

func main() {
	a := cli.NewApp()
	a.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "url",
			Value: "localhost:8500",
		},
		cli.BoolFlag{
			Name: "debug",
		},
	}
	a.Commands = []cli.Command{
		StartLauncherCmd(),
		ProcessCmd(),
		EngineCmd(),
	}
	if err := a.Run(os.Args); err != nil {
		logrus.Fatal("Error when executing command: ", err)
	}
}
