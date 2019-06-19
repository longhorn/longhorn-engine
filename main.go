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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine-launcher/engine"
	"github.com/longhorn/longhorn-engine-launcher/health"
	"github.com/longhorn/longhorn-engine-launcher/process"
	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/types"
	"github.com/longhorn/longhorn-engine-launcher/util"
)

func StartCmd() cli.Command {
	return cli.Command{
		Name: "daemon",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:8500",
			},
			cli.StringFlag{
				Name:  "logs-dir",
				Value: "./logs",
			},
			cli.StringFlag{
				Name:  "port-range",
				Value: "10000-30000",
			},
		},
		Action: func(c *cli.Context) {
			if err := start(c); err != nil {
				logrus.Fatalf("Error running start command: %v.", err)
			}
		},
	}
}

func start(c *cli.Context) error {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")
	portRange := c.String("port-range")

	if err := util.SetUpLogger(logsDir); err != nil {
		return err
	}

	shutdownCh := make(chan error)
	pl, err := process.NewLauncher(portRange, shutdownCh)
	if err != nil {
		return err
	}
	em, err := engine.NewEngineManager(pl, listen)
	if err != nil {
		return err
	}
	hc := health.NewHealthCheckServer(em, pl)

	addShutdown(func() {
		logrus.Infof("Try to gracefully shut down process manager at %v", listen)
		resp, err := pl.ProcessList(nil, &rpc.ProcessListRequest{})
		if err != nil {
			logrus.Errorf("Failed to list instance processes before shutdown")
			return
		}
		for _, p := range resp.Processes {
			pl.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
				Name: p.Spec.Name,
			})
		}

		for i := 0; i < types.WaitCount; i++ {
			resp, err := pl.ProcessList(nil, &rpc.ProcessListRequest{})
			if err != nil {
				logrus.Errorf("Failed to list instance processes when shutting down")
				return
			}
			if len(resp.Processes) == 0 {
				logrus.Infof("Process Manager has cleaned up all processes. Graceful shutdown succeeded")
				return
			}
			time.Sleep(types.WaitInterval)
		}
		logrus.Errorf("Failed to cleanup all processes for Process Manager graceful shutdown")
		return
	})

	listenAt, err := net.Listen("tcp", listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	rpcService := grpc.NewServer()
	rpc.RegisterLonghornProcessLauncherServiceServer(rpcService, pl)
	rpc.RegisterLonghornEngineManagerServiceServer(rpcService, em)
	healthpb.RegisterHealthServer(rpcService, hc)
	reflection.Register(rpcService)

	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.Errorf("Stopping due to %v:", err)
		}
		close(shutdownCh)
	}()
	logrus.Infof("Process Manager listening to %v", listen)

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
	a.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
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
		StartCmd(),
		EngineCmd(),
		ProcessCmd(),
	}
	if err := a.Run(os.Args); err != nil {
		logrus.Fatal("Error when executing command: ", err)
	}
}
