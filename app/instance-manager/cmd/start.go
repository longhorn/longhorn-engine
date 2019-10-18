package cmd

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/engine"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/health"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/process"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/util"
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
				Value: "/var/log/instances",
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

func cleanup(pm *process.Manager, em *engine.Manager) {
	logrus.Infof("Try to gracefully shut down Instance Manager")
	emResp, err := em.EngineList(nil, &empty.Empty{})
	if err != nil {
		logrus.Errorf("Failed to list engine processes before shutdown")
		return
	}
	for _, e := range emResp.Engines {
		em.EngineDelete(nil, &rpc.EngineRequest{
			Name: e.Spec.Name,
		})
	}
	for i := 0; i < types.WaitCount; i++ {
		emResp, err := em.EngineList(nil, &empty.Empty{})
		if err != nil {
			logrus.Errorf("Failed to list engine processes when shutting down")
			return
		}
		if len(emResp.Engines) == 0 {
			logrus.Infof("Instance Manager has shutdown all processes and cleaned up all engine processes. Graceful shutdown succeeded")
			return
		}
		time.Sleep(types.WaitInterval)
	}

	pmResp, err := pm.ProcessList(nil, &rpc.ProcessListRequest{})
	if err != nil {
		logrus.Errorf("Failed to list processes before shutdown")
		return
	}
	for _, p := range pmResp.Processes {
		pm.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
			Name: p.Spec.Name,
		})
	}

	for i := 0; i < types.WaitCount; i++ {
		pmResp, err := pm.ProcessList(nil, &rpc.ProcessListRequest{})
		if err != nil {
			logrus.Errorf("Failed to list instance processes when shutting down")
			return
		}
		if len(pmResp.Processes) == 0 {
			logrus.Infof("Instance Manager has shutdown all processes.")
			break
		}
		time.Sleep(types.WaitInterval)
	}

	logrus.Errorf("Failed to cleanup all processes for Instance Manager graceful shutdown")
}

func start(c *cli.Context) error {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")
	portRange := c.String("port-range")

	if err := util.SetUpLogger(logsDir); err != nil {
		return err
	}

	shutdownCh := make(chan error)
	pm, err := process.NewManager(portRange, logsDir, shutdownCh)
	if err != nil {
		return err
	}
	processUpdateCh, err := pm.Subscribe()
	if err != nil {
		return err
	}
	em, err := engine.NewEngineManager(pm, processUpdateCh, listen, shutdownCh)
	if err != nil {
		return err
	}
	hc := health.NewHealthCheckServer(em, pm)

	listenAt, err := net.Listen("tcp", listen)
	if err != nil {
		return errors.Wrap(err, "Failed to listen")
	}

	rpcService := grpc.NewServer()
	rpc.RegisterProcessManagerServiceServer(rpcService, pm)
	rpc.RegisterEngineManagerServiceServer(rpcService, em)
	healthpb.RegisterHealthServer(rpcService, hc)
	reflection.Register(rpcService)

	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.Errorf("Stopping due to %v:", err)
		}
		// graceful shutdown before exit
		cleanup(pm, em)
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager listening to %v", listen)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Instance Manager received %v to exit", sig)
		rpcService.Stop()
	}()

	return <-shutdownCh
}
