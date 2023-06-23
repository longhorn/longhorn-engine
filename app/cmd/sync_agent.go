package cmd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine/pkg/sync"
	syncagentrpc "github.com/longhorn/longhorn-engine/pkg/sync/rpc"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

func SyncAgentCmd() cli.Command {
	return cli.Command{
		Name:      "sync-agent",
		UsageText: "longhorn controller DIRECTORY SIZE",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9504",
			},
			cli.StringFlag{
				Name:  "listen-port-range",
				Value: "9700-9800",
			},
			cli.StringFlag{
				Name:  "replica",
				Usage: "specify replica address",
			},
			cli.StringFlag{
				Name:  "replica-instance-name",
				Value: "",
				Usage: "Name of the replica instance (for validation purposes)",
			},
		},
		Action: func(c *cli.Context) {
			if err := startSyncAgent(c); err != nil {
				logrus.WithError(err).Fatal("Error running sync-agent command")
			}
		},
	}
}

func SyncAgentServerResetCmd() cli.Command {
	return cli.Command{
		Name: "sync-agent-server-reset",
		Action: func(c *cli.Context) {
			if err := doReset(c); err != nil {
				logrus.WithError(err).Fatal("Error running sync-agent-server-reset command")
			}
		},
	}
}

func startSyncAgent(c *cli.Context) error {
	listenPort := c.String("listen")
	portRange := c.String("listen-port-range")
	replicaAddress := c.String("replica")
	volumeName := c.GlobalString("volume-name")
	replicaInstanceName := c.String("replica-instance-name")

	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid format for range: %s", portRange)
	}

	start, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return err
	}

	end, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return err
	}

	listen, err := net.Listen("tcp", listenPort)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}

	server := grpc.NewServer()
	ptypes.RegisterSyncAgentServiceServer(server, syncagentrpc.NewSyncAgentServer(start, end, replicaAddress,
		volumeName, replicaInstanceName))
	reflection.Register(server)

	logrus.Infof("Listening on sync %s", listenPort)

	return server.Serve(listen)
}

func doReset(c *cli.Context) error {
	url := c.GlobalString("url")
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	if err := task.Reset(); err != nil {
		logrus.WithError(err).Error("Failed to reset sync agent server")
		return err
	}
	logrus.Info("Successfully reset sync agent server")
	return nil
}
