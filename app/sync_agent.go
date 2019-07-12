package app

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine/sync"
	"github.com/longhorn/longhorn-engine/sync/rpc"
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
		},
		Action: func(c *cli.Context) {
			if err := startSyncAgent(c); err != nil {
				logrus.Fatalf("Error running sync-agent command: %v", err)
			}
		},
	}
}

func SyncAgentServerResetCmd() cli.Command {
	return cli.Command{
		Name: "sync-agent-server-reset",
		Action: func(c *cli.Context) {
			if err := doReset(c); err != nil {
				logrus.Fatalf("Error running sync-agent-server-reset command: %v", err)
			}
		},
	}
}

func startSyncAgent(c *cli.Context) error {
	listenPort := c.String("listen")
	portRange := c.String("listen-port-range")

	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return fmt.Errorf("Invalid format for range: %s", portRange)
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
		return errors.Wrap(err, "Failed to listen")
	}

	server := grpc.NewServer()
	rpc.RegisterSyncAgentServiceServer(server, rpc.NewSyncAgentServer(start, end))
	reflection.Register(server)

	logrus.Infof("Listening on sync %s", listenPort)

	return server.Serve(listen)
}

func doReset(c *cli.Context) error {
	task := sync.NewTask(c.GlobalString("url"))

	if err := task.Reset(); err != nil {
		logrus.Errorf("Failed to reset sync agent server")
		return err
	}
	logrus.Infof("Successfully reset sync agent server")
	return nil
}
