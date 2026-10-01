package cmd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-engine/pkg/sync"
	syncagentrpc "github.com/longhorn/longhorn-engine/pkg/sync/rpc"
)

func SyncAgentCmd() *cli.Command {
	return &cli.Command{
		Name:      "sync-agent",
		UsageText: "longhorn controller DIRECTORY SIZE",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9504",
			},
			&cli.StringFlag{
				Name:  "listen-port-range",
				Value: "9700-9800",
			},
			&cli.StringFlag{
				Name:  "replica",
				Usage: "specify replica address",
			},
			&cli.StringFlag{
				Name:  "replica-instance-name",
				Value: "",
				Usage: "Name of the replica instance (for validation purposes)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := startSyncAgent(c); err != nil {
				logrus.WithError(err).Fatal("Error running sync-agent command")
				return err
			}
			return nil
		},
	}
}

func SyncAgentServerResetCmd() *cli.Command {
	return &cli.Command{
		Name: "sync-agent-server-reset",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := doReset(c); err != nil {
				logrus.WithError(err).Fatal("Error running sync-agent-server-reset command")
				return err
			}
			return nil
		},
	}
}

func startSyncAgent(c *cli.Command) error {
	listenPort := c.String("listen")
	portRange := c.String("listen-port-range")
	replicaAddress := c.String("replica")
	volumeName := c.String("volume-name")
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Preserve the OS keepalive timings. The listener enables keepalive on each
	// accepted connection without letting the Go standard library replace them.
	// https://github.com/grpc/grpc-go/issues/6250
	listenConfig := net.ListenConfig{KeepAlive: -1}
	listen, err := listenConfig.Listen(ctx, "tcp", listenPort)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}

	server := syncagentrpc.NewSyncAgentServer(ctx, start, end, replicaAddress, volumeName, replicaInstanceName)

	logrus.Infof("Listening on sync %s", listenPort)

	return server.Serve(tcpKeepAliveListener{Listener: listen})
}

type tcpKeepAliveListener struct {
	net.Listener
}

func (l tcpKeepAliveListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			logrus.WithError(err).Warnf("Failed to enable TCP keepalive on connection from %v; using connection without TCP keepalive", conn.RemoteAddr())
		}
	}

	return conn, nil
}

func doReset(c *cli.Command) error {
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
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
