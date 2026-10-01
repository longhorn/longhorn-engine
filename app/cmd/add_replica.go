package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/docker/go-units"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-engine/pkg/sync"
)

func AddReplicaCmd() *cli.Command {
	return &cli.Command{
		Name:    "add-replica",
		Aliases: []string{"add"},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "restore",
				Usage: "Set this flag if the replica is being added to a restore/DR volume",
			},
			&cli.StringFlag{
				Name:  "size",
				Usage: "Volume nominal size in bytes or human readable 42kb, 42mb, 42gb",
			},
			&cli.StringFlag{
				Name:  "current-size",
				Usage: "Volume current size in bytes or human readable 42kb, 42mb, 42gb",
			},
			&cli.BoolFlag{
				Name:     "fast-sync",
				Required: false,
				Usage:    "Enable fast file synchronization using change time and checksum",
			},
			&cli.BoolFlag{
				Name:     "sync-local",
				Required: false,
				Usage:    "sync local replica",
			},
			&cli.IntFlag{
				Name:     "file-sync-http-client-timeout",
				Required: false,
				Value:    5,
				Usage:    "HTTP client timeout for replica file sync server",
			},
			&cli.Int64Flag{
				Name:     "grpc-timeout-seconds",
				Required: false,
				Value:    0,
				Usage:    "Specify the gRPC timeout for replica rebuilding. If specify a value <= 0, we will use 24h timeout",
			},
			&cli.StringFlag{
				Name:     "replica-instance-name",
				Required: false,
				Usage:    "Name of the replica instance (for validation purposes)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := addReplica(c); err != nil {
				logrus.WithError(err).Fatalf("Error running add replica command")
				return err
			}
			return nil
		},
	}
}

func addReplica(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replica := c.Args().Slice()[0]

	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
	replicaInstanceName := c.String("replica-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	size := c.String("size")
	if size == "" {
		return errors.New("size is required")
	}
	volumeSize, err := units.RAMInBytes(size)
	if err != nil {
		return err
	}

	size = c.String("current-size")
	if size == "" {
		return errors.New("current-size is required")
	}
	volumeCurrentSize, err := units.RAMInBytes(size)
	if err != nil {
		return err
	}

	fastSync := c.Bool("fast-sync")
	fileSyncHTTPClientTimeout := c.Int("file-sync-http-client-timeout")
	grpcTimeoutSeconds := c.Int64("grpc-timeout-seconds")

	if c.Bool("restore") {
		return task.AddRestoreReplica(volumeSize, volumeCurrentSize, replica, replicaInstanceName)
	}
	return task.AddReplica(volumeSize, volumeCurrentSize, replica, replicaInstanceName, fileSyncHTTPClientTimeout, fastSync, nil, grpcTimeoutSeconds)
}

func StartWithReplicasCmd() *cli.Command {
	return &cli.Command{
		Name:    "start-with-replicas",
		Aliases: []string{"start"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "size",
				Usage: "Volume nominal size in bytes or human readable 42kb, 42mb, 42gb",
			},
			&cli.StringFlag{
				Name:  "current-size",
				Usage: "Volume current size in bytes or human readable 42kb, 42mb, 42gb",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := startWithReplicas(c); err != nil {
				logrus.WithError(err).Fatalf("Error running start-with-replica command")
				return err
			}
			return nil
		},
	}
}

func startWithReplicas(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replicas := c.Args().Slice()

	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	size := c.String("size")
	if size == "" {
		return errors.New("size is required")
	}
	volumeSize, err := units.RAMInBytes(size)
	if err != nil {
		return err
	}

	size = c.String("current-size")
	if size == "" {
		return errors.New("current-size is required")
	}
	volumeCurrentSize, err := units.RAMInBytes(size)
	if err != nil {
		return err
	}

	return task.StartWithReplicas(volumeSize, volumeCurrentSize, replicas)
}

func RebuildStatusCmd() *cli.Command {
	return &cli.Command{
		Name:    "replica-rebuild-status",
		Aliases: []string{"rebuild-status"},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := rebuildStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running replica rebuild status")
				return err
			}
			return nil
		},
	}
}

func rebuildStatus(c *cli.Command) error {
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	statusMap, err := task.RebuildStatus()
	if err != nil {
		return err
	}

	output, err := json.MarshalIndent(statusMap, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func VerifyRebuildReplicaCmd() *cli.Command {
	return &cli.Command{
		Name:    "verify-rebuild-replica",
		Aliases: []string{"verify-rebuild"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "replica-instance-name",
				Required: false,
				Usage:    "Name of the replica instance (for validation purposes)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := verifyRebuildReplica(c); err != nil {
				logrus.WithError(err).Fatalf("Error running verify rebuild replica command")
				return err
			}
			return nil
		},
	}
}

func verifyRebuildReplica(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replicaAddress := c.Args().Slice()[0]
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
	replicaInstanceName := c.String("replica-instance-name")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	if err := task.VerifyRebuildReplica(replicaAddress, replicaInstanceName); err != nil {
		return err
	}
	return nil
}
