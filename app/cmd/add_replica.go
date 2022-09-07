package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/docker/go-units"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine/pkg/sync"
)

func AddReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "add-replica",
		ShortName: "add",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "restore",
				Usage: "Set this flag if the replica is being added to a restore/DR volume",
			},
			cli.StringFlag{
				Name:  "size",
				Usage: "Volume nominal size in bytes or human readable 42kb, 42mb, 42gb",
			},
			cli.StringFlag{
				Name:  "current-size",
				Usage: "Volume current size in bytes or human readable 42kb, 42mb, 42gb",
			},
		},
		Action: func(c *cli.Context) {
			if err := addReplica(c); err != nil {
				logrus.Fatalf("Error running add replica command: %v", err)
			}
		},
	}
}

func addReplica(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replica := c.Args()[0]

	url := c.GlobalString("url")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
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

	if c.Bool("restore") {
		return task.AddRestoreReplica(volumeSize, volumeCurrentSize, replica)
	}
	return task.AddReplica(volumeSize, volumeCurrentSize, replica)
}

func StartWithReplicasCmd() cli.Command {
	return cli.Command{
		Name:      "start-with-replicas",
		ShortName: "start",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "size",
				Usage: "Volume nominal size in bytes or human readable 42kb, 42mb, 42gb",
			},
			cli.StringFlag{
				Name:  "current-size",
				Usage: "Volume current size in bytes or human readable 42kb, 42mb, 42gb",
			},
		},
		Action: func(c *cli.Context) {
			if err := startWithReplicas(c); err != nil {
				logrus.Fatalf("Error running start-with-replica command: %v", err)
			}
		},
	}
}

func startWithReplicas(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replicas := c.Args()

	url := c.GlobalString("url")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
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

func RebuildStatusCmd() cli.Command {
	return cli.Command{
		Name:      "replica-rebuild-status",
		ShortName: "rebuild-status",
		Action: func(c *cli.Context) {
			if err := rebuildStatus(c); err != nil {
				logrus.Fatalf("Error running replica rebuild status: %v", err)
			}
		},
	}
}

func rebuildStatus(c *cli.Context) error {
	url := c.GlobalString("url")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
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

func VerifyRebuildReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "verify-rebuild-replica",
		ShortName: "verify-rebuild",
		Action: func(c *cli.Context) {
			if err := verifyRebuildReplica(c); err != nil {
				logrus.Fatalf("Error running verify rebuild replica command: %v", err)
			}
		},
	}
}

func verifyRebuildReplica(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	address := c.Args()[0]
	url := c.GlobalString("url")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
	if err != nil {
		return err
	}

	if err := task.VerifyRebuildReplica(address); err != nil {
		return err
	}
	return nil
}
