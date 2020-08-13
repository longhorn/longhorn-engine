package cmd

import (
	"encoding/json"
	"errors"
	"fmt"

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
	task := sync.NewTask(url)

	if c.Bool("restore") {
		return task.AddRestoreReplica(replica)
	}
	return task.AddReplica(replica)
}

func StartWithReplicasCmd() cli.Command {
	return cli.Command{
		Name:      "start-with-replicas",
		ShortName: "start",
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
	task := sync.NewTask(url)
	return task.StartWithReplicas(replicas)
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
	task := sync.NewTask(c.GlobalString("url"))
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

	task := sync.NewTask(c.GlobalString("url"))
	if err := task.VerifyRebuildReplica(address); err != nil {
		return err
	}
	return nil
}
