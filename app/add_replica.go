package app

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine/sync"
)

func AddReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "add-replica",
		ShortName: "add",
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
		Name: "replica-rebuild-status",
		Action: func(c *cli.Context) {
			if err := rebuildStatus(c); err != nil {
				logrus.Fatalf("Error running replica rebuild status: %v", err)
			}
		},
	}
}

func rebuildStatus(c *cli.Context) error {
	task := sync.NewTask(c.GlobalString("url"))
	rsMap, err := task.RebuildStatus()
	if err != nil {
		return err
	}

	rebuildStatus, err := json.Marshal(rsMap)
	if err != nil {
		return err
	}
	fmt.Println(string(rebuildStatus))

	return nil
}
