package app

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-engine/sync"
	"github.com/urfave/cli"
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
