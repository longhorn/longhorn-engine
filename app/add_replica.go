package app

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/sync"
)

func AddReplicaCmd() cli.Command {
	return cli.Command{
		Name: "add-replica",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "url",
				Value: "http://localhost:9501",
			},
			cli.BoolFlag{
				Name: "debug",
			},
		},
		Action: func(c *cli.Context) {
			if err := addReplica(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func addReplica(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replica := c.Args()[0]

	url := c.String("url")
	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	task := sync.NewTask(url)
	return task.AddReplica(replica)
}
