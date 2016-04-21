package app

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

func RmReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "rm-replica",
		ShortName: "rm",
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
			if err := rmReplica(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func rmReplica(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replica := c.Args()[0]

	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	controllerClient := getCli(c)
	_, err := controllerClient.DeleteReplica(replica)
	return err
}
