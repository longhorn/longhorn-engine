package cmd

import (
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func RmReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "rm-replica",
		ShortName: "rm",
		Action: func(c *cli.Context) {
			if err := rmReplica(c); err != nil {
				logrus.WithError(err).Fatalf("Error running rm replica command")
			}
		},
	}
}

func rmReplica(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replica := c.Args()[0]

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	return controllerClient.ReplicaDelete(replica)
}
