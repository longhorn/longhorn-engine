package cmd

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func RmReplicaCmd() *cli.Command {
	return &cli.Command{
		Name:    "rm-replica",
		Aliases: []string{"rm"},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := rmReplica(c); err != nil {
				logrus.WithError(err).Fatalf("Error running rm replica command")
				return err
			}
			return nil
		},
	}
}

func rmReplica(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("replica address is required")
	}
	replica := c.Args().Slice()[0]

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	return controllerClient.ReplicaDelete(replica)
}
