package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

func UpdateReplicaCmd() *cli.Command {
	return &cli.Command{
		Name:    "update-replica",
		Aliases: []string{"update"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "mode",
				Usage: "Replica mode. The value can be RO, RW or ERR.",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			_, err := updateReplica(c)
			if err != nil {
				logrus.WithError(err).Fatalf("Error running update replica command")
				return err
			}
			return nil
		},
	}
}

func updateReplica(c *cli.Command) (*types.ControllerReplicaInfo, error) {
	if c.NArg() == 0 {
		return nil, errors.New("replica address is required")
	}
	replica := c.Args().Slice()[0]

	mode := types.Mode(c.String("mode"))
	if mode != types.WO && mode != types.RW && mode != types.ERR {
		return nil, fmt.Errorf("unsupported replica mode: %v", mode)
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	return controllerClient.ReplicaUpdate(replica, mode)
}
