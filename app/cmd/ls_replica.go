package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

func LsReplicaCmd() *cli.Command {
	return &cli.Command{
		Name:    "ls-replica",
		Aliases: []string{"ls"},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := lsReplica(c); err != nil {
				logrus.WithError(err).Fatalf("Error running ls command")
				return err
			}
			return nil
		},
	}
}

func lsReplica(c *cli.Command) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	volumeName := c.String("volume-name")

	reps, err := controllerClient.ReplicaList()
	if err != nil {
		return err
	}

	format := "%s\t%s\t%v\n"
	tw := tabwriter.NewWriter(os.Stdout, 0, 20, 1, ' ', 0)
	_, _ = fmt.Fprintf(tw, format, "ADDRESS", "MODE", "CHAIN")
	for _, r := range reps {
		if r.Mode == types.ERR {
			_, _ = fmt.Fprintf(tw, format, r.Address, r.Mode, "")
			continue
		}
		chain := interface{}("")
		chainList, err := getChain(r.Address, volumeName)
		if err == nil {
			chain = chainList
		}
		_, _ = fmt.Fprintf(tw, format, r.Address, r.Mode, chain)
	}
	if errFlush := tw.Flush(); errFlush != nil {
		logrus.WithError(errFlush).Error("Failed to flush")
	}

	return nil
}

func getChain(address, volumeName string) ([]string, error) {
	// We don't know the replica's instanceName, so create a client without it.
	repClient, err := replicaClient.NewReplicaClient(address, volumeName, "")
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := repClient.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close replica client for replica address %s", address)
		}
	}()

	r, err := repClient.GetReplica()
	if err != nil {
		return nil, err
	}

	return r.Chain, err
}
