package app

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/client"
)

func LsReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "ls-replica",
		ShortName: "ls",
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
			if err := lsReplica(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func getCli(c *cli.Context) *client.ControllerClient {
	url := c.String("url")
	return client.NewControllerClient(url)

}

func lsReplica(c *cli.Context) error {
	controllerClient := getCli(c)

	reps, err := controllerClient.ListReplicas()
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 20, 1, ' ', 0)
	for _, r := range reps {
		chain := interface{}("")
		repClient, err := client.NewReplicaClient(r.Address)
		if err != nil {
			chain = err.Error()
		}

		if chain == "" {
			r, err := repClient.GetReplica()
			if err == nil {
				chain = r.Chain
			} else {
				chain = err.Error()
			}
		}

		fmt.Fprintf(tw, "%s\t%s\t%v\n", r.Address, r.Mode, chain)
	}
	tw.Flush()

	return nil
}
