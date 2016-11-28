package app

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/controller/client"
	replicaClient "github.com/rancher/longhorn/replica/client"
)

func LsReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "ls-replica",
		ShortName: "ls",
		Action: func(c *cli.Context) {
			if err := lsReplica(c); err != nil {
				logrus.Fatalf("Error running ls command: %v", err)
			}
		},
	}
}

func getCli(c *cli.Context) *client.ControllerClient {
	url := c.GlobalString("url")
	return client.NewControllerClient(url)

}

func lsReplica(c *cli.Context) error {
	controllerClient := getCli(c)

	reps, err := controllerClient.ListReplicas()
	if err != nil {
		return err
	}

	format := "%s\t%s\t%v\n"
	tw := tabwriter.NewWriter(os.Stdout, 0, 20, 1, ' ', 0)
	fmt.Fprintf(tw, format, "ADDRESS", "MODE", "CHAIN")
	for _, r := range reps {
		if r.Mode == "ERR" {
			fmt.Fprintf(tw, format, r.Address, r.Mode, "")
			continue
		}
		chain := interface{}("")
		chainList, err := getChain(r.Address)
		if err == nil {
			chain = chainList
		}
		fmt.Fprintf(tw, format, r.Address, r.Mode, chain)
	}
	tw.Flush()

	return nil
}

func getChain(address string) ([]string, error) {
	repClient, err := replicaClient.NewReplicaClient(address)
	if err != nil {
		return nil, err
	}

	r, err := repClient.GetReplica()
	if err != nil {
		return nil, err
	}

	return r.Chain, err
}
