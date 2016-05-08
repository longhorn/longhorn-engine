package app

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/sync"
	"github.com/rancher/longhorn/util"
)

func SnapshotCmd() cli.Command {
	return cli.Command{
		Name:      "snapshots",
		ShortName: "snapshot",
		Subcommands: []cli.Command{
			SnapshotCreateCmd(),
			SnapshotLsCmd(),
			SnapshotRmCmd(),
		},
		Action: func(c *cli.Context) {
			if err := lsSnapshot(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func SnapshotCreateCmd() cli.Command {
	return cli.Command{
		Name: "create",
		Action: func(c *cli.Context) {
			if err := createSnapshot(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func SnapshotRmCmd() cli.Command {
	return cli.Command{
		Name: "rm",
		Action: func(c *cli.Context) {
			if err := rmSnapshot(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func SnapshotLsCmd() cli.Command {
	return cli.Command{
		Name: "ls",
		Action: func(c *cli.Context) {
			if err := lsSnapshot(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func createSnapshot(c *cli.Context) error {
	cli := getCli(c)

	id, err := cli.Snapshot()
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

func rmSnapshot(c *cli.Context) error {
	var lastErr error
	url := c.GlobalString("url")
	task := sync.NewTask(url)

	for _, name := range c.Args() {
		if err := task.DeleteSnapshot(name); err == nil {
			fmt.Printf("deleted %s\n", name)
		} else {
			lastErr = err
			fmt.Fprintf(os.Stderr, "Failed to delete %s: %v", name, err)
		}
	}

	return lastErr
}

func lsSnapshot(c *cli.Context) error {
	cli := getCli(c)

	replicas, err := cli.ListReplicas()
	if err != nil {
		return err
	}

	first := true
	snapshots := []string{}
	for _, r := range replicas {
		if r.Mode == "ERR" {
			continue
		}

		if first {
			first = false
			chain, err := getChain(r.Address)
			if err != nil {
				return err
			}
			snapshots = chain[1:]
			continue
		}

		chain, err := getChain(r.Address)
		if err != nil {
			return err
		}

		snapshots = util.Filter(snapshots, func(i string) bool {
			return util.Contains(chain, i)
		})
	}

	format := "%s\n"
	tw := tabwriter.NewWriter(os.Stdout, 0, 20, 1, ' ', 0)
	fmt.Fprintf(tw, format, "ID")
	for _, s := range snapshots {
		s = strings.TrimSuffix(strings.TrimPrefix(s, "volume-snap-"), ".img")
		fmt.Fprintf(tw, format, s)
	}
	tw.Flush()

	return nil
}
