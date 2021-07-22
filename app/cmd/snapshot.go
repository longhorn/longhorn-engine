package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine/pkg/sync"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

func SnapshotCmd() cli.Command {
	return cli.Command{
		Name:      "snapshots",
		ShortName: "snapshot",
		Subcommands: []cli.Command{
			SnapshotCreateCmd(),
			SnapshotRevertCmd(),
			SnapshotLsCmd(),
			SnapshotRmCmd(),
			SnapshotPurgeCmd(),
			SnapshotPurgeStatusCommand(),
			SnapshotInfoCmd(),
		},
		Action: func(c *cli.Context) {
			if err := lsSnapshot(c); err != nil {
				logrus.Fatalf("Error running snapshot command: %v", err)
			}
		},
	}
}

func SnapshotCreateCmd() cli.Command {
	return cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			cli.StringSliceFlag{
				Name:  "label",
				Usage: "specify labels, in the format of `--label key1=value1 --label key2=value2`",
			},
		},
		Action: func(c *cli.Context) {
			if err := createSnapshot(c); err != nil {
				logrus.Fatalf("Error running create snapshot command: %v", err)
			}
		},
	}
}

func SnapshotRevertCmd() cli.Command {
	return cli.Command{
		Name: "revert",
		Action: func(c *cli.Context) {
			if err := revertSnapshot(c); err != nil {
				logrus.Fatalf("Error running revert snapshot command: %v", err)
			}
		},
	}
}

func SnapshotRmCmd() cli.Command {
	return cli.Command{
		Name: "rm",
		Action: func(c *cli.Context) {
			if err := rmSnapshot(c); err != nil {
				logrus.Fatalf("Error running rm snapshot command: %v", err)
			}
		},
	}
}

func SnapshotPurgeCmd() cli.Command {
	return cli.Command{
		Name: "purge",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "skip-if-in-progress",
				Usage: "set to mute errors if replica is already purging",
			},
		},
		Action: func(c *cli.Context) {
			if err := purgeSnapshot(c); err != nil {
				logrus.Fatalf("Error running purge snapshot command: %v", err)
			}
		},
	}
}

func SnapshotPurgeStatusCommand() cli.Command {
	return cli.Command{
		Name: "purge-status",
		Action: func(c *cli.Context) {
			if err := purgeSnapshotStatus(c); err != nil {
				logrus.Fatalf("Error running snapshot purge status command: %v", err)
			}
		},
	}
}

func SnapshotLsCmd() cli.Command {
	return cli.Command{
		Name: "ls",
		Action: func(c *cli.Context) {
			if err := lsSnapshot(c); err != nil {
				logrus.Fatalf("Error running ls snapshot command: %v", err)
			}
		},
	}
}

func SnapshotInfoCmd() cli.Command {
	return cli.Command{
		Name: "info",
		Action: func(c *cli.Context) {
			if err := infoSnapshot(c); err != nil {
				logrus.Fatalf("Error running snapshot info command: %v", err)
			}
		},
	}
}

func createSnapshot(c *cli.Context) error {
	var (
		labelMap map[string]string
		err      error
	)

	var name string
	if len(c.Args()) > 0 {
		name = c.Args()[0]
	}

	labels := c.StringSlice("label")
	if labels != nil {
		labelMap, err = util.ParseLabels(labels)
		if err != nil {
			return errors.Wrap(err, "cannot parse backup labels")
		}
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	id, err := controllerClient.VolumeSnapshot(name, labelMap)
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

func revertSnapshot(c *cli.Context) error {
	name := c.Args()[0]
	if name == "" {
		return fmt.Errorf("Missing parameter for snapshot")
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	if err = controllerClient.VolumeRevert(name); err != nil {
		return err
	}

	return nil
}

func rmSnapshot(c *cli.Context) error {
	var lastErr error
	url := c.GlobalString("url")
	task := sync.NewTask(url)

	for _, name := range c.Args() {
		if err := task.DeleteSnapshot(name); err != nil {
			lastErr = err
			fmt.Fprintf(os.Stderr, "Failed to delete %s: %v\n", name, err)
		}
	}

	return lastErr
}

func purgeSnapshot(c *cli.Context) error {
	url := c.GlobalString("url")
	skip := c.Bool("skip-if-in-progress")
	task := sync.NewTask(url)

	if err := task.PurgeSnapshots(skip); err != nil {
		return fmt.Errorf("Failed to purge snapshots: %v", err)
	}

	return nil
}

func purgeSnapshotStatus(c *cli.Context) error {
	task := sync.NewTask(c.GlobalString("url"))
	statusMap, err := task.PurgeSnapshotStatus()
	if err != nil {
		return err
	}

	output, err := json.MarshalIndent(statusMap, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func lsSnapshot(c *cli.Context) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	replicas, err := controllerClient.ReplicaList()
	if err != nil {
		return err
	}

	first := true
	snapshots := []string{}
	for _, r := range replicas {
		if r.Mode != types.RW {
			continue
		}

		if first {
			first = false
			chain, err := getChain(r.Address)
			if err != nil {
				return err
			}
			// Replica can just started and haven't prepare the head
			// file yet
			if len(chain) == 0 {
				break
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

func infoSnapshot(c *cli.Context) error {
	var output []byte

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	replicas, err := controllerClient.ReplicaList()
	if err != nil {
		return err
	}

	outputDisks, err := sync.GetSnapshotsInfo(replicas)
	if err != nil {
		return err
	}

	output, err = json.MarshalIndent(outputDisks, "", "\t")
	if err != nil {
		return err
	}

	if output == nil {
		return fmt.Errorf("Cannot find suitable replica for snapshot info")
	}
	fmt.Println(string(output))
	return nil
}
