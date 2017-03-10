package app

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/replica"
	replicaClient "github.com/rancher/longhorn/replica/client"
	"github.com/rancher/longhorn/sync"
	"github.com/rancher/longhorn/util"
)

const VolumeHeadName = "volume-head"

func SnapshotCmd() cli.Command {
	return cli.Command{
		Name:      "snapshots",
		ShortName: "snapshot",
		Subcommands: []cli.Command{
			SnapshotCreateCmd(),
			SnapshotRevertCmd(),
			SnapshotLsCmd(),
			SnapshotRmCmd(),
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
	cli := getCli(c)

	var name string
	if len(c.Args()) > 0 {
		name = c.Args()[0]
	}
	id, err := cli.Snapshot(name)
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

func revertSnapshot(c *cli.Context) error {
	cli := getCli(c)

	name := c.Args()[0]
	if name == "" {
		return fmt.Errorf("Missing parameter for snapshot")
	}

	err := cli.RevertSnapshot(name)
	if err != nil {
		return err
	}

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
			fmt.Fprintf(os.Stderr, "Failed to delete %s: %v\n", name, err)
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
		if r.Mode != "RW" {
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

	outputDisks := make(map[string]replica.DiskInfo)
	cli := getCli(c)

	replicas, err := cli.ListReplicas()
	if err != nil {
		return err
	}

	for _, r := range replicas {
		if r.Mode != "RW" {
			continue
		}

		disks, err := getDisks(r.Address)
		if err != nil {
			return err
		}

		for name, disk := range disks {
			snapshot := ""

			if !replica.IsHeadDisk(name) {
				snapshot, err = replica.GetSnapshotNameFromDiskName(name)
				if err != nil {
					return err
				}
			} else {
				snapshot = VolumeHeadName
			}
			children := []string{}
			for _, childDisk := range disk.Children {
				child := ""
				if !replica.IsHeadDisk(childDisk) {
					child, err = replica.GetSnapshotNameFromDiskName(childDisk)
					if err != nil {
						return err
					}
				} else {
					child = VolumeHeadName
				}
				children = append(children, child)
			}
			parent := ""
			if disk.Parent != "" {
				parent, err = replica.GetSnapshotNameFromDiskName(disk.Parent)
				if err != nil {
					return err
				}
			}
			info := replica.DiskInfo{
				Name:        snapshot,
				Parent:      parent,
				Removed:     disk.Removed,
				UserCreated: disk.UserCreated,
				Children:    children,
				Created:     disk.Created,
			}
			if _, exists := outputDisks[snapshot]; !exists {
				outputDisks[snapshot] = info
			} else {
				// Consolidate the result of snapshot in removing process
				if info.Removed && !outputDisks[snapshot].Removed {
					t := outputDisks[snapshot]
					t.Removed = true
					outputDisks[snapshot] = t
				}
			}
		}

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

func getDisks(address string) (map[string]replica.DiskInfo, error) {
	repClient, err := replicaClient.NewReplicaClient(address)
	if err != nil {
		return nil, err
	}

	r, err := repClient.GetReplica()
	if err != nil {
		return nil, err
	}

	return r.Disks, err
}
