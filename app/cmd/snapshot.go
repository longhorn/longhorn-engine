package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine/pkg/controller/client"
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
			SnapshotPurgeStatusCmd(),
			SnapshotInfoCmd(),
			SnapshotCloneCmd(),
			SnapshotCloneStatusCmd(),
			SnapshotHashCmd(),
			SnapshotHashCancelCmd(),
			SnapshotHashStatusCmd(),
		},
		Action: func(c *cli.Context) {
			if err := lsSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot command")
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
				logrus.WithError(err).Fatalf("Error running create snapshot command")
			}
		},
	}
}

func SnapshotRevertCmd() cli.Command {
	return cli.Command{
		Name: "revert",
		Action: func(c *cli.Context) {
			if err := revertSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running revert snapshot command")
			}
		},
	}
}

func SnapshotRmCmd() cli.Command {
	return cli.Command{
		Name: "rm",
		Action: func(c *cli.Context) {
			if err := rmSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running rm snapshot command")
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
				logrus.WithError(err).Fatalf("Error running purge snapshot command")
			}
		},
	}
}

func SnapshotPurgeStatusCmd() cli.Command {
	return cli.Command{
		Name: "purge-status",
		Action: func(c *cli.Context) {
			if err := purgeSnapshotStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot purge status command")
			}
		},
	}
}

func SnapshotLsCmd() cli.Command {
	return cli.Command{
		Name: "ls",
		Action: func(c *cli.Context) {
			if err := lsSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running ls snapshot command")
			}
		},
	}
}

func SnapshotInfoCmd() cli.Command {
	return cli.Command{
		Name: "info",
		Action: func(c *cli.Context) {
			if err := infoSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot info command")
			}
		},
	}
}

func SnapshotCloneCmd() cli.Command {
	return cli.Command{
		Name: "clone",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "snapshot-name",
				Usage: "Specify the name of snapshot needed to clone",
			},
			cli.StringFlag{
				Name:  "from-controller-address",
				Usage: "Specify the address of the engine controller of the source volume",
			},
			cli.StringFlag{
				Name:     "from-controller-instance-name",
				Required: false,
				Usage:    "Specify the name of the engine controller instance of the source volume (for validation purposes)",
			},
			cli.BoolFlag{
				Name:  "export-backing-image-if-exist",
				Usage: "Specify if the backing image should be exported if it exists",
			},
			cli.IntFlag{
				Name:     "file-sync-http-client-timeout",
				Required: false,
				Value:    5,
				Usage:    "HTTP client timeout for replica file sync server",
			},
		},
		Action: func(c *cli.Context) {
			if err := cloneSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot clone command")
			}
		},
	}
}

func SnapshotCloneStatusCmd() cli.Command {
	return cli.Command{
		Name: "clone-status",
		Action: func(c *cli.Context) {
			if err := cloneSnapshotStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot clone status command")
			}
		},
	}
}

func SnapshotHashCmd() cli.Command {
	return cli.Command{
		Name: "hash",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "rehash",
				Usage: "Rehash snapshot disk file",
			},
		},
		Action: func(c *cli.Context) {
			if err := hashSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running hash snapshot command")
			}
		},
	}
}

func SnapshotHashCancelCmd() cli.Command {
	return cli.Command{
		Name: "hash-cancel",
		Action: func(c *cli.Context) {
			if err := cancelHashSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running cancel hashing snapshot command")
			}
		},
	}
}

func SnapshotHashStatusCmd() cli.Command {
	return cli.Command{
		Name: "hash-status",
		Action: func(c *cli.Context) {
			if err := hashSnapshotStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot hash status command")
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
		return fmt.Errorf("missing parameter for snapshot")
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
	url := c.GlobalString("url")
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	var lastErr error
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
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	skip := c.Bool("skip-if-in-progress")
	if err := task.PurgeSnapshots(skip); err != nil {
		return errors.Wrap(err, "failed to purge snapshots")
	}

	return nil
}

func purgeSnapshotStatus(c *cli.Context) error {
	url := c.GlobalString("url")
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

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
	volumeName := c.GlobalString("volume-name")

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
			chain, err := getChain(r.Address, volumeName)
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

		chain, err := getChain(r.Address, volumeName)
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

	volumeName := c.GlobalString("volume-name")
	outputDisks, err := sync.GetSnapshotsInfo(replicas, volumeName)
	if err != nil {
		return err
	}

	output, err = json.MarshalIndent(outputDisks, "", "\t")
	if err != nil {
		return err
	}

	if output == nil {
		return fmt.Errorf("cannot find suitable replica for snapshot info")
	}
	fmt.Println(string(output))
	return nil
}

func cloneSnapshot(c *cli.Context) error {
	snapshotName := c.String("snapshot-name")
	if snapshotName == "" {
		return fmt.Errorf("missing required parameter --snapshot-name")
	}
	fromControllerAddress := c.String("from-controller-address")
	if fromControllerAddress == "" {
		return fmt.Errorf("missing required parameter --from-controller-address")
	}
	exportBackingImageIfExist := c.Bool("export-backing-image-if-exist")
	fileSyncHTTPClientTimeout := c.Int("file-sync-http-client-timeout")

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	volumeName := c.GlobalString("volume-name")
	fromControllerEngineInstanceName := c.String("from-controller-instance-name")
	fromControllerClient, err := client.NewControllerClient(fromControllerAddress, volumeName, fromControllerEngineInstanceName)
	if err != nil {
		return err
	}
	defer fromControllerClient.Close()

	if err := sync.CloneSnapshot(controllerClient, fromControllerClient, volumeName, snapshotName,
		exportBackingImageIfExist, fileSyncHTTPClientTimeout); err != nil {
		return err
	}
	return nil
}

func cloneSnapshotStatus(c *cli.Context) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	volumeName := c.GlobalString("volume-name")
	statusMap, err := sync.CloneStatus(controllerClient, volumeName)
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

func hashSnapshot(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	snapshotName := c.Args()[0]

	url := c.GlobalString("url")
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	rehash := c.Bool("rehash")

	if err := task.HashSnapshot(snapshotName, rehash); err != nil {
		return errors.Wrapf(err, "failed to hash snapshot %v", snapshotName)
	}

	return nil
}

func cancelHashSnapshot(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	snapshotName := c.Args()[0]

	url := c.GlobalString("url")
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	if err := task.HashSnapshotCancel(snapshotName); err != nil {
		return errors.Wrapf(err, "failed to cancel hashing snapshot %v", snapshotName)
	}

	return nil
}

func hashSnapshotStatus(c *cli.Context) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	snapshotName := c.Args()[0]

	url := c.GlobalString("url")
	volumeName := c.GlobalString("volume-name")
	engineInstanceName := c.GlobalString("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	statusMap, err := task.HashSnapshotStatus(snapshotName)
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
