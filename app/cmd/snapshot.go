package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	lhutils "github.com/longhorn/go-common-libs/utils"

	"github.com/longhorn/longhorn-engine/pkg/controller/client"
	"github.com/longhorn/longhorn-engine/pkg/sync"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

func SnapshotCmd() *cli.Command {
	return &cli.Command{
		Name:    "snapshots",
		Aliases: []string{"snapshot"},
		Commands: []*cli.Command{
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
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := lsSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotCreateCmd() *cli.Command {
	return &cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "label",
				Usage: "Specify labels, in the format of `--label key1=value1 --label key2=value2`",
			},
			&cli.BoolFlag{
				Name:  "freeze-fs",
				Usage: "Freeze the filesystem on the root partition before taking the snapshot",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := createSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running create snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotRevertCmd() *cli.Command {
	return &cli.Command{
		Name: "revert",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := revertSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running revert snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotRmCmd() *cli.Command {
	return &cli.Command{
		Name: "rm",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := rmSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running rm snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotPurgeCmd() *cli.Command {
	return &cli.Command{
		Name: "purge",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "skip-if-in-progress",
				Usage: "set to mute errors if replica is already purging",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := purgeSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running purge snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotPurgeStatusCmd() *cli.Command {
	return &cli.Command{
		Name: "purge-status",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := purgeSnapshotStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot purge status command")
				return err
			}
			return nil
		},
	}
}

func SnapshotLsCmd() *cli.Command {
	return &cli.Command{
		Name: "ls",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := lsSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running ls snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotInfoCmd() *cli.Command {
	return &cli.Command{
		Name: "info",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := infoSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot info command")
				return err
			}
			return nil
		},
	}
}

func SnapshotCloneCmd() *cli.Command {
	return &cli.Command{
		Name: "clone",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "snapshot-name",
				Usage: "Specify the name of snapshot needed to clone",
			},
			&cli.StringFlag{
				Name:  "from-controller-address",
				Usage: "Specify the address of the engine controller of the source volume",
			},
			&cli.StringFlag{
				Name:     "from-volume-name",
				Required: false,
				Usage:    "Specify the name of the source volume (for validation purposes)",
			},
			&cli.StringFlag{
				Name:     "from-controller-instance-name",
				Required: false,
				Usage:    "Specify the name of the engine controller instance of the source volume (for validation purposes)",
			},
			&cli.BoolFlag{
				Name:  "export-backing-image-if-exist",
				Usage: "Specify if the backing image should be exported if it exists",
			},
			&cli.IntFlag{
				Name:     "file-sync-http-client-timeout",
				Required: false,
				Value:    5,
				Usage:    "HTTP client timeout for replica file sync server",
			},
			&cli.Int64Flag{
				Name:     "grpc-timeout-seconds",
				Required: false,
				Value:    0,
				Usage:    "Specify the gRPC timeout for snapshot clone. If specify a value <= 0, we will use 24h timeout",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := cloneSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot clone command")
				return err
			}
			return nil
		},
	}
}

func SnapshotCloneStatusCmd() *cli.Command {
	return &cli.Command{
		Name: "clone-status",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := cloneSnapshotStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot clone status command")
				return err
			}
			return nil
		},
	}
}

func SnapshotHashCmd() *cli.Command {
	return &cli.Command{
		Name: "hash",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "rehash",
				Usage: "Rehash snapshot disk file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := hashSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running hash snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotHashCancelCmd() *cli.Command {
	return &cli.Command{
		Name: "hash-cancel",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := cancelHashSnapshot(c); err != nil {
				logrus.WithError(err).Fatalf("Error running cancel hashing snapshot command")
				return err
			}
			return nil
		},
	}
}

func SnapshotHashStatusCmd() *cli.Command {
	return &cli.Command{
		Name: "hash-status",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := hashSnapshotStatus(c); err != nil {
				logrus.WithError(err).Fatalf("Error running snapshot hash status command")
				return err
			}
			return nil
		},
	}
}

func createSnapshot(c *cli.Command) error {
	var (
		labelMap map[string]string
		err      error
	)

	var name string
	if c.Args().Len() > 0 {
		name = c.Args().First()
	}

	labels := c.StringSlice("label")
	if labels != nil {
		labelMap, err = util.ParseLabels(labels)
		if err != nil {
			return errors.Wrap(err, "cannot parse backup labels")
		}
	}

	freezeFilesystem := c.Bool("freeze-fs")

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	id, err := controllerClient.VolumeSnapshot(name, labelMap, freezeFilesystem)
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

func revertSnapshot(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	name := c.Args().First()
	if name == "" {
		return fmt.Errorf("missing parameter for snapshot")
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	if err = controllerClient.VolumeRevert(name); err != nil {
		return err
	}

	return nil
}

func rmSnapshot(c *cli.Command) error {
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url, volumeName, engineInstanceName)
	if err != nil {
		return err
	}

	var lastErr error
	for _, name := range c.Args().Slice() {
		if err := task.DeleteSnapshot(name); err != nil {
			lastErr = err
			fmt.Fprintf(os.Stderr, "Failed to delete %s: %v\n", name, err)
		}
	}

	return lastErr
}

func purgeSnapshot(c *cli.Command) error {
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
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

func purgeSnapshotStatus(c *cli.Command) error {
	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
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

func lsSnapshot(c *cli.Command) error {
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
			return lhutils.Contains(chain, i)
		})
	}

	format := "%s\n"
	tw := tabwriter.NewWriter(os.Stdout, 0, 20, 1, ' ', 0)
	_, _ = fmt.Fprintf(tw, format, "ID")
	for _, s := range snapshots {
		s = strings.TrimSuffix(strings.TrimPrefix(s, "volume-snap-"), ".img")
		_, _ = fmt.Fprintf(tw, format, s)
	}
	if errFlush := tw.Flush(); errFlush != nil {
		logrus.WithError(errFlush).Error("Failed to flush")
	}

	return nil
}

func infoSnapshot(c *cli.Command) error {
	var output []byte

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	replicas, err := controllerClient.ReplicaList()
	if err != nil {
		return err
	}

	volumeName := c.String("volume-name")
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

func cloneSnapshot(c *cli.Command) error {
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
	grpcTimeoutSeconds := c.Int64("grpc-timeout-seconds")

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
	fromVolumeName := c.String("from-volume-name")
	fromControllerInstanceName := c.String("from-controller-instance-name")
	fromControllerClient, err := client.NewControllerClient(fromControllerAddress, fromVolumeName,
		fromControllerInstanceName)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := fromControllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close from controller client")
		}
	}()

	if err := sync.CloneSnapshot(controllerClient, fromControllerClient, volumeName, fromVolumeName,
		snapshotName, exportBackingImageIfExist, fileSyncHTTPClientTimeout, grpcTimeoutSeconds); err != nil {
		return err
	}
	return nil
}

func cloneSnapshotStatus(c *cli.Command) error {
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

func hashSnapshot(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	snapshotName := c.Args().First()

	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
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

func cancelHashSnapshot(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	snapshotName := c.Args().First()

	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
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

func hashSnapshotStatus(c *cli.Command) error {
	if c.NArg() == 0 {
		return errors.New("snapshot name is required")
	}

	snapshotName := c.Args().First()

	url := c.String("url")
	volumeName := c.String("volume-name")
	engineInstanceName := c.String("engine-instance-name")
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
