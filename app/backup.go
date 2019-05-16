package app

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/backupstore"
	"github.com/rancher/backupstore/cmd"

	"github.com/rancher/longhorn-engine/sync"
	"github.com/rancher/longhorn-engine/types"
	"github.com/rancher/longhorn-engine/util"
)

func BackupCmd() cli.Command {
	return cli.Command{
		Name:      "backups",
		ShortName: "backup",
		Subcommands: []cli.Command{
			BackupCreateCmd(),
			BackupRestoreCmd(),
			RestoreToFileCmd(),
			cmd.BackupRemoveCmd(),
			cmd.BackupListCmd(),
			cmd.BackupInspectCmd(),
		},
	}
}

func BackupCreateCmd() cli.Command {
	return cli.Command{
		Name:  "create",
		Usage: "create a backup in objectstore: create <snapshot> --dest <dest>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "dest",
				Usage: "destination of backup if driver supports, would be url like s3://bucket@region/path/ or vfs:///path/",
			},
			cli.StringSliceFlag{
				Name:  "label",
				Usage: "specify labels for backup, in the format of `--label key1=value1 --label key2=value2`",
			},
		},
		Action: func(c *cli.Context) {
			if err := createBackup(c); err != nil {
				logrus.Fatalf("Error running create backup command: %v", err)
			}
		},
	}
}

func BackupRestoreCmd() cli.Command {
	return cli.Command{
		Name:  "restore",
		Usage: "restore a backup to current volume: restore <backup>  or  restore <backup> --incrementally --last-restored <last-restored>",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "incrementally, I",
				Usage: "Whether do incremental restore",
			},
			cli.StringFlag{
				Name:  "last-restored",
				Usage: "Last restored backup name",
			},
		},
		Action: func(c *cli.Context) {
			if err := restoreBackup(c); err != nil {
				logrus.Fatalf("Error running restore backup command: %v", err)
			}
		},
	}
}

func createBackup(c *cli.Context) error {
	url := c.GlobalString("url")
	task := sync.NewTask(url)

	dest := c.String("dest")
	if dest == "" {
		return fmt.Errorf("Missing required parameter --dest")
	}

	snapshot := c.Args().First()
	if snapshot == "" {
		return fmt.Errorf("Missing required parameter snapshot")
	}

	labels := c.StringSlice("label")
	if labels != nil {
		// Only validate it here, the real parse is done at backend
		if _, err := util.ParseLabels(labels); err != nil {
			return errors.Wrap(err, "cannot parse backup labels")
		}
	}

	credential := map[string]string{}
	backupType, err := util.CheckBackupType(dest)
	if err != nil {
		return err
	}
	if backupType == "s3" {
		accessKey := os.Getenv(types.AWSAccessKey)
		if accessKey == "" {
			return fmt.Errorf("Missing environment variable AWS_ACCESS_KEY_ID for s3 backup")
		}
		secretKey := os.Getenv(types.AWSSecretKey)
		if secretKey == "" {
			return fmt.Errorf("Missing environment variable AWS_SECRET_ACCESS_KEY for s3 backup")
		}
		credential[types.AWSAccessKey] = accessKey
		credential[types.AWSSecretKey] = secretKey
		credential[types.AWSEndPoint] = os.Getenv(types.AWSEndPoint)
	}

	backup, err := task.CreateBackup(snapshot, dest, labels, credential)
	if err != nil {
		return err
	}
	fmt.Println(backup)

	return nil
}

func restoreBackup(c *cli.Context) error {
	if c.Bool("incrementally") {
		return doRestoreBackupIncrementally(c)
	}
	return doRestoreBackup(c)
}

func doRestoreBackup(c *cli.Context) error {
	url := c.GlobalString("url")
	task := sync.NewTask(url)

	backup := c.Args().First()
	if backup == "" {
		return fmt.Errorf("Missing required parameter backup")
	}

	if err := task.RestoreBackup(backup); err != nil {
		return err
	}

	return nil
}

func doRestoreBackupIncrementally(c *cli.Context) error {
	url := c.GlobalString("url")
	task := sync.NewTask(url)

	backup := c.Args().First()
	if backup == "" {
		return fmt.Errorf("Missing required parameter backup")
	}
	backupName, err := backupstore.GetBackupFromBackupURL(backup)
	if err != nil {
		return err
	}

	lastRestored := c.String("last-restored")

	cli := getCli(c)
	err = cli.PrepareRestore(lastRestored)
	if err != nil {
		return err
	}

	if err := task.RestoreBackupIncrementally(backup, backupName, lastRestored); err != nil {
		// failed to restore, no need to update field lastRestored
		if extraErr := cli.FinishRestore(""); extraErr != nil {
			return errors.Wrapf(extraErr, "failed to execute and finsish incrementally restoring: %v", err)
		}
		return err
	}

	// TODO: will error out here cause dead lock?
	if err = cli.FinishRestore(backupName); err != nil {
		return errors.Wrapf(err, "failed to finsish incrementally restoring")
	}

	return nil
}
