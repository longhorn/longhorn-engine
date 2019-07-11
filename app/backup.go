package app

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/cmd"

	"github.com/longhorn/longhorn-engine/sync"
	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

func BackupCmd() cli.Command {
	return cli.Command{
		Name:      "backups",
		ShortName: "backup",
		Subcommands: []cli.Command{
			BackupCreateCmd(),
			BackupStatusCmd(),
			BackupRestoreCmd(),
			BackupRestoreStatusCmd(),
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

func BackupStatusCmd() cli.Command {
	return cli.Command{
		Name:  "status",
		Usage: "query the progress of the backup: status [<backupID>]",
		Action: func(c *cli.Context) {
			if err := checkBackupStatus(c); err != nil {
				logrus.Fatalf("Error querying backup status: %v", err)
			}
		},
	}
}

func getBackupStatus(c *cli.Context, backupID string, replicaAddress string) (*sync.BackupStatusInfo, error) {
	if backupID == "" {
		return nil, fmt.Errorf("Missing required parameter backupID")
	}
	//Fetch backupObject using the replicaIP
	task := sync.NewTask(c.GlobalString("url"))

	backupStatus, err := task.FetchBackupStatus(backupID, replicaAddress)
	if err != nil {
		return nil, err
	}

	return backupStatus, nil
}

func fetchAllBackups(c *cli.Context) error {
	backupProgressList := make(map[string]*sync.BackupStatusInfo)

	cli := getCli(c)
	backupReplicaMap, err := cli.BackupReplicaMappingGet()
	if err != nil {
		return fmt.Errorf("failed to get list of backupIDs:%v", err)
	}

	for backupID, replicaAddress := range backupReplicaMap {
		status, err := getBackupStatus(c, backupID, replicaAddress)
		if err != nil {
			if strings.Contains(err.Error(), "backup not found") {
				err := cli.BackupReplicaMappingDelete(backupID)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		if status == nil {
			continue
		}

		backupProgressList[backupID] = status
	}

	if backupProgressList == nil {
		return nil
	}

	output, err := cmd.ResponseOutput(backupProgressList)
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func checkBackupStatus(c *cli.Context) error {
	backupID := c.Args().First()
	if backupID == "" {
		return fetchAllBackups(c)
	}

	client := getCli(c)
	br, err := client.BackupReplicaMappingGet()
	if err != nil {
		return err
	}
	replicaAddress, present := br[backupID]
	if present == false {
		return fmt.Errorf("couldn't find replica address for backup:%v", backupID)
	}

	if replicaAddress == "" {
		return fmt.Errorf("replica address is empty")
	}

	status, err := getBackupStatus(c, backupID, replicaAddress)
	if err != nil {
		return err
	}

	backupStatus, err := json.Marshal(status)
	if err != nil {
		return err
	}
	fmt.Println(string(backupStatus))
	return nil
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

func BackupRestoreStatusCmd() cli.Command {
	return cli.Command{
		Name:  "restore-status",
		Usage: "Check if restore operation is currently going on",

		Action: func(c *cli.Context) {
			if err := restoreStatus(c); err != nil {
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

	if err := task.RestoreBackupIncrementally(backup, backupName, lastRestored); err != nil {
		logrus.Errorf("failed to perform incremental restore: %v", err)
		return err
	}
	return nil
}

func restoreStatus(c *cli.Context) error {
	task := sync.NewTask(c.GlobalString("url"))

	rsList, err := task.RestoreStatus()
	if err != nil {
		return err
	}

	restoreStatus, err := json.Marshal(rsList)
	if err != nil {
		return err
	}
	fmt.Println(string(restoreStatus))

	return nil
}
