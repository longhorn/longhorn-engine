package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/cmd"

	"github.com/longhorn/longhorn-engine/pkg/sync"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

func BackupCmd() cli.Command {
	return cli.Command{
		Name:      "backups",
		ShortName: "backup",
		Subcommands: []cli.Command{
			BackupCreateCmd(),
			BackupStatusCmd(),
			BackupRestoreCmd(),
			RestoreToFileCmd(),
			RestoreStatusCmd(),
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

func getReplicaModeMap(c *cli.Context) (map[string]types.Mode, error) {
	cli := getCli(c)
	replicas, err := cli.ReplicaList()
	if err != nil {
		return nil, fmt.Errorf("failed to get replica list: %v", err)
	}

	replicaModeMap := make(map[string]types.Mode)
	for _, replica := range replicas {
		replicaModeMap[replica.Address] = replica.Mode
	}

	return replicaModeMap, nil
}

func fetchAllBackups(c *cli.Context) error {
	backupProgressList := make(map[string]*sync.BackupStatusInfo)

	cli := getCli(c)
	backupReplicaMap, err := cli.BackupReplicaMappingGet()
	if err != nil {
		return fmt.Errorf("failed to get list of backupIDs: %v", err)
	}

	replicaModeMap, err := getReplicaModeMap(c)
	if err != nil {
		return err
	}

	for backupID, replicaAddress := range backupReplicaMap {
		// Only a replica in RW mode can create backups.
		if mode := replicaModeMap[replicaAddress]; mode != types.RW {
			err := cli.BackupReplicaMappingDelete(backupID)
			if err != nil {
				return err
			}
			backupProgressList[backupID] = &sync.BackupStatusInfo{
				ReplicaAddress: replicaAddress,
				State:          "error",
				Error: fmt.Sprintf("Failed to get backup status on %s for %v: %v",
					replicaAddress, backupID, "unknown replica"),
			}
			continue
		}

		status, err := getBackupStatus(c, backupID, replicaAddress)
		if err != nil {
			return err
		}

		if status == nil {
			continue
		}
		if strings.Contains(status.Error, "backup not found") {
			err := cli.BackupReplicaMappingDelete(backupID)
			if err != nil {
				return err
			}
			backupProgressList[backupID] = &sync.BackupStatusInfo{
				ReplicaAddress: replicaAddress,
				State:          "error",
				Error: fmt.Sprintf("Failed to get backup status on %s for %v: %v",
					replicaAddress, backupID, "backup not found"),
			}
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
	if !present || replicaAddress == "" {
		return fmt.Errorf("failed to get backup status for %v: %v",
			backupID, "replica not found")
	}

	replicaModeMap, err := getReplicaModeMap(c)
	if err != nil {
		return err
	}
	if mode := replicaModeMap[replicaAddress]; mode != types.RW {
		_ = client.BackupReplicaMappingDelete(backupID)
		return fmt.Errorf("Failed to get backup status on %s for %v: %v",
			replicaAddress, backupID, "unknown replica")
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

func RestoreStatusCmd() cli.Command {
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

	credential, err := util.GetBackupCredential(dest)
	if err != nil {
		return err
	}

	backup, err := task.CreateBackup(snapshot, dest, labels, credential)
	if err != nil {
		return err
	}
	backupCreateInfo, err := json.Marshal(backup)
	if err != nil {
		return err
	}
	fmt.Println(string(backupCreateInfo))

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

	credential, err := util.GetBackupCredential(backup)
	if err != nil {
		return err
	}

	backupURL := util.UnescapeURL(backup)
	if backup, err := backupstore.InspectBackup(backupURL); err != nil || backup == nil {
		return errors.Wrapf(err, "no backups found with url %v", backupURL)
	}

	if err := task.RestoreBackup(backupURL, credential); err != nil {
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
	backupURL := util.UnescapeURL(backup)
	backupName, err := backupstore.GetBackupFromBackupURL(backupURL)
	if err != nil {
		return err
	}

	credential, err := util.GetBackupCredential(backup)
	if err != nil {
		return err
	}

	lastRestored := c.String("last-restored")

	if err := task.RestoreBackupIncrementally(backupURL, backupName, lastRestored, credential); err != nil {
		logrus.Errorf("failed to perform incremental restore: %v", err)
		return err
	}
	return nil
}

func restoreStatus(c *cli.Context) error {
	task := sync.NewTask(c.GlobalString("url"))

	rsMap, err := task.RestoreStatus()
	if err != nil {
		return err
	}

	restoreStatus, err := json.Marshal(rsMap)
	if err != nil {
		return err
	}
	fmt.Println(string(restoreStatus))

	return nil
}
