package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore/cmd"

	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
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
			cmd.InspectVolumeCmd(),
			cmd.InspectBackupCmd(),
			cmd.GetConfigMetadataCmd(),
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
			cli.StringFlag{
				Name:  "backing-image-name",
				Usage: "specify backing image name of the volume for backup",
			},
			cli.StringFlag{
				Name:  "backing-image-checksum",
				Usage: "specify checksum of backing image file",
			},
			cli.StringSliceFlag{
				Name:  "label",
				Usage: "specify labels for backup, in the format of `--label key1=value1 --label key2=value2`",
			},
			cli.StringFlag{
				Name:  "backup-name",
				Usage: "specify the backup name. If it is not set, a random name will be generated automatically",
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

func getReplicaModeMap(replicas []*types.ControllerReplicaInfo) map[string]types.Mode {
	replicaModeMap := make(map[string]types.Mode)
	for _, replica := range replicas {
		replicaModeMap[replica.Address] = replica.Mode
	}

	return replicaModeMap
}

func fetchAllBackups(c *cli.Context) error {
	backupProgressList := make(map[string]*sync.BackupStatusInfo)

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	backupReplicaMap, err := controllerClient.BackupReplicaMappingGet()
	if err != nil {
		return fmt.Errorf("failed to get list of backupIDs: %v", err)
	}

	replicas, err := controllerClient.ReplicaList()
	if err != nil {
		return fmt.Errorf("failed to get replica list: %v", err)
	}
	replicaModeMap := getReplicaModeMap(replicas)

	clients := map[string]*replicaClient.ReplicaClient{}
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	for backupID, replicaAddress := range backupReplicaMap {
		// Only a replica in RW mode can create backups.
		if mode := replicaModeMap[replicaAddress]; mode != types.RW {
			err := controllerClient.BackupReplicaMappingDelete(backupID)
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

		repClient := clients[replicaAddress]
		if repClient == nil {
			repClient, err = replicaClient.NewReplicaClient(replicaAddress)
			if err != nil {
				err := fmt.Errorf("cannot create a replica client for IP[%v]: %v", replicaAddress, err)
				logrus.Error(err.Error())
				return err
			}
			clients[replicaAddress] = repClient
		}

		status, err := sync.FetchBackupStatus(repClient, backupID, replicaAddress)
		if err != nil {
			return err
		}

		if status == nil {
			continue
		}
		if strings.Contains(status.Error, "backup not found") {
			err := controllerClient.BackupReplicaMappingDelete(backupID)
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

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	br, err := controllerClient.BackupReplicaMappingGet()
	if err != nil {
		return err
	}
	replicaAddress, present := br[backupID]
	if !present || replicaAddress == "" {
		return fmt.Errorf("failed to get backup status for %v: %v",
			backupID, "replica not found")
	}

	replicas, err := controllerClient.ReplicaList()
	if err != nil {
		return fmt.Errorf("failed to get replica list: %v", err)
	}
	replicaModeMap := getReplicaModeMap(replicas)

	if mode := replicaModeMap[replicaAddress]; mode != types.RW {
		_ = controllerClient.BackupReplicaMappingDelete(backupID)
		return fmt.Errorf("Failed to get backup status on %s for %v: %v",
			replicaAddress, backupID, "unknown replica")
	}

	repClient, err := replicaClient.NewReplicaClient(replicaAddress)
	if err != nil {
		logrus.Errorf("Cannot create a replica client for IP[%v]: %v", replicaAddress, err)
		return err
	}
	defer repClient.Close()

	status, err := sync.FetchBackupStatus(repClient, backupID, replicaAddress)
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
		Usage: "restore a backup to current volume: restore <backup>",
		Action: func(c *cli.Context) {
			if err := restoreBackup(c); err != nil {
				errInfo, jsonErr := json.MarshalIndent(err, "", "\t")
				if jsonErr != nil {
					logrus.Errorf("Cannot marshal err [%v] to json: %v", err, jsonErr)
				} else {
					// If the error is not `TaskError`, the marshaled result is an empty json string.
					if string(errInfo) != "{}" {
						fmt.Println(string(errInfo))
					} else {
						fmt.Println(err.Error())
					}
				}
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
	dest := c.String("dest")
	if dest == "" {
		return fmt.Errorf("Missing required parameter --dest")
	}

	snapshot := c.Args().First()
	if snapshot == "" {
		return fmt.Errorf("Missing required parameter snapshot")
	}

	biName := c.String("backing-image-name")
	biChecksum := c.String("backing-image-checksum")

	labels := c.StringSlice("label")
	if labels != nil {
		// Only validate it here, the real parse is done at backend
		if _, err := util.ParseLabels(labels); err != nil {
			return errors.Wrap(err, "cannot parse backup labels")
		}
	}

	backupName := c.String("backup-name")

	credential, err := util.GetBackupCredential(dest)
	if err != nil {
		return err
	}

	url := c.GlobalString("url")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
	if err != nil {
		return err
	}

	backup, err := task.CreateBackup(backupName, snapshot, dest, biName, biChecksum, labels, credential)
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
	url := c.GlobalString("url")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
	if err != nil {
		return err
	}

	backup := c.Args().First()
	if backup == "" {
		return fmt.Errorf("Missing required parameter backup")
	}
	backupURL := util.UnescapeURL(backup)

	credential, err := util.GetBackupCredential(backup)
	if err != nil {
		return err
	}

	if err := task.RestoreBackup(backupURL, credential); err != nil {
		return err
	}

	return nil
}

func restoreStatus(c *cli.Context) error {
	url := c.GlobalString("url")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := sync.NewTask(ctx, url)
	if err != nil {
		return err
	}

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
