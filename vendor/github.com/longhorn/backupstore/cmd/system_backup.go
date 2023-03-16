package cmd

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore/systembackup"
	"github.com/longhorn/backupstore/util"
)

func SystemBackupUploadCmd() cli.Command {
	return cli.Command{
		Name:  "upload",
		Usage: "upload a system backup zip file to the object store: upload <local-path> <system-backup-url> --git-commit <longhorn-git-commit> --manager-image <manager-image> --engine-image <engine-image>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "git-commit",
				Usage: "specify the git commit of the system backup",
			},
			cli.StringFlag{
				Name:  "manager-image",
				Usage: "specify the manager image to use for system restore",
			},
			cli.StringFlag{
				Name:  "engine-image",
				Usage: "specify the engine image to use for system restore",
			},
		},
		Action: func(c *cli.Context) {
			if err := uploadSystemBackup(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run upload system-backup command")
			}
		},
	}
}

func SystemBackupDownloadCmd() cli.Command {
	return cli.Command{
		Name:  "download",
		Usage: "download a system backup zip file from the object store: download <system-backup-url> <local-path>",
		Action: func(c *cli.Context) {
			if err := downloadSystemBackup(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run download system backup command")
			}
		},
	}
}

func SystemBackupGetConfigCmd() cli.Command {
	return cli.Command{
		Name:  "get-config",
		Usage: "output the system backup config from the object store: get-config <system-backup-url>",
		Action: func(c *cli.Context) {
			if err := getSystemBackupConfig(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run get-config system backup command")
			}
		},
	}
}

func SystemBackupListCmd() cli.Command {
	return cli.Command{
		Name:  "list",
		Usage: "list system backups in the object store: list <backup-target-url>",
		Flags: []cli.Flag{},
		Action: func(c *cli.Context) {
			if err := listSystemBackup(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run list system backup command")
			}
		},
	}
}

func SystemBackupDeleteCmd() cli.Command {
	return cli.Command{
		Name:  "delete",
		Usage: "delete a system backup in the object store: delete <system-backup-url>",
		Action: func(c *cli.Context) {
			if err := deleteSystemBackup(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run delete system backup command")
			}
		},
	}
}

func uploadSystemBackup(c *cli.Context) error {
	if c.NArg() != 2 {
		return fmt.Errorf("missing required parameters to upload system backup")
	}

	source := c.Args()[0]

	backupTargetURL, longhornVersion, systemBackupName, err := systembackup.ParseSystemBackupURL(c.Args()[1])
	if err != nil {
		return err
	}

	longhornGitCommit := c.String("git-commit")
	if longhornGitCommit == "" {
		return fmt.Errorf("missing required parameter --git-commit")
	}

	managerImage := c.String("manager-image")
	if managerImage == "" {
		return fmt.Errorf("missing required parameter --manager-image")
	}

	engineImage := c.String("engine-image")
	if engineImage == "" {
		return fmt.Errorf("missing required parameter --engine-image")
	}

	sha256sum, err := util.GetFileChecksum(source)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v checksum", source)
	}

	config := &systembackup.Config{
		Name:              systemBackupName,
		LonghornVersion:   longhornVersion,
		LonghornGitCommit: longhornGitCommit,
		BackupTargetURL:   backupTargetURL,
		ManagerImage:      managerImage,
		EngineImage:       engineImage,
		CreatedAt:         time.Now().UTC(),
		Checksum:          sha256sum,
	}

	return systembackup.Upload(source, config)
}

func getSystemBackupConfig(c *cli.Context) error {
	if c.NArg() == 0 {
		return fmt.Errorf("missing required parameter for system backup URL")
	}

	backupTargetURL, longhornVersion, systemBackupName, err := systembackup.ParseSystemBackupURL(c.Args()[0])
	if err != nil {
		return err
	}

	cfg, err := systembackup.LoadConfig(systemBackupName, longhornVersion, backupTargetURL)
	if err != nil {
		return err
	}

	data, err := ResponseOutput(cfg)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func downloadSystemBackup(c *cli.Context) error {
	if c.NArg() != 2 {
		return fmt.Errorf("missing required parameters to download system backup")
	}

	backupTargetURL, longhornVersion, systemBackupName, err := systembackup.ParseSystemBackupURL(c.Args()[0])
	if err != nil {
		return err
	}

	destination := c.Args()[1]

	cfg, err := systembackup.LoadConfig(systemBackupName, longhornVersion, backupTargetURL)
	if err != nil {
		return err
	}

	return systembackup.Download(destination, cfg)
}

func listSystemBackup(c *cli.Context) error {
	if c.NArg() == 0 {
		return fmt.Errorf("missing required parameter for backup target URL")
	}

	bsURL := c.Args()[0]

	systemBackups, err := systembackup.List(bsURL)
	if err != nil {
		return err
	}

	resp, err := ResponseOutput(systemBackups)
	if err != nil {
		return err
	}

	fmt.Println(string(resp))
	return nil
}

func deleteSystemBackup(c *cli.Context) error {
	if c.NArg() == 0 {
		return fmt.Errorf("missing required parameter for system backup URL")
	}

	backupTargetURL, longhornVersion, systemBackupName, err := systembackup.ParseSystemBackupURL(c.Args()[0])
	if err != nil {
		return err
	}

	cfg := &systembackup.Config{
		Name:            systemBackupName,
		LonghornVersion: longhornVersion,
		BackupTargetURL: backupTargetURL,
	}
	return systembackup.Delete(cfg)
}
