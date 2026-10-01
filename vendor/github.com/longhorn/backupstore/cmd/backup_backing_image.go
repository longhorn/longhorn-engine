package cmd

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/backupbackingimage"
	"github.com/longhorn/backupstore/util"
)

func BackupBackingImageListCmd() *cli.Command {
	return &cli.Command{
		Name:  "ls-backing-image",
		Usage: "list backup backing images in backupstore: ls-backing-image <dest>",
		Action: func(ctx context.Context, c *cli.Command) error {
			cmdBackupBackingImageList(c)
			return nil
		},
	}
}

func cmdBackupBackingImageList(c *cli.Command) {
	if err := doBackupBackingImageList(c); err != nil {
		panic(err)
	}
}

func doBackupBackingImageList(c *cli.Command) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args().First()
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}

	bsdriver, err := backupstore.GetBackupStoreDriver(destURL)
	if err != nil {
		return err
	}
	list, err := backupbackingimage.GetAllBackupBackingImageNames(bsdriver)
	if err != nil {
		return err
	}

	data, err := ResponseOutput(list)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func InspectBackingImageCmd() *cli.Command {
	return &cli.Command{
		Name:  "inspect-backing-image",
		Usage: "output the backup backing image config from the object store: inspect-backing-image <backup-url>",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := inspectBackupBackingImageConfig(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run inspect-backing-image command")
				return err
			}
			return nil
		},
	}
}

func inspectBackupBackingImageConfig(c *cli.Command) error {
	if c.NArg() == 0 {
		return fmt.Errorf("missing required parameter for backup backing image URL")
	}

	if c.NArg() == 0 {
		return RequiredMissingError("backup-url")
	}
	backupURL := c.Args().First()
	if backupURL == "" {
		return RequiredMissingError("backup-url")
	}
	backupURL = util.UnescapeURL(backupURL)

	info, err := backupbackingimage.InspectBackupBackingImage(backupURL)
	if err != nil {
		return err
	}
	data, err := ResponseOutput(info)
	if err != nil {
		return err
	}

	fmt.Println(string(data))
	return nil
}

func BackupBackingImageRemoveCmd() *cli.Command {
	return &cli.Command{
		Name:  "rm-backing-image",
		Usage: "remove a backup backing image in objectstore",
		Action: func(ctx context.Context, c *cli.Command) error {
			cmdBackupBackingImageRemove(c)
			return nil
		},
	}
}

func cmdBackupBackingImageRemove(c *cli.Command) {
	if err := doBackupBackingImageRemove(c); err != nil {
		panic(err)
	}
}

func doBackupBackingImageRemove(c *cli.Command) error {
	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args().First()
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}

	destURL = util.UnescapeURL(destURL)
	err := backupbackingimage.RemoveBackingImageBackup(destURL)
	return err
}
