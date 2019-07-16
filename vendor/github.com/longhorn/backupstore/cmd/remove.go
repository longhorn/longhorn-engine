package cmd

import (
	"fmt"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/util"
)

func BackupRemoveCmd() cli.Command {
	return cli.Command{
		Name:    "remove",
		Aliases: []string{"rm", "delete"},
		Usage:   "remove a backup or backup volume in objectstore: rm <backup>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "volume",
				Usage: "volume name, only use it when deleting a backup volume with dest URL",
			},
		},
		Action: cmdBackupRemove,
	}
}

func cmdBackupRemove(c *cli.Context) {
	if err := doBackupRemove(c); err != nil {
		panic(err)
	}
}

func doBackupRemove(c *cli.Context) error {
	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args()[0]
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}

	volumeName := c.String("volume")
	if volumeName == "" {
		destURL = util.UnescapeURL(destURL)
		if err := backupstore.DeleteDeltaBlockBackup(destURL); err != nil {
			return err
		}
	} else {
		if !util.ValidateName(volumeName) {
			return fmt.Errorf("invalid backup volume name %v", volumeName)
		}
		if err := backupstore.DeleteBackupVolume(volumeName, destURL); err != nil {
			return err
		}
	}
	return nil
}
