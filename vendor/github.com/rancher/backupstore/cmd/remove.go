package cmd

import (
	"github.com/urfave/cli"

	"github.com/rancher/backupstore"
	"github.com/rancher/backupstore/util"
)

func BackupRemoveCmd() cli.Command {
	return cli.Command{
		Name:    "remove",
		Aliases: []string{"rm", "delete"},
		Usage:   "remove a backup in objectstore: rm <backup>",
		Action:  cmdBackupRemove,
	}
}

func cmdBackupRemove(c *cli.Context) {
	if err := doBackupRemove(c); err != nil {
		panic(err)
	}
}

func doBackupRemove(c *cli.Context) error {
	if c.NArg() == 0 {
		return RequiredMissingError("backup URL")
	}
	backupURL := c.Args()[0]
	if backupURL == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL = util.UnescapeURL(backupURL)

	if err := backupstore.DeleteDeltaBlockBackup(backupURL); err != nil {
		return err
	}
	return nil
}
