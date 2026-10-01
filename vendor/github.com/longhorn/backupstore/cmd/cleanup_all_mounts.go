package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
)

func BackupCleanupAllMountsCmd() cli.Command {
	return cli.Command{
		Name:   "cleanup-all-mounts",
		Usage:  "clean up unused mount points",
		Action: cmdCleanUpAllMounts,
	}
}

func cmdCleanUpAllMounts(c *cli.Context) {
	if err := doCleanUpAllMounts(c); err != nil {
		panic(err)
	}
}

func doCleanUpAllMounts(c *cli.Context) error {
	log := logrus.WithFields(logrus.Fields{"Command": "cleanup-mount"})

	if err := backupstore.CleanUpAllMounts(); err != nil {
		log.WithError(err).Warnf("Failed to clean up mount points")
		return err
	}

	return nil
}
