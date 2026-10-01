package cmd

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/backupstore"
)

func BackupCleanupAllMountsCmd() *cli.Command {
	return &cli.Command{
		Name:  "cleanup-all-mounts",
		Usage: "clean up unused mount points",
		Action: func(ctx context.Context, c *cli.Command) error {
			cmdCleanUpAllMounts(c)
			return nil
		},
	}
}

func cmdCleanUpAllMounts(c *cli.Command) {
	if err := doCleanUpAllMounts(c); err != nil {
		panic(err)
	}
}

func doCleanUpAllMounts(c *cli.Command) error {
	log := logrus.WithFields(logrus.Fields{"Command": "cleanup-mount"})

	if err := backupstore.CleanUpAllMounts(); err != nil {
		log.WithError(err).Warnf("Failed to clean up mount points")
		return err
	}

	return nil
}
