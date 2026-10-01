package cmd

import (
	"github.com/urfave/cli/v3"

	"github.com/longhorn/backupstore/cmd"
)

func SystemBackupCmd() *cli.Command {
	return &cli.Command{
		Name: "system-backup",
		Commands: []*cli.Command{
			cmd.SystemBackupUploadCmd(),
			cmd.SystemBackupDeleteCmd(),
			cmd.SystemBackupDownloadCmd(),
			cmd.SystemBackupListCmd(),
			cmd.SystemBackupGetConfigCmd(),
		},
	}
}
