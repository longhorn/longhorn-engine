package cmd

import (
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore/cmd"
)

func SystemBackupCmd() cli.Command {
	return cli.Command{
		Name: "system-backup",
		Subcommands: []cli.Command{
			cmd.SystemBackupUploadCmd(),
			cmd.SystemBackupDeleteCmd(),
			cmd.SystemBackupDownloadCmd(),
			cmd.SystemBackupListCmd(),
			cmd.SystemBackupGetConfigCmd(),
		},
	}
}
