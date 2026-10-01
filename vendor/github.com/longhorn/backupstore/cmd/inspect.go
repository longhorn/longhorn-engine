package cmd

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/util"
)

func InspectVolumeCmd() *cli.Command {
	return &cli.Command{
		Name:  "inspect-volume",
		Usage: "inspect a volume: inspect <volume>",
		Action: func(ctx context.Context, c *cli.Command) error {
			cmdInspectVolume(c)
			return nil
		},
	}
}

func cmdInspectVolume(c *cli.Command) {
	if err := doInspectVolume(c); err != nil {
		panic(err)
	}
}

func doInspectVolume(c *cli.Command) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args().First()
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}
	destURL = util.UnescapeURL(destURL)

	info, err := backupstore.InspectVolume(destURL)
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

func InspectBackupCmd() *cli.Command {
	return &cli.Command{
		Name:  "inspect",
		Usage: "inspect a backup: inspect <backup>",
		Action: func(ctx context.Context, c *cli.Command) error {
			cmdInspectBackup(c)
			return nil
		},
	}
}

func cmdInspectBackup(c *cli.Command) {
	if err := doInspectBackup(c); err != nil {
		panic(err)
	}
}

func doInspectBackup(c *cli.Command) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args().First()
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}
	destURL = util.UnescapeURL(destURL)

	info, err := backupstore.InspectBackup(destURL)
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
