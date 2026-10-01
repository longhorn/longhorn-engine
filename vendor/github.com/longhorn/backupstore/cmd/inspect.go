package cmd

import (
	"fmt"

	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/util"
)

func InspectVolumeCmd() cli.Command {
	return cli.Command{
		Name:   "inspect-volume",
		Usage:  "inspect a volume: inspect <volume>",
		Action: cmdInspectVolume,
	}
}

func cmdInspectVolume(c *cli.Context) {
	if err := doInspectVolume(c); err != nil {
		panic(err)
	}
}

func doInspectVolume(c *cli.Context) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args()[0]
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

func InspectBackupCmd() cli.Command {
	return cli.Command{
		Name:   "inspect",
		Usage:  "inspect a backup: inspect <backup>",
		Action: cmdInspectBackup,
	}
}

func cmdInspectBackup(c *cli.Context) {
	if err := doInspectBackup(c); err != nil {
		panic(err)
	}
}

func doInspectBackup(c *cli.Context) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args()[0]
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
