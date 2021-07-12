package cmd

import (
	"fmt"

	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/util"
)

func GetConfigMetadataCmd() cli.Command {
	return cli.Command{
		Name:        "head",
		Usage:       "get the config metadata",
		Description: "this returns the last modification time of a config file for now",
		Action:      cmdGetConfigMetadata,
	}
}

func cmdGetConfigMetadata(c *cli.Context) {
	if err := doGetConfigMetadata(c); err != nil {
		panic(err)
	}
}

func doGetConfigMetadata(c *cli.Context) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args()[0]
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}
	destURL = util.UnescapeURL(destURL)

	info, err := backupstore.GetConfigMetadata(destURL)
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
