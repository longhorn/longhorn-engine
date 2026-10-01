package cmd

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/util"
)

func GetConfigMetadataCmd() *cli.Command {
	return &cli.Command{
		Name:        "head",
		Usage:       "get the config metadata",
		Description: "this returns the last modification time of a config file for now",
		Action: func(ctx context.Context, c *cli.Command) error {
			cmdGetConfigMetadata(c)
			return nil
		},
	}
}

func cmdGetConfigMetadata(c *cli.Command) {
	if err := doGetConfigMetadata(c); err != nil {
		panic(err)
	}
}

func doGetConfigMetadata(c *cli.Command) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args().First()
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
