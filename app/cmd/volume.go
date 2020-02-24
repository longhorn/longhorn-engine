package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func InfoCmd() cli.Command {
	return cli.Command{
		Name: "info",
		Action: func(c *cli.Context) {
			if err := info(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
			}
		},
	}
}

func ExpandCmd() cli.Command {
	return cli.Command{
		Name: "expand",
		Flags: []cli.Flag{
			cli.Int64Flag{
				Name:  "size",
				Usage: "The new volume size. It should be larger than the current size",
			},
		},
		Action: func(c *cli.Context) {
			if err := expand(c); err != nil {
				logrus.Fatalf("Error running expand command: %v", err)
			}
		},
	}
}

func info(c *cli.Context) error {
	cli := getCli(c)

	volumeInfo, err := cli.VolumeGet()
	if err != nil {
		return err
	}

	output, err := json.MarshalIndent(volumeInfo, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func expand(c *cli.Context) error {
	cli := getCli(c)

	size := c.Int64("size")

	err := cli.VolumeExpand(size)
	if err != nil {
		return err
	}

	return nil
}
