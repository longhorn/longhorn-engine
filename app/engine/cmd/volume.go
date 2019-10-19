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
