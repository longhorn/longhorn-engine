package main

import (
	"encoding/json"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/longhorn-engine/meta"
)

func VersionCmd() cli.Command {
	return cli.Command{
		Name: "version",
		Action: func(c *cli.Context) {
			if err := version(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
			}
		},
	}
}

func version(c *cli.Context) error {
	v := meta.GetVersion()

	output, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}
