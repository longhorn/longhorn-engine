package main

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine/pkg/controller/client"
	"github.com/longhorn/longhorn-engine/pkg/meta"
)

func VersionCmd() cli.Command {
	return cli.Command{
		Name: "version",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name: "client-only",
			},
		},
		Action: func(c *cli.Context) {
			if err := version(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
			}
		},
	}
}

type VersionOutput struct {
	ClientVersion *meta.VersionOutput `json:"clientVersion"`
	ServerVersion *meta.VersionOutput `json:"serverVersion"`
}

func version(c *cli.Context) error {
	clientVersion := meta.GetVersion()
	v := VersionOutput{ClientVersion: &clientVersion}

	if !c.Bool("client-only") {
		url := c.GlobalString("url")
		volumeName := c.GlobalString("volume-name")
		engineInstanceName := c.GlobalString("engine-instance-name")
		controllerClient, err := client.NewControllerClient(url, volumeName, engineInstanceName)
		if err != nil {
			return err
		}
		defer controllerClient.Close()

		version, err := controllerClient.VersionDetailGet()
		if err != nil {
			return err
		}
		v.ServerVersion = version
	}
	output, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}
