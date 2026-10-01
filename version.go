package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-engine/pkg/controller/client"
	"github.com/longhorn/longhorn-engine/pkg/meta"
)

func VersionCmd() *cli.Command {
	return &cli.Command{
		Name: "version",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name: "client-only",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := version(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
				return err
			}
			return nil
		},
	}
}

type VersionOutput struct {
	ClientVersion *meta.VersionOutput `json:"clientVersion"`
	ServerVersion *meta.VersionOutput `json:"serverVersion"`
}

func version(c *cli.Command) error {
	clientVersion := meta.GetVersion()
	v := VersionOutput{ClientVersion: &clientVersion}

	if !c.Bool("client-only") {
		url := c.String("url")
		volumeName := c.String("volume-name")
		engineInstanceName := c.String("engine-instance-name")
		controllerClient, err := client.NewControllerClient(url, volumeName, engineInstanceName)
		if err != nil {
			return err
		}
		defer func() {
			if errClose := controllerClient.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close controller client")
			}
		}()

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
