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
				logrus.WithError(err).Fatalf("Error running expand command")
			}
		},
	}
}

func UnmapMarkSnapChainRemovedCmd() cli.Command {
	return cli.Command{
		Name:      "unmap-mark-snap-chain-removed",
		ShortName: "unmap-mark-snap",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name: "enable",
			},
			cli.BoolFlag{
				Name: "disable",
			},
		},
		Usage: "Enable marking the current snapshot chain as removed before unmapping",
		Action: func(c *cli.Context) {
			if err := unmapMarkSnapChainRemoved(c); err != nil {
				logrus.Fatalf("Error running unmap-mark-snap-chain-removed command: %v", err)
			}
		},
	}
}

func FrontendCmd() cli.Command {
	return cli.Command{
		Name: "frontend",
		Subcommands: []cli.Command{
			FrontendStartCmd(),
			FrontendShutdownCmd(),
		},
	}
}

func FrontendStartCmd() cli.Command {
	return cli.Command{
		Name:  "start",
		Usage: "start <frontend name>",
		Action: func(c *cli.Context) {
			if err := startFrontend(c); err != nil {
				logrus.WithError(err).Fatalf("Error running frontend start command")
			}
		},
	}
}

func FrontendShutdownCmd() cli.Command {
	return cli.Command{
		Name:  "shutdown",
		Usage: "shutdown",
		Action: func(c *cli.Context) {
			if err := shutdownFrontend(c); err != nil {
				logrus.WithError(err).Fatalf("Error running frontend shutdown command")
			}
		},
	}
}

func info(c *cli.Context) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	volumeInfo, err := controllerClient.VolumeGet()
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
	size := c.Int64("size")
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	return controllerClient.VolumeExpand(size)
}

func startFrontend(c *cli.Context) error {
	frontendName := c.Args().First()
	if frontendName == "" {
		return fmt.Errorf("missing required parameter frontendName")
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	return controllerClient.VolumeFrontendStart(frontendName)
}

func shutdownFrontend(c *cli.Context) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	return controllerClient.VolumeFrontendShutdown()
}

func unmapMarkSnapChainRemoved(c *cli.Context) error {
	enabled := c.Bool("enable")
	disabled := c.Bool("disable")
	if enabled && disabled {
		return fmt.Errorf("cannot enable and disable this option simultaneously")
	}
	if !enabled && !disabled {
		return fmt.Errorf("this option is not specified")
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer controllerClient.Close()

	return controllerClient.VolumeUnmapMarkSnapChainRemovedSet(enabled)
}
