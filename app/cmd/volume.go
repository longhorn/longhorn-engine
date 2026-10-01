package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func InfoCmd() *cli.Command {
	return &cli.Command{
		Name: "info",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := info(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
				return err
			}
			return nil
		},
	}
}

func ExpandCmd() *cli.Command {
	return &cli.Command{
		Name: "expand",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "size",
				Usage: "The new volume size. It should be larger than the current size",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := expand(c); err != nil {
				logrus.WithError(err).Fatalf("Error running expand command")
				return err
			}
			return nil
		},
	}
}

func UnmapMarkSnapChainRemovedCmd() *cli.Command {
	return &cli.Command{
		Name:    "unmap-mark-snap-chain-removed",
		Aliases: []string{"unmap-mark-snap"},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name: "enable",
			},
			&cli.BoolFlag{
				Name: "disable",
			},
		},
		Usage: "Enable marking the current snapshot chain as removed before unmapping",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := unmapMarkSnapChainRemoved(c); err != nil {
				logrus.Fatalf("Error running unmap-mark-snap-chain-removed command: %v", err)
				return err
			}
			return nil
		},
	}
}

func FrontendCmd() *cli.Command {
	return &cli.Command{
		Name: "frontend",
		Commands: []*cli.Command{
			FrontendStartCmd(),
			FrontendShutdownCmd(),
		},
	}
}

func FrontendStartCmd() *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "start <frontend name>",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := startFrontend(c); err != nil {
				logrus.WithError(err).Fatalf("Error running frontend start command")
				return err
			}
			return nil
		},
	}
}

func FrontendShutdownCmd() *cli.Command {
	return &cli.Command{
		Name:  "shutdown",
		Usage: "shutdown",
		Action: func(ctx context.Context, c *cli.Command) error {
			if err := shutdownFrontend(c); err != nil {
				logrus.WithError(err).Fatalf("Error running frontend shutdown command")
				return err
			}
			return nil
		},
	}
}

func info(c *cli.Command) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

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

func expand(c *cli.Command) error {
	size := c.Int64("size")
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	return controllerClient.VolumeExpand(size)
}

func startFrontend(c *cli.Command) error {
	frontendName := c.Args().First()
	if frontendName == "" {
		return fmt.Errorf("missing required parameter frontendName")
	}

	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	return controllerClient.VolumeFrontendStart(frontendName)
}

func shutdownFrontend(c *cli.Command) error {
	controllerClient, err := getControllerClient(c)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	return controllerClient.VolumeFrontendShutdown()
}

func unmapMarkSnapChainRemoved(c *cli.Command) error {
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
	defer func() {
		if errClose := controllerClient.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close controller client")
		}
	}()

	return controllerClient.VolumeUnmapMarkSnapChainRemovedSet(enabled)
}
