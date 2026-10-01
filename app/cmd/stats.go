package cmd

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

// Journal flush operations since last flush
func Journal() *cli.Command {
	return &cli.Command{
		Name: "journal",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "limit",
				Value: 0,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			controllerClient, err := getControllerClient(c)
			if err != nil {
				logrus.Fatalln("Error running journal command:", err)
				return err
			}
			defer func() {
				if errClose := controllerClient.Close(); errClose != nil {
					logrus.WithError(errClose).Error("Failed to close controller client")
				}
			}()

			if err = controllerClient.JournalList(c.Int("limit")); err != nil {
				logrus.Fatalln("Error running journal command:", err)
				return err
			}
			return nil
		},
	}
}
