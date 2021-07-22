package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

// Journal flush operations since last flush
func Journal() cli.Command {
	return cli.Command{
		Name: "journal",
		Flags: []cli.Flag{
			cli.IntFlag{
				Name:  "limit",
				Value: 0,
			},
		},
		Action: func(c *cli.Context) {
			controllerClient, err := getControllerClient(c)
			if err != nil {
				logrus.Fatalln("Error running journal command:", err)
			}
			defer controllerClient.Close()

			if err = controllerClient.JournalList(c.Int("limit")); err != nil {
				logrus.Fatalln("Error running journal command:", err)
			}
		},
	}
}
