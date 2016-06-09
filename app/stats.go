package app

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
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
			controllerClient := getCli(c)
			err := controllerClient.ListJournal(c.Int("limit"))
			if err != nil {
				logrus.Fatalln("Error running journal command:", err)
			}
		},
	}
}
