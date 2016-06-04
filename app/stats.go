package app

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

func LsStats() cli.Command {
	return cli.Command{
		Name:      "ls-stats",
		ShortName: "stats",
		Flags: []cli.Flag{
			cli.IntFlag{
				Name:  "limit",
				Value: 0,
			},
		},
		Action: func(c *cli.Context) {
			controllerClient := getCli(c)
			err := controllerClient.ListStats(c.Int("limit"))
			if err != nil {
				logrus.Fatalln("Error running stats command:", err)
			}
		},
	}
}
