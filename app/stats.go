package app

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

func LsStats() cli.Command {
	return cli.Command{
		Name:      "ls-stats",
		ShortName: "stats",
		Action: func(c *cli.Context) {
			controllerClient := getCli(c)
			err := controllerClient.ListStats()
			if err != nil {
				logrus.Fatalln("Error running stats command:", err)
			}
		},
	}
}
