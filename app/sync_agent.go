package app

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/sync/agent"
)

func SyncAgentCmd() cli.Command {
	return cli.Command{
		Name:      "sync-agent",
		UsageText: "longhorn controller DIRECTORY SIZE",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9504",
			},
			cli.StringFlag{
				Name:  "listen-port-range",
				Value: "9700-9800",
			},
		},
		Action: func(c *cli.Context) {
			if err := startSyncAgent(c); err != nil {
				logrus.Fatal(err)
			}
		},
	}
}

func startSyncAgent(c *cli.Context) error {
	listen := c.String("listen")
	portRange := c.String("listen-port-range")

	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return fmt.Errorf("Invalid format for range: %s", portRange)
	}

	start, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return err
	}

	end, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return err
	}

	server := agent.NewServer(start, end)
	router := agent.NewRouter(server)
	logrus.Infof("Listening on sync %s", listen)

	return http.ListenAndServe(listen, router)
}
