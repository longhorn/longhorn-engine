package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-instance-manager/client"
	"github.com/longhorn/longhorn-instance-manager/util"
)

func ProcessCmd() cli.Command {
	return cli.Command{
		Name: "process",
		Subcommands: []cli.Command{
			ProcessCreateCmd(),
			ProcessDeleteCmd(),
			ProcessGetCmd(),
			ProcessListCmd(),
		},
	}
}

func ProcessCreateCmd() cli.Command {
	return cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "binary",
			},
			cli.IntFlag{
				Name: "port-count",
			},
			cli.StringSliceFlag{
				Name:  "port-args",
				Usage: "Automatically add additional arguments when starting the process. In case of space, use `,` instead.",
			},
		},
		Action: func(c *cli.Context) {
			if err := createProcess(c); err != nil {
				logrus.Fatalf("Error running process create command: %v.", err)
			}
		},
	}
}

func createProcess(c *cli.Context) error {
	url := c.GlobalString("url")

	cli := client.NewProcessManagerClient(url)
	process, err := cli.ProcessCreate(c.String("name"), c.String("binary"),
		c.Int("port-count"), c.Args(), c.StringSlice("port-args"))
	if err != nil {
		return fmt.Errorf("failed to create process: %v", err)
	}
	return util.PrintJSON(process)
}

func ProcessDeleteCmd() cli.Command {
	return cli.Command{
		Name: "delete",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
		},
		Action: func(c *cli.Context) {
			if err := deleteProcess(c); err != nil {
				logrus.Fatalf("Error running process delete command: %v.", err)
			}
		},
	}
}

func deleteProcess(c *cli.Context) error {
	url := c.GlobalString("url")

	cli := client.NewProcessManagerClient(url)
	process, err := cli.ProcessDelete(c.String("name"))
	if err != nil {
		return fmt.Errorf("failed to delete process: %v", err)
	}
	return util.PrintJSON(process)
}

func ProcessGetCmd() cli.Command {
	return cli.Command{
		Name: "get",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
		},
		Action: func(c *cli.Context) {
			if err := getProcess(c); err != nil {
				logrus.Fatalf("Error running process get command: %v.", err)
			}
		},
	}
}

func getProcess(c *cli.Context) error {
	url := c.GlobalString("url")

	cli := client.NewProcessManagerClient(url)
	process, err := cli.ProcessGet(c.String("name"))
	if err != nil {
		return fmt.Errorf("failed to delete process: %v", err)
	}
	return util.PrintJSON(process)
}

func ProcessListCmd() cli.Command {
	return cli.Command{
		Name:      "list",
		ShortName: "ls",
		Action: func(c *cli.Context) {
			if err := listProcess(c); err != nil {
				logrus.Fatalf("Error running engine stop command: %v.", err)
			}
		},
	}
}

func listProcess(c *cli.Context) error {
	url := c.GlobalString("url")

	cli := client.NewProcessManagerClient(url)
	processes, err := cli.ProcessList()
	if err != nil {
		return fmt.Errorf("failed to list processes: %v", err)
	}
	return util.PrintJSON(processes)
}
