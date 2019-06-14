package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func StartLauncherCmd() cli.Command {
	return cli.Command{
		Name: "start",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:8500",
			},
		},
		Action: func(c *cli.Context) {
			if err := startLauncher(c); err != nil {
				logrus.Fatalf("Error running start command: %v.", err)
			}
		},
	}
}

func startLauncher(c *cli.Context) error {
	listen := c.String("listen")

	l, err := NewEngineLauncher(listen)
	if err != nil {
		return err
	}

	if err := l.StartRPCServer(); err != nil {
		return err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Receive %v to exit", sig)
		l.Shutdown()
	}()

	return l.WaitForShutdown()
}
