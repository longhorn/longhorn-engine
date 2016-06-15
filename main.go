package main

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/docker/docker/pkg/reexec"
	"github.com/rancher/longhorn/app"
	"github.com/rancher/longhorn/backup"
	"github.com/rancher/sparse-tools/cli/sfold"
	"github.com/rancher/sparse-tools/cli/ssync"
)

func main() {
	reexec.Register("ssync", ssync.Main)
	reexec.Register("sfold", sfold.Main)
	reexec.Register("sbackup", backup.Main)

	if !reexec.Init() {
		longhornCli()
	}
}

func cmdNotFound(c *cli.Context, command string) {
	panic(fmt.Errorf("Unrecognized command: %s", command))
}

func onUsageError(c *cli.Context, err error, isSubcommand bool) error {
	panic(fmt.Errorf("Usage error, please check your command"))
}

func longhornCli() {
	pprofFile := os.Getenv("PPROFILE")
	if pprofFile != "" {
		f, err := os.Create(pprofFile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	a := cli.NewApp()
	a.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
	a.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "url",
			Value: "http://localhost:9501",
		},
		cli.BoolFlag{
			Name: "debug",
		},
	}
	a.Commands = []cli.Command{
		app.ControllerCmd(),
		app.ReplicaCmd(),
		app.SyncAgentCmd(),
		app.AddReplicaCmd(),
		app.LsReplicaCmd(),
		app.RmReplicaCmd(),
		app.SnapshotCmd(),
		app.BackupCmd(),
		app.Journal(),
	}
	a.CommandNotFound = cmdNotFound
	a.OnUsageError = onUsageError

	if err := a.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}
