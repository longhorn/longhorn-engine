package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

	"github.com/docker/docker/pkg/reexec"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/sparse-tools/cli/sfold"
	"github.com/longhorn/sparse-tools/cli/ssync"

	"github.com/longhorn/longhorn-engine/app"
	"github.com/longhorn/longhorn-engine/meta"
)

// following variables will be filled by `-ldflags "-X ..."`
var (
	Version   string
	GitCommit string
	BuildDate string
)

func main() {
	defer cleanup()
	reexec.Register("ssync", ssync.Main)
	reexec.Register("sfold", sfold.Main)

	if !reexec.Init() {
		longhornCli()
	}
}

// ResponseLogAndError would log the error before call ResponseError()
func ResponseLogAndError(v interface{}) {
	if e, ok := v.(*logrus.Entry); ok {
		logrus.Errorln(e.Message)
		fmt.Println(e.Message)
	} else {
		e, isErr := v.(error)
		_, isRuntimeErr := e.(runtime.Error)
		if isErr && !isRuntimeErr {
			logrus.Errorln(fmt.Sprint(e))
			fmt.Println(fmt.Sprint(e))
		} else {
			logrus.Errorln("Caught FATAL error: ", v)
			debug.PrintStack()
			fmt.Println("Caught FATAL error: ", v)
		}
	}
}

func cleanup() {
	if r := recover(); r != nil {
		ResponseLogAndError(r)
		os.Exit(1)
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

	a.Version = Version
	meta.Version = Version
	meta.GitCommit = GitCommit
	meta.BuildDate = BuildDate

	a.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
	a.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "url",
		},
		cli.BoolFlag{
			Name: "debug",
		},
	}
	a.Commands = []cli.Command{
		app.ControllerCmd(),
		app.ReplicaCmd(),
		app.SyncAgentCmd(),
		app.SyncAgentServerResetCmd(),
		app.AddReplicaCmd(),
		app.LsReplicaCmd(),
		app.RmReplicaCmd(),
		app.SnapshotCmd(),
		app.BackupCmd(),
		app.Journal(),
		app.InfoCmd(),
		VersionCmd(),
	}
	a.CommandNotFound = cmdNotFound
	a.OnUsageError = onUsageError

	if err := a.Run(os.Args); err != nil {
		logrus.Fatal("Error when executing command: ", err)
	}
}
