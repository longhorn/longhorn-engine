package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"github.com/moby/sys/reexec"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/sparse-tools/cli/ssync"

	"github.com/longhorn/longhorn-engine/app/cmd"
	"github.com/longhorn/longhorn-engine/pkg/meta"
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

func cmdNotFound(ctx context.Context, c *cli.Command, command string) {
	panic(fmt.Errorf("unrecognized command: %s", command))
}

func onUsageError(ctx context.Context, c *cli.Command, err error, isSubcommand bool) error {
	panic(fmt.Errorf("usage error, please check your command"))
}

func longhornCli() {
	pprofFile := os.Getenv("PPROFILE")
	if pprofFile != "" {
		f, err := os.Create(pprofFile)
		if err != nil {
			log.Fatal(err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			logrus.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	meta.Version = Version
	meta.GitCommit = GitCommit
	meta.BuildDate = BuildDate

	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (function string, file string) {
			fileName := fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
			funcName := path.Base(f.Function)
			return funcName, fileName
		},
		TimestampFormat: time.RFC3339Nano,
		FullTimestamp:   true,
	})

	a := &cli.Command{
		Version: Version,
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			if c.Bool("debug") {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return ctx, nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "url",
				Value: "http://localhost:9501",
			},
			&cli.StringFlag{
				Name:     "volume-name",
				Required: false,
				Usage:    "Name of the volume (for validation purposes)",
			},
			&cli.StringFlag{
				Name:     "engine-instance-name",
				Required: false,
				Usage:    "Name of the engine instance (for validation purposes)",
			},
			&cli.BoolFlag{
				Name: "debug",
			},
		},
		Commands: []*cli.Command{
			cmd.ControllerCmd(),
			cmd.ReplicaCmd(),
			cmd.SyncAgentCmd(),
			cmd.SyncAgentServerResetCmd(),
			cmd.StartWithReplicasCmd(),
			cmd.AddReplicaCmd(),
			cmd.VerifyRebuildReplicaCmd(),
			cmd.LsReplicaCmd(),
			cmd.RmReplicaCmd(),
			cmd.UpdateReplicaCmd(),
			cmd.RebuildStatusCmd(),
			cmd.SnapshotCmd(),
			cmd.SnapshotHashCmd(),
			cmd.SnapshotHashCancelCmd(),
			cmd.SnapshotHashStatusCmd(),
			cmd.BackupCmd(),
			cmd.ExpandCmd(),
			cmd.UnmapMarkSnapChainRemovedCmd(),
			cmd.Journal(),
			cmd.InfoCmd(),
			cmd.FrontendCmd(),
			cmd.SystemBackupCmd(),
			cmd.ProfilerCmd(),
			VersionCmd(),
		},
		CommandNotFound: cmdNotFound,
		OnUsageError:    onUsageError,
	}

	_ = a.Walk(func(command *cli.Command) error {
		// Keep v1 semantics: slice flag values (e.g. --label k=a,b) must not be split on commas.
		command.DisableSliceFlagSeparator = true
		if len(command.Commands) > 0 {
			command.CommandNotFound = cmdNotFound
		}
		return nil
	})

	if err := a.Run(context.Background(), os.Args); err != nil {
		logrus.WithError(err).Fatal("Error when executing command")
	}
}
