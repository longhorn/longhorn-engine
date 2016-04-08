package main

import (
	"log"
	"os"
	"runtime/pprof"

	"github.com/codegangsta/cli"
	"github.com/rancher/longhorn/app"
)

func main() {
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
	a.Commands = []cli.Command{
		app.ControllerCmd(),
		app.ReplicaCmd(),
	}

	a.Run(os.Args)
}
