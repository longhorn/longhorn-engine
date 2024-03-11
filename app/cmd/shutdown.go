package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	lhutils "github.com/longhorn/go-common-libs/utils"
)

var (
	hooks = []func() (err error){}
)

func addShutdown(f func() (err error)) {
	if len(hooks) == 0 {
		registerShutdown()
	}

	hooks = append(hooks, f)
	logrus.Debugf("Added shutdown func %v", lhutils.GetFunctionPath(f))
}

func registerShutdown() {
	c := make(chan os.Signal, 1024)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for s := range c {
			logrus.Warnf("Received signal %v to shutdown", s)
			exitCode := 0
			for _, hook := range hooks {
				logrus.Warnf("Starting to execute registered shutdown func %v", lhutils.GetFunctionPath(hook))
				if err := hook(); err != nil {
					logrus.WithError(err).Warnf("Failed to execute hook %v", lhutils.GetFunctionPath(hook))
					exitCode = 1
				}
			}
			os.Exit(exitCode)
		}
	}()
}
