package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/util"
)

var (
	hooks = []func() (err error){}
)

func addShutdown(f func() (err error)) {
	if len(hooks) == 0 {
		registerShutdown()
	}

	hooks = append(hooks, f)
	logrus.Debugf("Added shutdown func %v", util.GetFunctionName(f))
}

func registerShutdown() {
	c := make(chan os.Signal, 1024)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for s := range c {
			logrus.Warnf("Received signal %v to shutdown", s)
			exitCode := 0
			for _, hook := range hooks {
				logrus.Warnf("Starting to execute registered shutdown func %v", util.GetFunctionName(hook))
				if err := hook(); err != nil {
					logrus.WithError(err).Warnf("Failed to execute hook %v", util.GetFunctionName(hook))
					exitCode = 1
				}
			}
			os.Exit(exitCode)
		}
	}()
}
