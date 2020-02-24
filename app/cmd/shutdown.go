package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/util"
)

var (
	hooks = []func(){}
)

func addShutdown(f func()) {
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
			for _, hook := range hooks {
				logrus.Warnf("Starting to execute registered shutdown func %v", util.GetFunctionName(hook))
				hook()
			}
			os.Exit(1)
		}
	}()
}
