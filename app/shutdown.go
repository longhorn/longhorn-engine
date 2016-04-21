package app

import (
	"os"
	"os/signal"
	"syscall"
)

var (
	hooks = []func(){}
)

func addShutdown(f func()) {
	if len(hooks) == 0 {
		registerShutdown()
	}

	hooks = append(hooks, f)
}

func registerShutdown() {
	c := make(chan os.Signal, 1024)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range c {
			for _, hook := range hooks {
				hook()
			}
			os.Exit(1)
		}
	}()
}
