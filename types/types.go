package types

import (
	"time"
)

const (
	GRPCServiceTimeout = 1 * time.Minute

	ProcessStateRunning  = "running"
	ProcessStateStarting = "starting"
	ProcessStateStopped  = "stopped"
	ProcessStateStopping = "stopping"
	ProcessStateError    = "error"
)

var (
	WaitInterval = time.Second
	WaitCount    = 60
)
