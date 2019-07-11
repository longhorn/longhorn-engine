package types

import "time"

const (
	GRPCServiceTimeout = 1 * time.Minute

	WaitInterval = time.Second
	WaitCount    = 60

	ProcessStateRunning  = "running"
	ProcessStateStarting = "starting"
	ProcessStateStopped  = "stopped"
	ProcessStateStopping = "stopping"
	ProcessStateError    = "error"
)
