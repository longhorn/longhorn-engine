package types

import "time"

const (
	GRPCServiceTimeout = 1 * time.Minute

	ProcessStateRunning = "running"
	ProcessStateStopped = "stopped"
	ProcessStateError   = "error"
)
