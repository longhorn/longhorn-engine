package replica

type ProgressState string

const (
	ProgressStateInProgress = ProgressState("in_progress")
	ProgressStateComplete   = ProgressState("complete")
	ProgressStateError      = ProgressState("error")
)
