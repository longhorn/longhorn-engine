package replica

type ProgressState string

const (
	backupBlockSize = 2 << 20 // 2MiB

	ProgressStateInProgress = ProgressState("in_progress")
	ProgressStateComplete   = ProgressState("complete")
	ProgressStateError      = ProgressState("error")
)
