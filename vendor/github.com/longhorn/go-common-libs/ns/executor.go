package ns

import (
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/cockroachdb/errors"

	log "github.com/sirupsen/logrus"

	"github.com/longhorn/go-common-libs/exec"
	"github.com/longhorn/go-common-libs/proc"
	"github.com/longhorn/go-common-libs/types"
)

const (
	maxNsDirRefreshRetries    = 10
	nsDirRefreshRetryInterval = 1 * time.Second
)

// Executor is a struct responsible for executing commands in a specific
// namespace using nsenter.
type Executor struct {
	mu sync.RWMutex

	namespaces  []types.Namespace // The namespaces to enter.
	nsDirectory string            // The directory of the namespace.
	processName string            // The name of the associated process.
	processDir  string            // The process directory.

	executor exec.ExecuteInterface // An interface for executing commands. This allows mocking for unit tests.
}

// NewNamespaceExecutor creates a new namespace executor for the given process name,
// namespaces and proc directory. If the process name is not empty, it will try to
// use the process namespace directory. Otherwise, it will use the host namespace
// directory. The namespaces are the namespaces to enter. The proc directory is
// the directory where the process information is stored. It will also verify the
// existence of the nsenter binary.
func NewNamespaceExecutor(processName, procDirectory string, namespaces []types.Namespace) (*Executor, error) {
	nsDir, err := proc.GetProcessNamespaceDirectory(processName, procDirectory)
	if err != nil {
		return nil, err
	}

	NamespaceExecutor := &Executor{
		namespaces:  namespaces,
		nsDirectory: nsDir,
		processName: processName,
		processDir:  procDirectory,
		executor:    exec.NewExecutor(),
	}

	if _, err := NamespaceExecutor.executor.Execute(nil, types.NsBinary, []string{"-V"}, types.ExecuteDefaultTimeout); err != nil {
		return nil, errors.Wrap(err, "cannot find nsenter for namespace switching")
	}

	return NamespaceExecutor, nil
}

// refresh updates nsDirectory in case the corresponding process was restarted and the cached nsDirectory is no longer valid.
// It returns an error if the process is not found or the namespace directory cannot be determined.
func (nsexec *Executor) refresh() error {
	nsDir, err := proc.GetProcessNamespaceDirectory(nsexec.processName, nsexec.processDir)
	if err != nil {
		return err
	}
	nsexec.nsDirectory = nsDir
	return nil
}

// prepareCommandArgs prepares the nsenter command arguments, and the environment variables are not ignored.
func (nsexec *Executor) prepareCommandArgs(binary string, args, envs []string) []string {
	cmdArgs := []string{}
	for _, ns := range nsexec.namespaces {
		nsPath := filepath.Join(nsexec.nsDirectory, ns.String())
		switch ns {
		case types.NamespaceIpc:
			cmdArgs = append(cmdArgs, "--ipc="+nsPath)
		case types.NamespaceMnt:
			cmdArgs = append(cmdArgs, "--mount="+nsPath)
		case types.NamespaceNet:
			cmdArgs = append(cmdArgs, "--net="+nsPath)
		}
	}
	if len(envs) > 0 {
		cmdArgs = append(cmdArgs, "env")
		cmdArgs = append(cmdArgs, envs...)
	}

	cmdArgs = append(cmdArgs, binary)
	return append(cmdArgs, args...)
}

// Execute executes the command in the namespace. If NsDirectory is empty,
// it will execute the command in the current namespace.
func (nsexec *Executor) Execute(envs []string, binary string, args []string, timeout time.Duration) (string, error) {
	return nsexec.executeWithRetry(func() (string, error) {
		return nsexec.executor.Execute(nil, types.NsBinary, nsexec.prepareCommandArgs(binary, args, envs), timeout)
	})
}

// ExecuteWithStdin executes the command in the namespace with stdin.
// If NsDirectory is empty, it will execute the command in the current namespace.
func (nsexec *Executor) ExecuteWithStdin(envs []string, binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return nsexec.executeWithRetry(func() (string, error) {
		return nsexec.executor.ExecuteWithStdin(types.NsBinary, nsexec.prepareCommandArgs(binary, args, envs), stdinString, timeout)
	})
}

// ExecuteWithStdinPipe executes the command in the namespace with stdin pipe.
// If NsDirectory is empty, it will execute the command in the current namespace.
func (nsexec *Executor) ExecuteWithStdinPipe(envs []string, binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return nsexec.executeWithRetry(func() (string, error) {
		return nsexec.executor.ExecuteWithStdinPipe(types.NsBinary, nsexec.prepareCommandArgs(binary, args, envs), stdinString, timeout)
	})
}

// staleNsDirPattern matches nsenter errors when the namespace directory no
// longer exists, e.g.:
//
//	nsenter: cannot open /proc/12345/ns/mnt: No such file or directory
var staleNsDirPattern = regexp.MustCompile(`nsenter: cannot open .+/ns/.+: No such file or directory`)

// isNsDirStaleError checks if the error indicates that the cached namespace
// directory is stale (the process was restarted and its PID changed).
func (nsexec *Executor) isNsDirStaleError(err error) bool {
	if err == nil {
		return false
	}
	return staleNsDirPattern.MatchString(err.Error())
}

// executeWithRetry wraps an execution function with retry logic that detects
// stale namespace directories and refreshes them. When the cached process PID
// becomes stale (e.g., iscsid restarted), nsenter fails with "No such file or
// directory". This method retries after refreshing the namespace directory.
func (nsexec *Executor) executeWithRetry(execFn func() (string, error)) (output string, err error) {
	retryErr := retry.Do(
		func() error {
			nsexec.mu.RLock()
			output, err = execFn()
			nsexec.mu.RUnlock()

			return err
		},
		retry.Attempts(maxNsDirRefreshRetries),
		retry.Delay(nsDirRefreshRetryInterval),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.RetryIf(func(err error) bool {
			return nsexec.isNsDirStaleError(err)
		}),
		retry.OnRetry(func(n uint, err error) {
			nsexec.mu.Lock()
			defer nsexec.mu.Unlock()

			log.WithError(err).Warnf(
				"Detected stale namespace directory, refreshing: process=%s procDir=%s cachedNsDir=%s",
				nsexec.processName, nsexec.processDir, nsexec.nsDirectory,
			)

			if refreshErr := nsexec.refresh(); refreshErr != nil {
				log.WithError(refreshErr).Warnf(
					"Failed to refresh namespace directory: process=%s procDir=%s",
					nsexec.processName, nsexec.processDir,
				)
			}
		}),
	)
	if retryErr != nil {
		if err != nil {
			return output, errors.Wrapf(err, "failed after %d attempts to refresh stale namespace directory", maxNsDirRefreshRetries)
		}
		return output, retryErr
	}
	return output, nil
}
