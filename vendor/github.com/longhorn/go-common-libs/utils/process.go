package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/c9s/goprocinfo/linux"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-common-libs/types"
)

// ProcessFinder is a struct to find process information.
type ProcessFinder struct {
	procDirectory string // The directory path where the process information is stored.
}

func NewProcFinder(procDir string) *ProcessFinder {
	return &ProcessFinder{procDir}
}

// GetProcessStatus returns the process status for the given process.
func (p *ProcessFinder) GetProcessStatus(proc string) (*linux.ProcessStatus, error) {
	path := filepath.Join(p.procDirectory, proc, "status")
	return linux.ReadProcessStatus(path)
}

// FindAncestorByName returns the ancestor process status for the given process.
func (p *ProcessFinder) FindAncestorByName(ancestorProcess, pid string) (*linux.ProcessStatus, error) {
	ps, err := p.GetProcessStatus(pid)
	if err != nil {
		return nil, err
	}

	for {
		if ps.PPid == 0 {
			return ps, nil
		}
		ps, err = p.GetProcessStatus(fmt.Sprint(ps.PPid))
		if err != nil {
			return nil, err
		}
	}
}

// GetProcessPIDs returns the PIDs for the given process name.
func GetProcessPIDs(processName, procDir string) ([]uint64, error) {
	files, err := os.ReadDir(procDir)
	if err != nil {
		return nil, err
	}

	processFinder := NewProcFinder(procDir)

	var pids []uint64
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		if _, err := strconv.Atoi(file.Name()); err != nil {
			// Not a numerical values representing a running process
			continue
		}

		pid, err := strconv.ParseUint(file.Name(), 10, 64)
		if err != nil {
			// Not a numerical values representing a running process
			continue
		}

		processStatus, err := processFinder.GetProcessStatus(fmt.Sprint(pid))
		if err != nil {
			logrus.WithError(err).Debugf("Failed to get PID (%v) status", pid)
			continue
		}

		if processStatus.Name == processName {
			pids = append(pids, pid)
		}
	}

	// If no process is found, return the host namespace PID
	if len(pids) == 0 {
		pids = append(pids, GetHostNamespacePID(types.HostProcDirectory))
	}

	return pids, nil
}

// GetHostNamespacePID returns the PID of the host namespace.
func GetHostNamespacePID(hostProcDir string) uint64 {
	pf := NewProcFinder(hostProcDir)
	processes := []string{
		types.ProcessDockerd,
		types.ProcessContainerd,
		types.ProcessContainerdShim,
	}
	for _, process := range processes {
		proc, err := pf.FindAncestorByName(process, types.ProcessSelf)
		if err != nil {
			continue
		}
		return proc.Pid

	}
	// fall back to use pid 1
	return 1
}

// GetNamespaceDirectory returns the namespace directory for the given PID.
func GetNamespaceDirectory(procDir, pid string) string {
	return filepath.Join(procDir, pid, "ns")
}

// GetHostNamespaceDirectory returns the namespace directory for the host namespace.
func GetHostNamespaceDirectory(hostProcDir string) string {
	return GetNamespaceDirectory(hostProcDir, fmt.Sprint(GetHostNamespacePID(hostProcDir)))
}

// GetProcessAncestorNamespaceDirectory returns the namespace directory for the
// ancestor of the given process.
func GetProcessAncestorNamespaceDirectory(process, procDir string) (string, error) {
	pf := NewProcFinder(procDir)
	proc, err := pf.FindAncestorByName(process, types.ProcessSelf)
	if err != nil {
		return "", fmt.Errorf("failed to get ancestor namespace of %v", process)
	}
	return GetNamespaceDirectory(procDir, fmt.Sprint(proc.Pid)), nil
}

// GetProcessNamespaceDirectory returns the namespace directory for the given process.
// If processName is ProcessNone, it returns the host namespace directory.
func GetProcessNamespaceDirectory(processName, procDir string) (string, error) {
	if processName == types.ProcessNone {
		return GetHostNamespaceDirectory(procDir), nil
	}

	pids, err := GetProcessPIDs(processName, procDir)
	if err != nil {
		return "", err
	}

	return GetNamespaceDirectory(procDir, fmt.Sprint(pids[0])), nil
}
