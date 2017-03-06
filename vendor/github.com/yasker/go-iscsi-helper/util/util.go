package util

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	NSBinary = "nsenter"
)

var (
	cmdTimeout = time.Minute // one minute by default
)

func GetLocalIPs() ([]string, error) {
	results := []string{}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				results = append(results, ip.IP.String())
			}
		}
	}
	return results, nil
}

type NamespaceExecutor struct {
	ns string
}

func NewNamespaceExecutor(ns string) (*NamespaceExecutor, error) {
	ne := &NamespaceExecutor{
		ns: ns,
	}

	if ns == "" {
		return ne, nil
	}
	mntNS := filepath.Join(ns, "mnt")
	netNS := filepath.Join(ns, "net")
	if _, err := Execute(NSBinary, []string{"-V"}); err != nil {
		return nil, fmt.Errorf("Cannot find nsenter for namespace switching")
	}
	if _, err := Execute(NSBinary, []string{"--mount=" + mntNS, "mount"}); err != nil {
		return nil, fmt.Errorf("Invalid mount namespace %v, error %v", mntNS, err)
	}
	if _, err := Execute(NSBinary, []string{"--net=" + netNS, "ip", "addr"}); err != nil {
		return nil, fmt.Errorf("Invalid net namespace %v, error %v", netNS, err)
	}
	return ne, nil
}

func (ne *NamespaceExecutor) Execute(name string, args []string) (string, error) {
	if ne.ns == "" {
		return Execute(name, args)
	}
	cmdArgs := []string{
		"--mount=" + filepath.Join(ne.ns, "mnt"),
		"--net=" + filepath.Join(ne.ns, "net"),
		name,
	}
	cmdArgs = append(cmdArgs, args...)
	return Execute(NSBinary, cmdArgs)
}

func Execute(binary string, args []string) (string, error) {
	var output []byte
	var err error
	cmd := exec.Command(binary, args...)
	done := make(chan struct{})

	go func() {
		output, err = cmd.CombinedOutput()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(cmdTimeout):
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				log.Printf("Problem killing process pid=%v: %s", cmd.Process.Pid, err)
			}

		}
		return "", fmt.Errorf("Timeout executing: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	if err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %v, error %v", binary, args, string(output), err)
	}
	return string(output), nil
}
