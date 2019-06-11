package util

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	NSBinary = "nsenter"
)

var (
	cmdTimeout = time.Minute // one minute by default
)

func getIPFromAddrs(addrs []net.Addr) string {
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				return strings.Split(ip.IP.String(), "/")[0]
			}
		}
	}
	return ""
}

func GetIPToHost() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	// TODO: This is a workaround, we want to get the interface IP connect
	// to the host, it's likely eth1 with one network attached to the host.
	for _, iface := range ifaces {
		if iface.Name == "eth1" {
			addrs, err := iface.Addrs()
			if err != nil {
				return "", err
			}
			ip := getIPFromAddrs(addrs)
			if ip != "" {
				return ip, nil
			}
		}
	}
	// And there is no eth1, so get the first real ip
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	ip := getIPFromAddrs(addrs)
	if ip != "" {
		return ip, nil
	}
	return "", fmt.Errorf("Cannot find IP connect to the host")
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
