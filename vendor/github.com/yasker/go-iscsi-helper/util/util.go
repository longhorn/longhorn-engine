package util

import (
	"fmt"
	"net"
	"path/filepath"

	cutil "github.com/rancher/convoy/util"
)

const (
	NSBinary = "nsenter"
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
	if _, err := cutil.Execute(NSBinary, []string{"-V"}); err != nil {
		return nil, fmt.Errorf("Cannot find nsenter for namespace switching")
	}
	if _, err := cutil.Execute(NSBinary, []string{"--mount=" + mntNS, "mount"}); err != nil {
		return nil, fmt.Errorf("Invalid mount namespace %v, error %v", mntNS, err)
	}
	if _, err := cutil.Execute(NSBinary, []string{"--net=" + netNS, "ip", "addr"}); err != nil {
		return nil, fmt.Errorf("Invalid net namespace %v, error %v", netNS, err)
	}
	return ne, nil
}

func (ne *NamespaceExecutor) Execute(name string, args []string) (string, error) {
	if ne.ns == "" {
		return cutil.Execute(name, args)
	}
	cmdArgs := []string{
		"--mount=" + filepath.Join(ne.ns, "mnt"),
		"--net=" + filepath.Join(ne.ns, "net"),
		name,
	}
	cmdArgs = append(cmdArgs, args...)
	return cutil.Execute(NSBinary, cmdArgs)
}
