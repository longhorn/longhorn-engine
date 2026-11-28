package util

import (
	"fmt"
	"net"
	"os"

	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	lhnet "github.com/longhorn/go-common-libs/net"
	lhtypes "github.com/longhorn/go-common-libs/types"
)

// hostInterfaceName is the interface that the host default IP discovery
// checks first, before falling back to the first global-unicast address.
const hostInterfaceName = "eth1"

func getIPFromAddrs(addrs []net.Addr) string {
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && ip.IP.IsGlobalUnicast() {
			return strings.Split(ip.IP.String(), "/")[0]
		}
	}
	return ""
}

// GetIPToHost returns the primary POD_IP, in canonical form, for the V1
// frontend. The host initiator cannot reach pod-local Multus storage networks.
// Standalone callers without POD_IP fall back to the host default IP discovery.
func GetIPToHost() (string, error) {
	return getIPToHost(os.Getenv(lhnet.EnvPodIP), getHostDefaultIP)
}

func getIPToHost(podIP string, getDefaultIP func() (string, error)) (string, error) {
	if podIP != "" {
		ip := net.ParseIP(podIP)
		if !lhnet.IsUsableIPForFamily(ip, lhnet.IPFamilyUnspecified) {
			return "", fmt.Errorf("invalid POD_IP %q", podIP)
		}
		// The portal becomes the session's persistent portal, which reload and
		// rescan match as text against iscsiadm output, so keep it canonical.
		return ip.String(), nil
	}

	logrus.Warnf("%s is not set; falling back to the host default IP discovery, which may not match the pod primary IP family", lhnet.EnvPodIP)
	return getDefaultIP()
}

func getHostDefaultIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	// TODO: This is a workaround, we want to get the interface IP connect
	// to the host, it's likely eth1 with one network attached to the host.
	for _, iface := range ifaces {
		if iface.Name == hostInterfaceName {
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
	return "", fmt.Errorf("cannot find IP connect to the host")
}

func RemoveFile(file string) error {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		// file doesn't exist
		return nil
	}

	if err := remove(file); err != nil {
		return errors.Wrapf(err, "failed to remove file %v", file)
	}

	return nil
}

func RemoveDevice(dev string) error {
	if _, err := os.Stat(dev); err == nil {
		if err := remove(dev); err != nil {
			return errors.Wrapf(err, "failed to removing device %s", dev)
		}
	}
	return nil
}

func DuplicateDevice(dev *lhtypes.BlockDeviceInfo, dest string) error {
	if err := mknod(dest, dev.Major, dev.Minor); err != nil {
		return errors.Wrapf(err, "cannot create device node %s for device %s", dest, dev.Name)
	}
	if err := os.Chmod(dest, 0660); err != nil {
		return errors.Wrapf(err, "cannot change permission of the device %s", dest)
	}
	// We use the group 6 by default because this is common group for disks
	// See more at https://github.com/longhorn/longhorn/issues/8088#issuecomment-1982300242
	if err := os.Chown(dest, 0, 6); err != nil {
		return errors.Wrapf(err, "cannot change ownership of the device %s", dest)
	}
	return nil
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0660
	fileMode |= unix.S_IFBLK
	dev := int(unix.Mkdev(uint32(major), uint32(minor)))

	logrus.Infof("Creating device %s %d:%d", device, major, minor)
	return unix.Mknod(device, uint32(fileMode), dev)
}

func removeAsync(path string, done chan<- error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.Errorf("Unable to remove: %v", path)
		done <- err
	}
	done <- nil
}

func remove(path string) error {
	done := make(chan error)
	go removeAsync(path, done)
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout trying to delete %s", path)
	}
}
