package sys

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// FindBlockDeviceForMount returns the block device associated with a given mount path.
func FindBlockDeviceForMount(mountPath string) (string, error) {
	return findBlockDeviceForMountWithFile(mountPath, "/proc/mounts")
}

// findBlockDeviceForMountWithFile returns the block device associated with a given mount path
// by reading the specified mounts file.
//
// This function allows dependency injection for unit testing.
func findBlockDeviceForMountWithFile(mountPath, mountsFile string) (string, error) {
	f, err := os.Open(mountsFile)
	if err != nil {
		return "", errors.Wrapf(err, "failed to open %s", mountsFile)
	}
	defer f.Close() // nolint: errcheck // we don't care about errors here, we just want to close the file.

	mountPath = filepath.Clean(mountPath)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[1] == mountPath {
			return fields[0], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", errors.Wrapf(err, "error reading %s", mountsFile)
	}
	return "", fmt.Errorf("mount path %s not found", mountPath)
}

// ResolveBlockDeviceToPhysicalDevice returns the physical block device
// (e.g., /dev/nvme0 for NVMe, /dev/sda for SATA) corresponding to the given
// block device (e.g., /dev/nvme0n1p2, /dev/nvme0n1).
func ResolveBlockDeviceToPhysicalDevice(
	blockDevice string,
) (string, error) {
	return resolveBlockDeviceToPhysicalDeviceWithDeps(
		blockDevice,
		filepath.EvalSymlinks,
	)
}

// deviceRegexp matches "block/<device>" or "nvme/<device>".
// The second capturing group ([^/]+) isolates the device name.
//
// Note: This regex may need to be updated if additional device types are
// required.
var deviceRegexp = regexp.MustCompile(`(block|nvme)/([^/]+)`)

// resolveBlockDeviceToPhysicalDeviceWithDeps resolves a block device path
// (e.g., /dev/nvme0n1p3 or /dev/sda2) to its top-level physical device
// (e.g., /dev/nvme0 or /dev/sda).
//
// This function allows dependency injection for unit testing.
func resolveBlockDeviceToPhysicalDeviceWithDeps(
	blockDevice string,
	evalSymlinks func(string) (string, error),
) (string, error) {
	// Resolve the device symlink (e.g., /dev/... -> actual device).
	realDev, err := evalSymlinks(blockDevice)
	if err != nil {
		return "", errors.Wrapf(err, "failed to resolve symlink for %q", blockDevice)
	}

	// Construct the sysfs path for the device.
	devName := filepath.Base(realDev)
	sysPath := filepath.Join("/sys/class/block", devName)

	// Resolve the sysfs path to its full canonical path.
	// (e.g., /sys/devices/.../nvme/nvme0/nvme0n1/nvme0n1p3).
	realSysPath, err := evalSymlinks(sysPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to resolve sysfs path for %q", sysPath)
	}

	// Extract all device layers from the sysfs path.
	matches := deviceRegexp.FindAllStringSubmatch(realSysPath, -1)
	if len(matches) == 0 {
		return "", fmt.Errorf("failed to parse device layers for %q (resolved %s)", blockDevice, realDev)
	}

	// Extract the physical device name from the sysfs path (e.g., 'nvme0' or 'sda').
	lastMatch := matches[len(matches)-1]
	topDevice := lastMatch[2]
	if topDevice == "" {
		return "", fmt.Errorf("failed to identify top-level device for %q (resolved %s)", blockDevice, realDev)
	}

	return "/dev/" + topDevice, nil
}

// ResolveMountPathToPhysicalDevice returns the physical block device (e.g., /dev/nvme0)
// for the given mount path (e.g., /var/lib/longhorn)
func ResolveMountPathToPhysicalDevice(mountPath string) (string, error) {
	return resolveMountPathToPhysicalDeviceWithDeps(
		mountPath,
		"/proc/mounts",
		filepath.EvalSymlinks,
	)
}

// resolveMountPathToPhysicalDeviceWithDeps returns the top-level physical device
// (e.g., /dev/nvme0 for NVMe, /dev/sda for SATA) corresponding to the given
// mount path (e.g., /var/lib/longhorn).
//
// This function allows dependency injection for unit testing.
func resolveMountPathToPhysicalDeviceWithDeps(
	mountPath string,
	mountsFile string,
	evalSymlinks func(string) (string, error),
) (string, error) {
	blockDevice, err := findBlockDeviceForMountWithFile(mountPath, mountsFile)
	if err != nil {
		return "", err
	}

	return resolveBlockDeviceToPhysicalDeviceWithDeps(blockDevice, evalSymlinks)
}
