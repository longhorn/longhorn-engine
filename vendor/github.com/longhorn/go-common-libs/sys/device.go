package sys

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// FindBlockDeviceForMount returns the block device associated with a given mount path.
func FindBlockDeviceForMount(mountPath string) (string, error) {
	return findBlockDeviceForMountWithDeps(mountPath, "/proc/mounts", findBlockDeviceByMajorMinor)
}

// findBlockDeviceForMountWithDeps returns the block device associated with a given mount path
// by reading the specified mounts file.
//
// This function allows dependency injection for unit testing.
func findBlockDeviceForMountWithDeps(
	mountPath string,
	mountsFile string,
	resolveDevice func(string) (string, error),
) (string, error) {
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
			device := fields[0]

			// Skip pseudo-filesystems that don't have /dev/ prefix
			if !strings.HasPrefix(device, "/dev/") {
				return "", fmt.Errorf("mount path %s uses non-block device %q", mountPath, device)
			}

			// LVM device-mapper paths (e.g., /dev/mapper/... or /dev/dm-*) should be
			// returned as-is. In containerized environments, the underlying dm-*
			// backing devices may not be visible inside the container, so attempting
			// to resolve them to a physical block device can fail.
			if strings.HasPrefix(device, "/dev/mapper/") || strings.HasPrefix(device, "/dev/dm-") {
				return device, nil
			}

			// Resolve device using the injected resolveDevice function.
			// This handles special devices like /dev/root and symlinks consistently.
			actualDev, err := resolveDevice(device)
			if err != nil {
				return "", errors.Wrapf(err, "failed to resolve %q to actual device", device)
			}
			return actualDev, nil
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

// findBlockDeviceByMajorMinor finds the actual device in /sys/class/block that matches
// the given device's major:minor numbers. This is primarily used as a fallback for
// special device paths (such as /dev/root) that do not have a direct sysfs entry and
// therefore cannot be resolved via simple symlink or sysfs path lookups.
func findBlockDeviceByMajorMinor(devicePath string) (string, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(devicePath, &stat); err != nil {
		return "", errors.Wrapf(err, "failed to stat device %q", devicePath)
	}

	// Extract major and minor numbers using proper Linux device number encoding
	major := unix.Major(uint64(stat.Rdev))
	minor := unix.Minor(uint64(stat.Rdev))

	// Fast path: if the device basename exists in /sys/class/block, check its dev file first.
	// This avoids scanning all entries when the name already matches. This is only expected
	// to succeed for direct device paths like /dev/sda1; symlinks such as
	// /dev/disk/by-uuid/... have different basenames and will fall back to the full scan.
	baseName := filepath.Base(devicePath)
	devPath := filepath.Join("/sys/class/block", baseName, "dev")
	if contentBytes, err := os.ReadFile(devPath); err == nil {
		content := strings.TrimSpace(string(contentBytes))
		var entryMajor, entryMinor uint32
		if _, err := fmt.Sscanf(content, "%d:%d", &entryMajor, &entryMinor); err == nil {
			if entryMajor == major && entryMinor == minor {
				return "/dev/" + baseName, nil
			}
		}
		// If the basename exists but doesn't match, fall through to full scan.
	}

	// Search /sys/class/block for a device with matching major:minor
	entries, err := os.ReadDir("/sys/class/block")
	if err != nil {
		return "", errors.Wrap(err, "failed to read /sys/class/block")
	}

	for _, entry := range entries {
		devFile := filepath.Join("/sys/class/block", entry.Name(), "dev")
		contentBytes, err := os.ReadFile(devFile)
		if err != nil {
			continue // Skip if we can't read the dev file
		}
		content := strings.TrimSpace(string(contentBytes))

		var entryMajor, entryMinor uint32
		if _, err := fmt.Sscanf(content, "%d:%d", &entryMajor, &entryMinor); err != nil {
			continue
		}

		if entryMajor == major && entryMinor == minor {
			return "/dev/" + entry.Name(), nil
		}
	}

	return "", fmt.Errorf("no device found in /sys/class/block matching %q (major:%d, minor:%d)", devicePath, major, minor)
}

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
