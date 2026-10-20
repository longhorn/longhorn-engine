package sys

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// FindBlockDeviceForMount returns the block device associated with a given mount path.
func FindBlockDeviceForMount(mountPath string) (string, error) {
	return findBlockDeviceForMountWithDeps(mountPath, "/proc/mounts", findBlockDeviceByMajorMinor)
}

// maxStackedDeviceDepth bounds how many stacked device layers (e.g. LVM on top
// of dm-crypt, or a partitioned md array) FindBlockDeviceForPath follows.
const maxStackedDeviceDepth = 8

// Mocked in tests.
var (
	sysfsDir = "/sys"
	// The mount table of the calling thread rather than of the process, as
	// they differ once the thread has joined another mount namespace.
	mountInfoPath   = "/proc/thread-self/mountinfo"
	filesystemsPath = "/proc/filesystems"
	statBlockDev    = statBlockDeviceNumber
	statMountID     = getMountID
	readMounts      = readMountInfo
)

// mountInfoReadAttempts bounds how many times the mount table is read when the
// mount looked up is missing from it.
const mountInfoReadAttempts = 3

// statBlockDeviceNumber returns the device number of the block device file at
// path.
func statBlockDeviceNumber(path string) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return 0, err
	}
	// The types of Stat_t.Mode and Stat_t.Rdev differ between platforms.
	if uint32(st.Mode)&unix.S_IFMT != unix.S_IFBLK { // nolint: unconvert
		return 0, fmt.Errorf("%s is not a block device", path)
	}
	return uint64(st.Rdev), nil // nolint: unconvert
}

// mountInfo is an entry of /proc/<pid>/mountinfo.
type mountInfo struct {
	id         string
	devNum     string // "<major>:<minor>" of the mounted filesystem
	mountPoint string
	fsType     string
	source     string
}

// FindBlockDeviceForPath returns the block device (e.g. /dev/sdb1) backing the
// filesystem that contains the given path. Unlike FindBlockDeviceForMount, the
// path does not need to be a mount point: like findmnt --target, it is resolved
// through the mount containing it, so it can be any file or directory.
//
// The mount is resolved to a block device through its device number, or
// through its source for filesystems such as btrfs that use an anonymous
// device number but need a block device. Only the device the filesystem is
// mounted from is considered, so a btrfs filesystem spanning multiple devices
// is reported as being on that device.
//
// A stacked device (e.g. an LVM logical volume or dm-crypt device) is resolved
// to its underlying device as long as each layer has exactly one. A partition
// of a stacked device (e.g. /dev/md0p1) is resolved through the device it
// belongs to, while a partition of a plain disk (e.g. /dev/sda1) is returned
// as is. The path is looked up in the mount namespace of the calling thread.
func FindBlockDeviceForPath(path string) (string, error) {
	mount, err := findMountForPath(path)
	if err != nil {
		return "", err
	}

	name, err := mountBlockDeviceName(mount)
	if err != nil {
		return "", errors.Wrapf(err, "failed to find block device for %s", path)
	}
	if name == "" {
		return "", fmt.Errorf("%s is on %s filesystem %q mounted at %s, which is not backed by a block device",
			path, mount.fsType, mount.source, mount.mountPoint)
	}

	// Up to maxStackedDeviceDepth stacked layers plus the bottom device.
	for range maxStackedDeviceDepth + 1 {
		owner := name
		slaves, err := readBlockDeviceSlaves(owner)
		if err != nil {
			return "", err
		}
		if len(slaves) == 0 {
			// A partition has no slaves of its own; the underlying devices of
			// e.g. md0p1 are listed under its parent md0.
			parent, err := blockDevicePartitionParent(name)
			if err != nil {
				return "", err
			}
			if parent != "" {
				owner = parent
				if slaves, err = readBlockDeviceSlaves(owner); err != nil {
					return "", err
				}
			}
		}
		if len(slaves) == 0 {
			// sysfs encodes "/" in device names as "!", e.g. cciss!c0d0p1 for
			// /dev/cciss/c0d0p1.
			return "/dev/" + strings.ReplaceAll(name, "!", "/"), nil
		}
		if len(slaves) > 1 {
			return "", fmt.Errorf("%s is on device %s, which spans multiple devices %v", path, owner, slaves)
		}
		name = slaves[0]
	}
	return "", fmt.Errorf("%s is on a device stacked more than %d levels deep", path, maxStackedDeviceDepth)
}

// findMountForPath returns the mount containing the given path, identified by
// the mount ID the kernel reports for it. The kernel resolves the path itself,
// including relative paths, symlinks followed by "..", and procfs magic links
// such as /proc/self/fd/<fd> of an unlinked file.
//
// Mount points are not matched against the path, as the listing order of
// mountinfo does not tell which of the mounts on a path is visible, e.g.
// after a mount has been moved under another one.
func findMountForPath(path string) (mountInfo, error) {
	// Get the mount ID first, as resolving the path may trigger an automount
	// that must then be in the mount table.
	id, err := statMountID(path)
	if err != nil {
		return mountInfo{}, errors.Wrapf(err, "failed to get mount ID of %s", path)
	}

	// A mount table larger than a page is read in several chunks, and an
	// unmount in between can make the kernel skip an entry, so read it again
	// when the mount is missing.
	for range mountInfoReadAttempts {
		mounts, err := readMounts()
		if err != nil {
			return mountInfo{}, err
		}
		for _, mount := range mounts {
			if mount.id == id {
				return mount, nil
			}
		}
	}
	return mountInfo{}, fmt.Errorf("mount %s of %s not found in %s", id, path, mountInfoPath)
}

// readMountInfo returns the mounts listed in mountInfoPath, in order.
func readMountInfo() ([]mountInfo, error) {
	// Read the whole file rather than scanning it, as lines have no length
	// limit, e.g. with long overlayfs lowerdir options.
	content, err := os.ReadFile(mountInfoPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read %s", mountInfoPath)
	}

	var mounts []mountInfo
	for line := range strings.Lines(string(content)) {
		mount, err := parseMountInfoLine(strings.TrimSuffix(line, "\n"))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s", mountInfoPath)
		}
		mounts = append(mounts, mount)
	}
	return mounts, nil
}

// parseMountInfoLine parses a line of /proc/<pid>/mountinfo, e.g.
//
//	36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
//
// where the optional fields before "-" may be absent.
func parseMountInfoLine(line string) (mountInfo, error) {
	// Fields are separated by single ASCII spaces. Spaces, tabs, newlines and
	// backslashes in them are escaped, but other whitespace is not.
	fields := strings.Split(line, " ")
	if len(fields) < 10 {
		return mountInfo{}, fmt.Errorf("invalid mountinfo line %q", line)
	}
	sep := slices.Index(fields[6:], "-")
	if sep < 0 || 6+sep+2 >= len(fields) {
		return mountInfo{}, fmt.Errorf("invalid mountinfo line %q", line)
	}
	sep += 6
	return mountInfo{
		id:         fields[0],
		devNum:     fields[2],
		mountPoint: unescapeMountInfoField(fields[4]),
		fsType:     fields[sep+1],
		source:     unescapeMountInfoField(fields[sep+2]),
	}, nil
}

// unescapeMountInfoField decodes the octal escapes (e.g. \040 for a space)
// used in mountinfo fields.
func unescapeMountInfoField(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if c, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(c))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// mountBlockDeviceName returns the sysfs name of the block device a mount is
// on, or an empty string if it is not on one.
func mountBlockDeviceName(mount mountInfo) (string, error) {
	// For most filesystems, the device number is that of their block device.
	name, err := blockDeviceNameByNumber(mount.devNum)
	if err != nil || name != "" {
		return name, err
	}

	// Others, such as btrfs, use an anonymous device number, so fall back to
	// the mount source, but only for filesystems that need a block device.
	// For the others (nodev in /proc/filesystems), such as tmpfs, overlayfs
	// or ZFS, the source is just a label, e.g. "tmpfs" or "tank/data".
	if !filepath.IsAbs(mount.source) {
		return "", nil
	}
	nodev, err := isNodevFilesystem(mount.fsType)
	if err != nil {
		return "", err
	}
	if nodev {
		return "", nil
	}
	dev, err := statBlockDev(mount.source)
	if err != nil {
		// The source may be missing from /dev, e.g. in an unprivileged
		// container, so look it up in sysfs by its kernel name instead.
		if os.IsNotExist(err) {
			if name := sysfsBlockDeviceName(mount.source); name != "" {
				return name, nil
			}
		}
		return "", errors.Wrapf(err, "failed to stat mount source %s", mount.source)
	}
	devNum := fmt.Sprintf("%d:%d", unix.Major(dev), unix.Minor(dev))
	name, err = blockDeviceNameByNumber(devNum)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", fmt.Errorf("block device %s (%s) not found in sysfs", mount.source, devNum)
	}
	return name, nil
}

// isNodevFilesystem returns whether the given filesystem type does not need a
// block device, according to /proc/filesystems. Unknown types are assumed not
// to need one.
func isNodevFilesystem(fsType string) (bool, error) {
	// FUSE filesystems are listed with a subtype, e.g. fuse.sshfs.
	fsType, _, _ = strings.Cut(fsType, ".")

	content, err := os.ReadFile(filesystemsPath)
	if err != nil {
		return false, errors.Wrapf(err, "failed to read %s", filesystemsPath)
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		// Each line is "nodev\t<type>" or "\t<type>".
		flag, name, ok := strings.Cut(line, "\t")
		if ok && name == fsType {
			return flag == "nodev", nil
		}
	}
	return true, nil
}

// sysfsBlockDeviceName returns the sysfs name of the block device at the given
// /dev path (e.g. cciss!c0d0p1 for /dev/cciss/c0d0p1) if sysfs has a block
// device by that name, or an empty string otherwise.
func sysfsBlockDeviceName(devPath string) string {
	rel, ok := strings.CutPrefix(devPath, "/dev/")
	if !ok || rel == "" {
		return ""
	}
	name := strings.ReplaceAll(rel, "/", "!")
	if name == "." || name == ".." {
		return ""
	}
	if _, err := os.Stat(filepath.Join(sysfsDir, "class", "block", name)); err != nil {
		return ""
	}
	return name
}

// blockDeviceNameByNumber returns the sysfs name of the block device with the
// given "<major>:<minor>" number, or an empty string if there is none.
func blockDeviceNameByNumber(devNum string) (string, error) {
	devLink := filepath.Join(sysfsDir, "dev", "block", devNum)
	target, err := os.Readlink(devLink)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", errors.Wrapf(err, "failed to read %s", devLink)
	}
	return filepath.Base(target), nil
}

// readBlockDeviceSlaves returns the names of the devices underlying the given
// block device, if any.
func readBlockDeviceSlaves(name string) ([]string, error) {
	slavesDir := filepath.Join(sysfsDir, "class", "block", name, "slaves")
	entries, err := os.ReadDir(slavesDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "failed to read %s", slavesDir)
	}
	slaves := make([]string, 0, len(entries))
	for _, entry := range entries {
		slaves = append(slaves, entry.Name())
	}
	return slaves, nil
}

// blockDevicePartitionParent returns the name of the device containing the
// given partition, or an empty string if the device is not a partition.
func blockDevicePartitionParent(name string) (string, error) {
	devDir := filepath.Join(sysfsDir, "class", "block", name)
	if _, err := os.Stat(filepath.Join(devDir, "partition")); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", errors.Wrapf(err, "failed to check whether %s is a partition", name)
	}
	// A partition's sysfs directory is nested in its parent's, e.g.
	// /sys/devices/virtual/block/md0/md0p1.
	realDir, err := filepath.EvalSymlinks(devDir)
	if err != nil {
		return "", errors.Wrapf(err, "failed to resolve %s", devDir)
	}
	return filepath.Base(filepath.Dir(realDir)), nil
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
