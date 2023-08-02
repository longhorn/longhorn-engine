package types

import ()

const OsReleaseFilePath = "/etc/os-release"
const SysClassBlockDirectory = "/sys/class/block/"

const OSDistroTalosLinux = "talos"

// CachedOSDistro is a global variable that caches the OS distro.
var CachedOSDistro = ""

// BlockDeviceInfo is a struct that contains the block device info.
type BlockDeviceInfo struct {
	Name  string // Name of the block device (e.g. sda, sdb, sdc, etc.).
	Major int    // Major number of the block device.
	Minor int    // Minor number of the block device.
}
