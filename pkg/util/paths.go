package util

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultLonghornDataPath    = "/var/lib/longhorn"
	DefaultLonghornControlPath = "/var/lib/longhorn"
	LonghornDataPathEnv        = "LONGHORN_DATA_PATH"
	LonghornControlPathEnv     = "LONGHORN_CONTROL_PATH"

	hostMountPrefix = "/host"
)

var (
	longhornDataPath        = normalizeLonghornDataPath(os.Getenv(LonghornDataPathEnv))
	longhornControlPath     = normalizeLonghornControlPath(os.Getenv(LonghornControlPathEnv))
	hostLonghornDataPath    = filepath.Join(hostMountPrefix, strings.TrimPrefix(longhornDataPath, "/"))
	hostLonghornControlPath = filepath.Join(hostMountPrefix, strings.TrimPrefix(longhornControlPath, "/"))
	freezePointDirectory    = filepath.Join(longhornControlPath, "freeze")
	fileLockDirectory       = filepath.Join(hostLonghornControlPath, ".lock")
	unixDomainSocketDir     = filepath.Join(hostLonghornControlPath, "unix-domain-socket")
)

func normalizeLonghornDataPath(path string) string {
	if path == "" {
		path = DefaultLonghornDataPath
	}

	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return DefaultLonghornDataPath
	}

	return cleaned
}

func normalizeLonghornControlPath(path string) string {
	if path == "" {
		path = DefaultLonghornControlPath
	}

	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned == string(filepath.Separator) || cleaned == "/dev" || strings.HasPrefix(cleaned, "/dev/") {
		return DefaultLonghornControlPath
	}

	return cleaned
}

func GetLonghornDataPath() string {
	return longhornDataPath
}

func GetLonghornControlPath() string {
	return longhornControlPath
}

func GetHostLonghornDataPath() string {
	return hostLonghornDataPath
}

func GetFreezePointDirectory() string {
	return freezePointDirectory
}

func GetFileLockDirectory() string {
	return fileLockDirectory
}

func GetUnixDomainSocketDirectoryInContainer() string {
	return unixDomainSocketDir
}
