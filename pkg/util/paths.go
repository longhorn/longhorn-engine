package util

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultLonghornDataPath = "/var/lib/longhorn"
	LonghornDataPathEnv     = "LONGHORN_DATA_PATH"

	hostMountPrefix = "/host"
)

var (
	longhornDataPath     = normalizeLonghornDataPath(os.Getenv(LonghornDataPathEnv))
	hostLonghornDataPath = filepath.Join(hostMountPrefix, strings.TrimPrefix(longhornDataPath, "/"))
	freezePointDirectory = filepath.Join(longhornDataPath, "freeze")
	fileLockDirectory    = filepath.Join(hostLonghornDataPath, ".lock")
	unixDomainSocketDir  = filepath.Join(hostLonghornDataPath, "unix-domain-socket")
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

func GetLonghornDataPath() string {
	return longhornDataPath
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
