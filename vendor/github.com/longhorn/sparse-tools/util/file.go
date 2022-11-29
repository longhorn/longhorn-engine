package util

import (
	"os"
	"syscall"
	"time"
)

func GetFileChangeTime(fileName string) (string, error) {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return "", err
	}

	stat := fileInfo.Sys().(*syscall.Stat_t)
	return time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)).String(), nil
}
