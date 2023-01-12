package util

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/types"
)

const (
	tmpFileSuffix = ".tmp"
)

func SetSnapshotHashInfoToChecksumFile(checksumFileName string, info *types.SnapshotHashInfo) error {
	return encodeToFile(info, checksumFileName)
}

func GetSnapshotHashInfoFromChecksumFile(checksumFileName string) (*types.SnapshotHashInfo, error) {
	f, err := os.Open(checksumFileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var info types.SnapshotHashInfo

	if err := json.NewDecoder(f).Decode(&info); err != nil {
		return nil, err
	}

	return &info, nil
}

func encodeToFile(obj interface{}, path string) (err error) {
	tmpPath := fmt.Sprintf("%s.%s", path, tmpFileSuffix)

	defer func() {
		if err != nil {
			if _, err := os.Stat(tmpPath); err == nil {
				if err := os.Remove(tmpPath); err != nil {
					logrus.WithError(err).Warnf("Failed to remove %v", tmpPath)
				}
			}
		}
	}()

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(&obj); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}
