package diskutil

import (
	"fmt"
	"strconv"
	"strings"
)

func GenerateSnapshotDiskName(name string) string {
	return fmt.Sprintf(SnapshotDiskName, name)
}

func GenerateSnapshotDiskMetaName(diskName string) string {
	return diskName + DiskMetadataSuffix
}

func GenerateDeltaFileName(name string) string {
	return fmt.Sprintf(DeltaDiskName, name)
}

func GenerateSnapTempFileName(fileName string) string {
	return fileName + snapTmpSuffix
}

func GetSnapshotNameFromTempFileName(tmpFileName string) (string, error) {
	if !strings.HasSuffix(tmpFileName, snapTmpSuffix) {
		return "", fmt.Errorf("invalid snapshot tmp filename")
	}
	return strings.TrimSuffix(tmpFileName, snapTmpSuffix), nil
}

func GetSnapshotNameFromDiskName(diskName string) (string, error) {
	if !strings.HasPrefix(diskName, SnapshotDiskPrefix) || !strings.HasSuffix(diskName, SnapshotDiskSuffix) {
		return "", fmt.Errorf("invalid snapshot disk name %v", diskName)
	}
	result := strings.TrimPrefix(diskName, SnapshotDiskPrefix)
	result = strings.TrimSuffix(result, SnapshotDiskSuffix)
	return result, nil
}

func GenerateExpansionSnapshotName(size int64) string {
	return fmt.Sprintf(expansionSnapshotInfix, size)
}

func GenerateExpansionSnapshotLabels(size int64) map[string]string {
	return map[string]string{
		replicaExpansionLabelKey: strconv.FormatInt(size, 10),
	}
}

func IsHeadDisk(diskName string) bool {
	if strings.HasPrefix(diskName, VolumeHeadDiskPrefix) &&
		strings.HasSuffix(diskName, VolumeHeadDiskSuffix) {
		return true
	}
	return false
}
