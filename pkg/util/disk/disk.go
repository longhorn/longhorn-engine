package diskutil

import (
	"fmt"
	"strconv"
	"strings"
)

func GenerateSnapshotDiskName(name string) string {
	return fmt.Sprintf(SnapshotDiskName, name)
}

func GenerateSnapshotDiskChecksumName(diskName string) string {
	return diskName + DiskChecksumSuffix
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

// GenerateExpansionSnapshotName includes the volume name in the expansion
// snapshot name. All replicas of a volume share the same volume name, so they
// generate the same name, while different volumes get different names. This
// avoids Snapshot CR name collisions within the same Longhorn namespace when
// two volumes are expanded to the same size. When the volume name is empty, it
// falls back to the size-only form to keep the result a valid DNS-1123 name.
func GenerateExpansionSnapshotName(volumeName string, size int64) string {
	name := fmt.Sprintf(expansionSnapshotInfix, size)
	if volumeName != "" {
		name += "-" + volumeName
	}
	return name
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
