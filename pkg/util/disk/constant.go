package diskutil

const (
	VolumeHeadDiskPrefix = "volume-head-"
	VolumeHeadDiskSuffix = ".img"
	VolumeHeadDiskName   = VolumeHeadDiskPrefix + "%03d" + VolumeHeadDiskSuffix

	SnapshotDiskPrefix = "volume-snap-"
	SnapshotDiskSuffix = ".img"
	SnapshotDiskName   = SnapshotDiskPrefix + "%s" + SnapshotDiskSuffix

	DeltaDiskPrefix = "volume-delta-"
	DeltaDiskSuffix = ".img"
	DeltaDiskName   = DeltaDiskPrefix + "%s" + DeltaDiskSuffix

	DiskMetadataSuffix = ".meta"
	DiskChecksumSuffix = ".checksum"

	snapTmpSuffix = ".snap_tmp"

	expansionSnapshotInfix = "expand-%d"

	replicaExpansionLabelKey = "replica-expansion"
)

const (
	VolumeSectorSize       = 4096
	ReplicaSectorSize      = 512
	BackingImageSectorSize = 512
)
