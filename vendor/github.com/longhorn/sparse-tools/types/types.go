package types

const (
	SnapshotDiskSuffix = ".img"
	DiskChecksumSuffix = ".checksum"
)

type SnapshotHashInfo struct {
	Method            string `json:"method"`
	Checksum          string `json:"checksum"`
	ChangeTime        string `json:"change_time"`
	LastHashedAt      string `json:"last_hashed_at"`
	SilentlyCorrupted bool   `json:"silently_corrupted"`
}
