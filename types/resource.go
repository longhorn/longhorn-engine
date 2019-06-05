package types

type ReplicaInfo struct {
	Dirty           bool                `json:"dirty"`
	Rebuilding      bool                `json:"rebuilding"`
	Head            string              `json:"head"`
	Parent          string              `json:"parent"`
	Size            string              `json:"size"`
	SectorSize      int64               `json:"sectorSize,string"`
	BackingFile     string              `json:"backingFile"`
	State           string              `json:"state"`
	Chain           []string            `json:"chain"`
	Disks           map[string]DiskInfo `json:"disks"`
	RemainSnapshots int                 `json:"remainsnapshots"`
	RevisionCounter int64               `json:"revisioncounter,string"`
}

type DiskInfo struct {
	Name        string            `json:"name"`
	Parent      string            `json:"parent"`
	Children    map[string]bool   `json:"children"`
	Removed     bool              `json:"removed"`
	UserCreated bool              `json:"usercreated"`
	Created     string            `json:"created"`
	Size        string            `json:"size"`
	Labels      map[string]string `json:"labels"`
}
