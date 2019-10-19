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

type PrepareRemoveAction struct {
	Action string `json:"action"`
	Source string `json:"source"`
	Target string `json:"target"`
}

type VolumeInfo struct {
	Name          string `json:"name"`
	ReplicaCount  int    `json:"replicaCount"`
	Endpoint      string `json:"endpoint"`
	Frontend      string `json:"frontend"`
	FrontendState string `json:"frontendState"`
	IsRestoring   bool   `json:"isRestoring"`
	LastRestored  string `json:"lastRestored"`
}

type ControllerReplicaInfo struct {
	Address string `json:"address"`
	Mode    Mode   `json:"mode"`
}

type PrepareRebuildOutput struct {
	Disks []string `json:"disks"`
}
