package api

type Process struct {
	Name      string   `json:"name"`
	Binary    string   `json:"binary"`
	Args      []string `json:"args"`
	PortCount int32    `json:"portCount"`
	PortArgs  []string `json:"portArgs"`

	State     string `json:"state"`
	ErrorMsg  string `json:"errorMsg"`
	PortStart int32  `json:"portStart"`
	PortEnd   int32  `json:"portEnd"`
}

type Engine struct {
	Name       string
	VolumeName string
	Binary     string
	ListenAddr string
	Listen     string
	Size       int64
	Frontend   string
	Backends   []string
	Replicas   []string

	Endpoint string
}
