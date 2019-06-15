package api

type Process struct {
	Name          string   `json:"name"`
	Binary        string   `json:"binary"`
	Args          []string `json:"args"`
	ReservedPorts []int32  `json:"reservedPorts"`
	State         string   `json:"state"`
	ErrorMsg      string   `json:"errorMsg"`
}
