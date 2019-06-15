package api

type Process struct {
	Name          string   `json:"name"`
	Binary        string   `json:"binary"`
	Args          []string `json:"args"`
	ReservedPorts []int32  `json:"reservedPorts"`
	Status        string   `json:"status"`
	ErrorMsg      string   `json:"errorMsg"`
}
