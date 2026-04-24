package meta

const (
	// CLIAPIVersion used to communicate with user e.g. longhorn-manager
	CLIAPIVersion = 12
	// CLIAPIMinVersion is the minimum CLI API version that the engine supports. If the CLI API version of the client is lower than this, the engine will can not be used.
	// such as longhorn-manager, CLIAPIVersion 8 is introduced at 1.5.0, and CLIAPIMinVersion 8 is introduced at 1.6.0,
	// which means longhorn-manager lower than 1.5.0 can not work with engine 1.6.0 or later.
	CLIAPIMinVersion = 8

	// ControllerAPIVersion used to communicate with instance-manager
	ControllerAPIVersion    = 6
	ControllerAPIMinVersion = 4

	// DataFormatVersion used by the Replica to store data
	DataFormatVersion    = 1
	DataFormatMinVersion = 1
)

// Following variables are filled in by main.go
var (
	Version   string
	GitCommit string
	BuildDate string
)

type VersionOutput struct {
	Version   string `json:"version"`
	GitCommit string `json:"gitCommit"`
	BuildDate string `json:"buildDate"`

	CLIAPIVersion           int `json:"cliAPIVersion"`
	CLIAPIMinVersion        int `json:"cliAPIMinVersion"`
	ControllerAPIVersion    int `json:"controllerAPIVersion"`
	ControllerAPIMinVersion int `json:"controllerAPIMinVersion"`
	DataFormatVersion       int `json:"dataFormatVersion"`
	DataFormatMinVersion    int `json:"dataFormatMinVersion"`
}

func GetVersion() VersionOutput {
	return VersionOutput{
		Version:   Version,
		GitCommit: GitCommit,
		BuildDate: BuildDate,

		CLIAPIVersion:           CLIAPIVersion,
		CLIAPIMinVersion:        CLIAPIMinVersion,
		ControllerAPIVersion:    ControllerAPIVersion,
		ControllerAPIMinVersion: ControllerAPIMinVersion,
		DataFormatVersion:       DataFormatVersion,
		DataFormatMinVersion:    DataFormatMinVersion,
	}
}
