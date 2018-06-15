package meta

const (
	// CLIAPIVersion used to communicate with user e.g. longhorn-manager
	CLIAPIVersion    = 1
	CLIAPIMinVersion = 1

	// ControllerAPIVersion used to communicate with engine-launcher
	ControllerAPIVersion    = 1
	ControllerAPIMinVersion = 1

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
	Version   string
	GitCommit string
	BuildDate string

	CLIAPIVersion           int
	CLIAPIMinVersion        int
	ControllerAPIVersion    int
	ControllerAPIMinVersion int
	DataFormatVersion       int
	DataFormatMinVersion    int
}

func GetVersion() *VersionOutput {
	return &VersionOutput{
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
