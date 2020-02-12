package meta

// For the following API versions, if a new field/API is added and the gRPC/cli
// backward compatibility is not broken, bumping up APIVersion is enough.
// But if a existing field/API is modified or deleted, bumping up both APIVersion
// and APIMinVersion are required.
const (
	// CLIAPIVersion used to communicate with user e.g. longhorn-manager
	// These 2 version numbers involves in the common volume operation commands,
	// e.g. snapshot and backup.
	CLIAPIVersion    = 3
	CLIAPIMinVersion = 3

	// ControllerAPIVersion used to indicate the current live upgrade mechanism.
	// And the live upgrade mechanism is related to the instance-manager process
	// replacement and the engine/replica starting command.
	// Besides, ControllerAPIMinVersion needs to be bumped up once
	// InstanceManagerAPIMinVersion is bumped up.
	ControllerAPIVersion    = 3
	ControllerAPIMinVersion = 3

	// DataFormatVersion used by the Replica to store data
	DataFormatVersion    = 1
	DataFormatMinVersion = 1

	// InstanceManagerAPIVersion used to communicate with user e.g.longhorn-manager.
	// It indicates the current API version of the instance-manager gRPC server.
	// These 2 version numbers rely on the instance-manager gRPC definition.
	InstanceManagerAPIVersion = 1
	// InstanceManagerAPIMinVersion indicates the minimal version that this engine can share
	// the instance-manager with other engines.
	InstanceManagerAPIMinVersion = 1
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

	CLIAPIVersion                int `json:"cliAPIVersion"`
	CLIAPIMinVersion             int `json:"cliAPIMinVersion"`
	ControllerAPIVersion         int `json:"controllerAPIVersion"`
	ControllerAPIMinVersion      int `json:"controllerAPIMinVersion"`
	DataFormatVersion            int `json:"dataFormatVersion"`
	DataFormatMinVersion         int `json:"dataFormatMinVersion"`
	InstanceManagerAPIVersion    int `json:"instanceManagerAPIVersion"`
	InstanceManagerAPIMinVersion int `json:"instanceManagerAPIMinVersion"`
}

func GetVersion() VersionOutput {
	return VersionOutput{
		Version:   Version,
		GitCommit: GitCommit,
		BuildDate: BuildDate,

		CLIAPIVersion:                CLIAPIVersion,
		CLIAPIMinVersion:             CLIAPIMinVersion,
		ControllerAPIVersion:         ControllerAPIVersion,
		ControllerAPIMinVersion:      ControllerAPIMinVersion,
		DataFormatVersion:            DataFormatVersion,
		DataFormatMinVersion:         DataFormatMinVersion,
		InstanceManagerAPIVersion:    InstanceManagerAPIVersion,
		InstanceManagerAPIMinVersion: InstanceManagerAPIMinVersion,
	}
}
