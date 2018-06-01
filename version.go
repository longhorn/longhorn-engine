package main

import (
	"encoding/json"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/urfave/cli"
)

type VersionOutput struct {
	Version string

	CLIAPIVersion           int
	CLIAPIMinVersion        int
	ControllerAPIVersion    int
	ControllerAPIMinVersion int
	DataFormatVersion       int
	DataFormatMinVersion    int

	GitCommit string
	BuildDate string
}

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

// following variables will be filled by `-ldflags "-X ..."`
var (
	Version   string
	GitCommit string
	BuildDate string
)

func VersionCmd() cli.Command {
	return cli.Command{
		Name: "version",
		Action: func(c *cli.Context) {
			if err := version(c); err != nil {
				logrus.Fatalln("Error running info command:", err)
			}
		},
	}
}

func version(c *cli.Context) error {
	v := &VersionOutput{
		Version: Version,

		CLIAPIVersion:           CLIAPIVersion,
		CLIAPIMinVersion:        CLIAPIMinVersion,
		ControllerAPIVersion:    ControllerAPIVersion,
		ControllerAPIMinVersion: ControllerAPIMinVersion,
		DataFormatVersion:       DataFormatVersion,
		DataFormatMinVersion:    DataFormatMinVersion,

		GitCommit: GitCommit,
		BuildDate: BuildDate,
	}

	output, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}
