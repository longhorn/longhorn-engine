package util

import (
	"fmt"

	lhutils "github.com/longhorn/go-common-libs/utils"
)

const ISCSIdProcess = "iscsid"

func GetISCSIdNamespaceDirectory(procDir string) (string, error) {
	pids, err := lhutils.GetProcessPIDs(ISCSIdProcess, procDir)
	if err != nil {
		return "", err
	}

	return lhutils.GetNamespaceDirectory(procDir, fmt.Sprint(pids[0])), nil
}
