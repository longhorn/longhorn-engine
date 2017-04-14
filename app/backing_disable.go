// +build !qcow

package app

import (
	"fmt"

	"github.com/rancher/longhorn-engine/replica"
)

func openBackingFile(file string) (*replica.BackingFile, error) {
	if file == "" {
		return nil, nil
	}
	return nil, fmt.Errorf("Backing file is not supported")
}
