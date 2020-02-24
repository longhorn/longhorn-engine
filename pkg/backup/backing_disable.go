// +build !qcow

package backup

import (
	"fmt"

	"github.com/longhorn/longhorn-engine/pkg/replica"
)

func openBackingFile(file string) (*replica.BackingFile, error) {
	if file == "" {
		return nil, nil
	}
	return nil, fmt.Errorf("Backing file is not supported")
}
