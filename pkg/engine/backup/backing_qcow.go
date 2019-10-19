// +build qcow

package backup

import (
	"github.com/longhorn/longhorn-engine/pkg/engine/qcow"
	"github.com/longhorn/longhorn-engine/pkg/engine/replica"
)

func openBackingFile(file string) (*replica.BackingFile, error) {
	if file == "" {
		return nil, nil
	}

	f, err := qcow.Open(file)
	if err != nil {
		return nil, err
	}

	size, err := f.Size()
	if err != nil {
		return nil, err
	}

	return &replica.BackingFile{
		Name:       file,
		Disk:       f,
		Size:       size,
		SectorSize: 512,
	}, nil
}
