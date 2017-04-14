// +build qcow

package backup

import (
	"github.com/rancher/longhorn-engine/qcow"
	"github.com/rancher/longhorn-engine/replica"
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
