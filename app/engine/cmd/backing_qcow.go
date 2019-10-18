// +build qcow

package cmd

import (
	"github.com/longhorn/longhorn-engine/qcow"
	"github.com/longhorn/longhorn-engine/replica"
	"github.com/longhorn/longhorn-engine/util"
)

func openBackingFile(file string) (*replica.BackingFile, error) {
	if file == "" {
		return nil, nil
	}

	file, err := util.ResolveBackingFilepath(file)
	if err != nil {
		return nil, err
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
