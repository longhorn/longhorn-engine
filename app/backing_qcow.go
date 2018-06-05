// +build qcow

package app

import (
	"github.com/rancher/longhorn-engine/qcow"
	"github.com/rancher/longhorn-engine/replica"
	"github.com/rancher/longhorn-engine/util"
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
