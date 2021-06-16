package replica

import (
	"github.com/rancher/go-fibmap"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

const MaxExtentsBuffer = 1024

func LoadDiffDiskLocationList(diffDisk *diffDisk, disk types.DiffDisk, currentFileIndex byte) error {
	fd := disk.Fd()

	// The backing file will have a Fd of 0
	if fd == 0 {
		return nil
	}

	start := uint64(0)
	end := uint64(len(diffDisk.location)) * uint64(diffDisk.sectorSize)
	for {
		extents, errno := fibmap.Fiemap(fd, start, end-start, MaxExtentsBuffer)
		if errno != 0 {
			return errno
		}

		if len(extents) == 0 {
			return nil
		}

		for _, extent := range extents {
			for i := int64(0); i < int64(extent.Length); i += diffDisk.sectorSize {
				diffDisk.location[(int64(extent.Logical)+i)/diffDisk.sectorSize] = currentFileIndex
			}
			if extent.Flags&fibmap.FIEMAP_EXTENT_LAST != 0 {
				return nil
			}
		}

		start = extents[len(extents)-1].Logical + extents[len(extents)-1].Length
	}
}
