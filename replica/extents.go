package replica

import (
	"github.com/frostschutz/go-fibmap"
	"github.com/rancher/longhorn/types"
)

type UsedGenerator struct {
	err  error
	disk types.DiffDisk
	d    *diffDisk
}

func newGenerator(diffDisk *diffDisk, disk types.DiffDisk) *UsedGenerator {
	return &UsedGenerator{
		disk: disk,
		d:    diffDisk,
	}
}

func (u *UsedGenerator) Err() error {
	return u.err
}

func (u *UsedGenerator) Generate() <-chan int64 {
	c := make(chan int64)
	go u.findExtents(c)
	return c
}

func (u *UsedGenerator) findExtents(c chan<- int64) {
	defer close(c)

	fd := u.disk.Fd()

	// The backing file will have a Fd of 0
	if fd == 0 {
		return
	}

	start := uint64(0)
	end := uint64(len(u.d.location)) * uint64(u.d.sectorSize)
	for {
		extents, errno := fibmap.Fiemap(fd, start, end-start, 1024)
		if errno != 0 {
			u.err = errno
			return
		}

		if len(extents) == 0 {
			return
		}

		for _, extent := range extents {
			start = extent.Logical + extent.Length
			for i := int64(0); i < int64(extent.Length); i += u.d.sectorSize {
				c <- (int64(extent.Logical) + i) / u.d.sectorSize
			}
			if extent.Flags&fibmap.FIEMAP_EXTENT_LAST != 0 {
				return
			}
		}
	}
}
