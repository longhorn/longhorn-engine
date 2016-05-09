package replica

import (
	"fmt"
	"os"

	"github.com/frostschutz/go-fibmap"
	fio "github.com/rancher/sparse-tools/directfio"
)

type diffDisk struct {
	// mapping of sector to index in the files array. a value of 0 is special meaning
	// we don't know the location yet.
	location []byte
	// list of files in child, parent, grandparent, etc order.
	// index 0 is nil and index 1 is the active write layer
	files      []*os.File
	sectorSize int64
}

func (d *diffDisk) RemoveIndex(index int) error {
	if err := d.files[index].Close(); err != nil {
		return err
	}

	for i := 0; i < len(d.location); i++ {
		if d.location[i] >= byte(index) {
			// set back to unknown
			d.location[i] = 0
		}
	}

	d.files = append(d.files[:index], d.files[index+1:]...)

	return nil
}

func (d *diffDisk) WriteAt(buf []byte, offset int64) (int, error) {
	if int64(len(buf))%d.sectorSize != 0 || offset%d.sectorSize != 0 {
		return 0, fmt.Errorf("Write not a multiple of %d", d.sectorSize)
	}

	target := byte(len(d.files) - 1)
	startSector := offset / d.sectorSize
	sectors := int64(len(buf)) / d.sectorSize

	c, err := fio.WriteAt(d.files[target], buf, offset)

	// Regardless of err mark bytes as written
	for i := int64(0); i < sectors; i++ {
		d.location[startSector+i] = target
	}

	return c, err
}

func (d *diffDisk) ReadAt(buf []byte, offset int64) (int, error) {
	if int64(len(buf))%d.sectorSize != 0 || offset%d.sectorSize != 0 {
		return 0, fmt.Errorf("Read not a multiple of %d", d.sectorSize)
	}

	count := 0
	sectors := int64(len(buf)) / d.sectorSize
	readSectors := int64(1)
	startSector := offset / d.sectorSize
	target, err := d.lookup(startSector)
	if err != nil {
		return count, err
	}

	for i := int64(1); i < sectors; i++ {
		newTarget, err := d.lookup(startSector + i)
		if err != nil {
			return count, err
		}

		if newTarget == target {
			readSectors++
		} else {
			c, err := d.read(target, buf, offset, i-readSectors, readSectors)
			count += c
			if err != nil {
				return count, err
			}
			readSectors = 1
			target = newTarget
		}
	}

	if readSectors > 0 {
		c, err := d.read(target, buf, offset, sectors-readSectors, readSectors)
		count += c
		if err != nil {
			return count, err
		}
	}

	return count, nil
}

func (d *diffDisk) read(target byte, buf []byte, offset int64, startSector int64, sectors int64) (int, error) {
	bufStart := startSector * d.sectorSize
	bufEnd := sectors * d.sectorSize
	newBuf := buf[bufStart : bufStart+bufEnd]
	return fio.ReadAt(d.files[target], newBuf, offset+bufStart)
}

func (d *diffDisk) lookup(sector int64) (byte, error) {
	dlength := int64(len(d.location))
	if sector >= dlength {
		// We know the IO will result in EOF
		return byte(len(d.files) - 1), nil
	}

	// small optimization
	if dlength == 2 {
		return 1, nil
	}

	target := d.location[sector]

	if target == 0 {
		for i := len(d.files) - 1; i > 0; i-- {
			e, err := fibmap.Fiemap(d.files[i].Fd(), uint64(sector*d.sectorSize), uint64(d.sectorSize), 1)
			if err != 0 {
				return byte(0), err
			}
			if len(e) > 0 {
				d.location[sector] = byte(i)
				return byte(i), nil
			}
		}
		return byte(len(d.files) - 1), nil
	}
	return target, nil
}
