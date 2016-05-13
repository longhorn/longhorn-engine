package replica

import (
	"fmt"
	"sync"

	"github.com/frostschutz/go-fibmap"
	"github.com/rancher/longhorn/types"
)

type diffDisk struct {
	rmLock sync.Mutex
	// mapping of sector to index in the files array. a value of 0 is special meaning
	// we don't know the location yet.
	location []byte
	// list of files in child, parent, grandparent, etc order.
	// index 0 is nil and index 1 is the active write layer
	files      []types.DiffDisk
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
	startOffset := offset % d.sectorSize
	startCut := d.sectorSize - startOffset
	endOffset := (int64(len(buf)) + offset) % d.sectorSize

	if len(buf) == 0 {
		return 0, nil
	}

	if startOffset == 0 && endOffset == 0 {
		return d.fullWriteAt(buf, offset)
	}

	// single block
	if startCut >= int64(len(buf)) {
		return d.readModifyWrite(buf, offset)
	}

	if _, err := d.readModifyWrite(buf[0:startCut], offset); err != nil {
		return 0, err
	}

	if _, err := d.fullWriteAt(buf[startCut:int64(len(buf))-endOffset], offset+startCut); err != nil {
		return 0, err
	}

	if _, err := d.readModifyWrite(buf[int64(len(buf))-endOffset:], offset+int64(len(buf))-endOffset); err != nil {
		return 0, err
	}

	return len(buf), nil
}

func (d *diffDisk) readModifyWrite(buf []byte, offset int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	d.rmLock.Lock()
	defer d.rmLock.Unlock()

	readBuf := make([]byte, d.sectorSize)
	readOffset := (offset / d.sectorSize) * d.sectorSize

	if _, err := d.fullReadAt(readBuf, readOffset); err != nil {
		return 0, err
	}

	copy(readBuf[offset%d.sectorSize:], buf)

	return d.fullWriteAt(readBuf, readOffset)
}

func (d *diffDisk) fullWriteAt(buf []byte, offset int64) (int, error) {
	if int64(len(buf))%d.sectorSize != 0 || offset%d.sectorSize != 0 {
		return 0, fmt.Errorf("Write len(%d), offset %d not a multiple of %d", len(buf), offset, d.sectorSize)
	}

	target := byte(len(d.files) - 1)
	startSector := offset / d.sectorSize
	sectors := int64(len(buf)) / d.sectorSize

	c, err := d.files[target].WriteAt(buf, offset)

	// Regardless of err mark bytes as written
	for i := int64(0); i < sectors; i++ {
		d.location[startSector+i] = target
	}

	return c, err
}

func (d *diffDisk) ReadAt(buf []byte, offset int64) (int, error) {
	startOffset := offset % d.sectorSize
	startCut := d.sectorSize - startOffset
	endOffset := (int64(len(buf)) + offset) % d.sectorSize

	if len(buf) == 0 {
		return 0, nil
	}

	if startOffset == 0 && endOffset == 0 {
		return d.fullReadAt(buf, offset)
	}

	readBuf := make([]byte, d.sectorSize)
	if _, err := d.fullReadAt(readBuf, offset-startOffset); err != nil {
		return 0, err
	}

	copy(buf, readBuf[startOffset:])

	if startCut >= int64(len(buf)) {
		return len(buf), nil
	}

	if _, err := d.fullReadAt(buf[startCut:int64(len(buf))-endOffset], offset+startCut); err != nil {
		return 0, err
	}

	if endOffset > 0 {
		if _, err := d.fullReadAt(readBuf, offset+int64(len(buf))-endOffset); err != nil {
			return 0, err
		}

		copy(buf[int64(len(buf))-endOffset:], readBuf[:endOffset])
	}

	return len(buf), nil
}

func (d *diffDisk) fullReadAt(buf []byte, offset int64) (int, error) {
	if int64(len(buf))%d.sectorSize != 0 || offset%d.sectorSize != 0 {
		return 0, fmt.Errorf("Read not a multiple of %d", d.sectorSize)
	}

	if len(buf) == 0 {
		return 0, nil
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
	return d.files[target].ReadAt(newBuf, offset+bufStart)
}

func (d *diffDisk) lookup(sector int64) (byte, error) {
	if sector >= int64(len(d.location)) {
		// We know the IO will result in EOF
		return byte(len(d.files) - 1), nil
	}

	// small optimization
	if int64(len(d.files)) == 2 {
		return 1, nil
	}

	target := d.location[sector]

	if target == 0 {
		for i := len(d.files) - 1; i > 0; i-- {
			if i == 1 {
				// This is important that for index 1 we don't check Fiemap because it may be a base image file
				// Also the result has to be 1
				d.location[sector] = byte(i)
				return byte(i), nil
			}

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
