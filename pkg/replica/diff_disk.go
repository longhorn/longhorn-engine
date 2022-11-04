package replica

import (
	"fmt"
	"io"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/rancher/go-fibmap"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

type diffDisk struct {
	rmLock sync.Mutex
	// mapping of sector to index in the files array. a value of 0 is special meaning
	// we don't know the location yet.
	location []byte
	// list of files in grandparent, parent, child, etc order.
	// index 0 is nil or backing file and index n-1 is the active write layer
	files      []types.DiffDisk
	sectorSize int64
	// current size of the head file.
	size int64
}

func (d *diffDisk) RemoveIndex(index int) error {
	if err := d.files[index].Close(); err != nil {
		return err
	}

	for i := 0; i < len(d.location); i++ {
		if d.location[i] == byte(index) {
			// set back to unknown
			d.location[i] = 0
		} else if d.location[i] > byte(index) {
			// move back by one
			d.location[i]--
		}
	}

	d.files = append(d.files[:index], d.files[index+1:]...)

	return nil
}

func (d *diffDisk) Expand(size int64) {
	d.rmLock.Lock()
	defer d.rmLock.Unlock()

	newLocationSize := int(size / d.sectorSize)
	if size%d.sectorSize != 0 {
		newLocationSize++
	}

	d.location = append(d.location, make([]byte, newLocationSize-len(d.location))...)
	d.size = size
	return
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
		return 0, fmt.Errorf("write len(%d), offset %d not a multiple of %d", len(buf), offset, d.sectorSize)
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
		return 0, fmt.Errorf("read not a multiple of %d", d.sectorSize)
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

func (d *diffDisk) read(target byte, buf []byte, startOffset int64, startSector int64, sectors int64) (int, error) {
	bufStart := startSector * d.sectorSize
	bufLength := sectors * d.sectorSize
	offset := startOffset + bufStart
	size, err := d.files[target].Size()
	if err != nil {
		return 0, err
	}

	// Reading the out-of-bound part is not allowed
	if bufLength > d.size-offset {
		logrus.Warnf("Trying to read the out-of-bound part")
		return 0, io.ErrUnexpectedEOF
	}

	// May read the expanded part
	if offset >= size {
		return 0, nil
	}
	var newBuf []byte
	if bufLength > size-offset {
		newBuf = buf[bufStart : bufStart+size-offset]
	} else {
		newBuf = buf[bufStart : bufStart+bufLength]
	}
	return d.files[target].ReadAt(newBuf, offset)
}

func (d *diffDisk) lookup(sector int64) (byte, error) {
	if sector >= int64(len(d.location)) {
		// We know the IO will result in EOF
		return byte(len(d.files) - 1), nil
	}

	if len(d.files) < 2 {
		return 0, fmt.Errorf("BUG: replica files cannot be less than 2")
	}

	if d.location[sector] == 0 {
		for i := len(d.files) - 1; i > 1; i-- {
			e, errno := fibmap.Fiemap(d.files[i].Fd(), uint64(sector*d.sectorSize), uint64(d.sectorSize), 1)
			if errno != 0 {
				return 0, fmt.Errorf(errno.Error())
			}
			if len(e) > 0 {
				d.location[sector] = byte(i)
				break
			}
		}
		if d.location[sector] == 0 {
			// We have hit the index 1 without found any data
			// This is important that for index 1 we don't check Fiemap because it may be a base image file
			// Also the result has to be 1 anyway, no matter it contains data or not
			d.location[sector] = 1
		}
	}
	return d.location[sector], nil
}

func (d *diffDisk) initializeSectorLocation(value byte) {
	for i := 0; i < len(d.location); i++ {
		d.location[i] = value
	}
}

func (d *diffDisk) preload() error {
	for i, f := range d.files {
		if i == 0 {
			continue
		}

		if err := LoadDiffDiskLocationList(d, f, byte(i)); err != nil {
			return err
		}

		if i == (len(d.files) - 1) {
			size, err := f.Size()
			if err != nil {
				return err
			}
			d.size = size
		}
	}

	return nil
}
