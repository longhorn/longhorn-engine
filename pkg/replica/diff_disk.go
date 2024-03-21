package replica

import (
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/rancher/go-fibmap"

	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
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

// read called by fullReadAt reads the data from the target disk into buf
// offset is the offset of the whole volume
// startSector indicates the sector in the buffer from which the data should be read from the target disk.
// sectors indicates how many sectors we are going to read
func (d *diffDisk) read(target byte, buf []byte, offset int64, startSector int64, sectors int64) (int, error) {
	bufStart := startSector * d.sectorSize
	bufLength := sectors * d.sectorSize
	diskReadStartOffset := offset + bufStart
	diskReadEndOffset := diskReadStartOffset + bufLength
	diskSize, err := d.files[target].Size()
	if err != nil {
		return 0, err
	}
	volumeSize := d.size

	count := 0

	if diskReadStartOffset < diskSize {
		newBuf := buf[bufStart : bufStart+min(bufLength, diskSize-diskReadStartOffset)]
		n, err := d.files[target].ReadAt(newBuf, diskReadStartOffset)
		if err != nil {
			return n, err
		}
		count += n
	}
	if diskReadEndOffset <= diskSize {
		return count, nil
	}

	// Read out of disk boundary but still in Volume boundary, we should remain the valid range with 0 and return the correct count
	if diskReadEndOffset <= volumeSize {
		return count + int(diskReadEndOffset-max(diskSize, diskReadStartOffset)), nil
	}

	// Read out of Volume boundary, we should remain the valid range with 0 and return the correct count with EOF error
	return count + int(volumeSize-max(diskSize, diskReadStartOffset)), io.ErrUnexpectedEOF
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

func (d *diffDisk) UnmapAt(unmappableDisks []string, length uint32, offset int64) (int, error) {
	if length < uint32(d.sectorSize) {
		return 0, nil
	}

	origLength := length
	startSectorOffset := offset % d.sectorSize
	endSectorOffset := (int64(length) + offset) % d.sectorSize

	if startSectorOffset != 0 {
		offset += d.sectorSize - startSectorOffset
		length -= uint32(d.sectorSize - startSectorOffset)
	}
	if endSectorOffset != 0 {
		length -= uint32(endSectorOffset)
	}
	if length == 0 {
		return 0, nil
	}

	// The the final length must be smaller than original length. The following case should not happen.
	// The only case is the length is calculated to a negative number and then overflows a large number.
	// This protection mechanism avoids unmapping the abnormal length and returns errors.
	if origLength < length {
		return 0, fmt.Errorf("final unmap length(%v) should not be larger than original length(%v)", length, origLength)
	}

	var unmappedSizeErr error
	unmappedSize := int64(0)
	actualSizeBefore, actualSizeAfter := int64(0), int64(0)
	// Do unmap for the volume head and all continuous removing snapshots
	for idx := len(d.files) - 1; idx > 0 && len(unmappableDisks) > len(d.files)-1-idx; idx-- {
		diskFilePath := unmappableDisks[len(d.files)-1-idx]

		if unmappedSizeErr == nil {
			actualSizeBefore = util.GetFileActualSize(diskFilePath)
			if actualSizeBefore < 0 {
				unmappedSizeErr = fmt.Errorf("failed to get the disk file %v actual size before unmap", diskFilePath)
			}
		}

		apparentSize, err := d.files[idx].Size()
		if err != nil {
			return 0, errors.Wrapf(err, "failed to get the disk file %v apparent size before unmap", diskFilePath)
		}
		if apparentSize < offset {
			continue
		}
		curLength := length
		end := offset + int64(length)
		if apparentSize < end {
			curLength = uint32(apparentSize - offset)
		}
		if curLength <= 0 {
			continue
		}
		if _, err := d.files[idx].UnmapAt(curLength, offset); err != nil {
			return 0, errors.Wrapf(err, "failed to do unmap with offset %v and length %v for the disk file %v", offset, curLength, diskFilePath)
		}

		if unmappedSizeErr == nil {
			actualSizeAfter = util.GetFileActualSize(diskFilePath)
			if actualSizeAfter < 0 {
				unmappedSizeErr = fmt.Errorf("failed to get the disk file %v actual size after unmap", diskFilePath)
			}
		}
		if unmappedSizeErr == nil {
			unmappedSize += actualSizeBefore - actualSizeAfter
		}
	}
	if unmappedSizeErr != nil {
		logrus.Warnf("Unmapping disks succeeded but the unmapping size calculation failed, will return unmapping count 0 instead: %v", unmappedSizeErr)
		unmappedSize = 0
	}

	return int(unmappedSize), nil
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
