package replica

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

const (
	metadataSuffix    = ".meta"
	imgSuffix         = ".img"
	volumeMetaData    = "volume.meta"
	defaultSectorSize = 4096
	headName          = "volume-head-%03d.img"
	diskName          = "volume-snap-%s.img"
)

var (
	diskPattern = regexp.MustCompile(`volume-head-(\d)+.img`)
)

type Replica struct {
	sync.RWMutex
	volume         diffDisk
	dir            string
	info           Info
	diskData       map[string]disk
	activeDiskData []disk
	readOnly       bool
}

type Info struct {
	Size        int64
	Head        string
	Dirty       bool
	Rebuilding  bool
	Parent      string
	SectorSize  int64
	BackingFile *BackingFile `json:"-"`
}

type disk struct {
	name   string
	Parent string
}

type BackingFile struct {
	Size       int64
	SectorSize int64
	Name       string
	Disk       types.DiffDisk
}

func ReadInfo(dir string) (Info, error) {
	var info Info
	err := (&Replica{dir: dir}).unmarshalFile(volumeMetaData, &info)
	return info, err
}

func New(size, sectorSize int64, dir string, backingFile *BackingFile) (*Replica, error) {
	return construct(false, size, sectorSize, dir, "", backingFile)
}

func NewReadOnly(dir, head string, backingFile *BackingFile) (*Replica, error) {
	// size and sectorSize don't matter because they will be read from metadata
	return construct(true, 0, 512, dir, head, backingFile)
}

func construct(readonly bool, size, sectorSize int64, dir, head string, backingFile *BackingFile) (*Replica, error) {
	if size%sectorSize != 0 {
		return nil, fmt.Errorf("Size %d not a multiple of sector size %d", size, sectorSize)
	}

	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	r := &Replica{
		dir:            dir,
		activeDiskData: make([]disk, 1),
		diskData:       map[string]disk{},
	}
	r.info.Size = size
	r.info.SectorSize = sectorSize
	r.info.BackingFile = backingFile
	r.volume.sectorSize = defaultSectorSize

	exists, err := r.readMetadata()
	if err != nil {
		return nil, err
	}

	// Reference r.info.Size because it may have changed from reading
	// metadata
	locationSize := r.info.Size / r.volume.sectorSize
	if size%defaultSectorSize != 0 {
		locationSize++
	}
	r.volume.location = make([]byte, locationSize)
	r.volume.files = []types.DiffDisk{nil}

	if r.readOnly && !exists {
		return nil, os.ErrNotExist
	}

	if head != "" {
		r.info.Head = head
	}

	if exists {
		if err := r.openFiles(); err != nil {
			return nil, err
		}
	} else if size <= 0 {
		return nil, os.ErrNotExist
	} else {
		if err := r.createDisk("000"); err != nil {
			return nil, err
		}
	}

	r.info.Parent = r.diskData[r.info.Head].Parent

	r.insertBackingFile()

	return r, r.writeVolumeMetaData(true, r.info.Rebuilding)
}

func GenerateSnapshotDiskName(name string) string {
	return fmt.Sprintf(diskName, name)
}

func (r *Replica) insertBackingFile() {
	if r.info.BackingFile == nil {
		return
	}

	d := disk{name: r.info.BackingFile.Name}
	r.activeDiskData = append([]disk{disk{}, d}, r.activeDiskData[1:]...)
	r.volume.files = append([]types.DiffDisk{nil, r.info.BackingFile.Disk}, r.volume.files[1:]...)
	r.diskData[d.name] = d
}

func (r *Replica) SetRebuilding(rebuilding bool) error {
	err := r.writeVolumeMetaData(true, rebuilding)
	if err != nil {
		return err
	}
	r.info.Rebuilding = rebuilding
	return nil
}

func (r *Replica) Reload() (*Replica, error) {
	newReplica, err := New(r.info.Size, r.info.SectorSize, r.dir, r.info.BackingFile)
	if err != nil {
		return nil, err
	}
	newReplica.info.Dirty = r.info.Dirty
	return newReplica, nil
}

func (r *Replica) findDisk(name string) int {
	for i, d := range r.activeDiskData {
		if d.name == name {
			return i
		}
	}
	return 0
}

func (r *Replica) relinkChild(index int) error {
	childData := &r.activeDiskData[index+1]
	if index == 1 {
		childData.Parent = ""
	} else {
		childData.Parent = r.activeDiskData[index-1].name
	}

	r.diskData[childData.name] = *childData
	return r.encodeToFile(*childData, childData.name+metadataSuffix)
}

func (r *Replica) RemoveDiffDisk(name string) error {
	r.Lock()
	defer r.Unlock()

	index := r.findDisk(name)
	if index <= 0 {
		return nil
	}

	if len(r.activeDiskData)-1 == index {
		return fmt.Errorf("Can not delete the active differencing disk")
	}

	if err := r.relinkChild(index); err != nil {
		return err
	}

	if err := r.volume.RemoveIndex(index); err != nil {
		return err
	}

	if len(r.activeDiskData)-2 == index {
		r.info.Parent = r.diskData[r.info.Head].Parent
	}

	r.activeDiskData = append(r.activeDiskData[:index], r.activeDiskData[index+1:]...)
	delete(r.diskData, name)

	if err := r.rmDisk(name); err != nil {
		// ignore error deleting files
		logrus.Errorf("Failed to delete %s: %v", name, err)
	}

	return nil
}

func (r *Replica) Info() Info {
	return r.info
}

func (r *Replica) Chain() ([]string, error) {
	result := make([]string, 0, len(r.activeDiskData))

	cur := r.info.Head
	for cur != "" {
		result = append(result, cur)
		if _, ok := r.diskData[cur]; !ok {
			return nil, fmt.Errorf("Failed to find metadata for %s", cur)
		}
		cur = r.diskData[cur].Parent
	}

	return result, nil
}

func (r *Replica) writeVolumeMetaData(dirty, rebuilding bool) error {
	info := r.info
	info.Dirty = dirty
	info.Rebuilding = rebuilding
	return r.encodeToFile(&info, volumeMetaData)
}

func (r *Replica) isBackingFile(index int) bool {
	if r.info.BackingFile == nil {
		return false
	}
	return index == 1
}

func (r *Replica) close() error {
	for i, f := range r.volume.files {
		if f != nil && !r.isBackingFile(i) {
			f.Close()
		}
	}

	return r.writeVolumeMetaData(false, r.info.Rebuilding)
}

func (r *Replica) encodeToFile(obj interface{}, file string) error {
	if r.readOnly {
		return nil
	}

	f, err := os.Create(path.Join(r.dir, file+".tmp"))
	if err != nil {
		return err
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(&obj); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(path.Join(r.dir, file+".tmp"), path.Join(r.dir, file))
}

func (r *Replica) nextFile(parsePattern *regexp.Regexp, pattern, parent string) (string, error) {
	if parent == "" {
		return fmt.Sprintf(pattern, 0), nil
	}

	matches := parsePattern.FindStringSubmatch(parent)
	if matches == nil {
		return "", fmt.Errorf("Invalid name %s does not match pattern: %v", parent, parsePattern)
	}

	index, _ := strconv.Atoi(matches[1])
	return fmt.Sprintf(pattern, index+1), nil
}

func (r *Replica) openFile(name string, flag int) (types.DiffDisk, error) {
	f, err := os.OpenFile(path.Join(r.dir, name), syscall.O_DIRECT|os.O_RDWR|os.O_CREATE|flag, 0666)
	if err != nil {
		return nil, err
	}
	return &directFile{
		File: f,
	}, nil
}

func (r *Replica) createNewHead(oldHead, parent string) (types.DiffDisk, disk, error) {
	newHeadName, err := r.nextFile(diskPattern, headName, oldHead)
	if err != nil {
		return nil, disk{}, err
	}

	if _, err := os.Stat(path.Join(r.dir, newHeadName)); err == nil {
		return nil, disk{}, fmt.Errorf("%s already exists", newHeadName)
	}

	f, err := r.openFile(newHeadName, os.O_TRUNC)
	if err != nil {
		return nil, disk{}, err
	}
	if err := syscall.Truncate(path.Join(r.dir, newHeadName), r.info.Size); err != nil {
		return nil, disk{}, err
	}

	newDisk := disk{Parent: parent, name: newHeadName}
	err = r.encodeToFile(&newDisk, newHeadName+metadataSuffix)
	return f, newDisk, err
}

func (r *Replica) linkDisk(oldname, newname string) error {
	if oldname == "" {
		return nil
	}

	dest := path.Join(r.dir, newname)
	if _, err := os.Stat(dest); err == nil {
		logrus.Infof("Old file %s exists, deleting", dest)
		if err := os.Remove(dest); err != nil {
			return err
		}
	}

	if err := os.Link(path.Join(r.dir, oldname), dest); err != nil {
		return err
	}

	return os.Link(path.Join(r.dir, oldname+metadataSuffix), path.Join(r.dir, newname+metadataSuffix))
}

func (r *Replica) rmDisk(name string) error {
	if name == "" {
		return nil
	}

	lastErr := os.Remove(path.Join(r.dir, name))
	if err := os.Remove(path.Join(r.dir, name+metadataSuffix)); err != nil {
		lastErr = err
	}
	return lastErr
}

func (r *Replica) revertDisk(parent string) (*Replica, error) {
	if _, err := os.Stat(path.Join(r.dir, parent)); err != nil {
		return nil, err
	}

	oldHead := r.info.Head
	f, newHeadDisk, err := r.createNewHead(oldHead, parent)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info := r.info
	info.Head = newHeadDisk.name
	info.Dirty = true
	info.Parent = newHeadDisk.Parent

	if err := r.encodeToFile(&info, volumeMetaData); err != nil {
		r.encodeToFile(&r.info, volumeMetaData)
		return nil, err
	}

	rNew, err := r.Reload()
	if err != nil {
		return nil, err
	}

	r.rmDisk(oldHead)
	return rNew, nil
}

func (r *Replica) createDisk(name string) error {
	if r.readOnly {
		return fmt.Errorf("Can not create disk on read-only replica")
	}

	done := false
	oldHead := r.info.Head
	newSnapName := GenerateSnapshotDiskName(name)

	if oldHead == "" {
		newSnapName = ""
	}

	f, newHeadDisk, err := r.createNewHead(oldHead, newSnapName)
	if err != nil {
		return err
	}
	defer func() {
		if !done {
			r.rmDisk(newHeadDisk.name)
			r.rmDisk(newHeadDisk.Parent)
			f.Close()
			return
		}
		r.rmDisk(oldHead)
	}()

	if err := r.linkDisk(r.info.Head, newHeadDisk.Parent); err != nil {
		return err
	}

	info := r.info
	info.Head = newHeadDisk.name
	info.Dirty = true
	info.Parent = newHeadDisk.Parent

	if err := r.encodeToFile(&info, volumeMetaData); err != nil {
		return err
	}

	done = true
	r.diskData[newHeadDisk.name] = newHeadDisk
	if newHeadDisk.Parent != "" {
		r.diskData[newHeadDisk.Parent] = r.diskData[oldHead]
		r.activeDiskData[len(r.activeDiskData)-1].name = newHeadDisk.Parent
	}
	delete(r.diskData, oldHead)

	r.info = info
	r.volume.files = append(r.volume.files, f)
	r.activeDiskData = append(r.activeDiskData, newHeadDisk)

	return nil
}

func (r *Replica) openFiles() error {
	chain, err := r.Chain()
	if err != nil {
		return err
	}

	for i := len(chain) - 1; i >= 0; i-- {
		parent := chain[i]
		f, err := r.openFile(parent, 0)
		if err != nil {
			return err
		}

		r.volume.files = append(r.volume.files, f)
		r.activeDiskData = append(r.activeDiskData, r.diskData[parent])
	}

	return nil
}

func (r *Replica) readMetadata() (bool, error) {
	r.diskData = map[string]disk{}

	files, err := ioutil.ReadDir(r.dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	for _, file := range files {
		if file.Name() == volumeMetaData {
			if err := r.unmarshalFile(file.Name(), &r.info); err != nil {
				return false, err
			}
			r.volume.sectorSize = defaultSectorSize
		} else if strings.HasSuffix(file.Name(), metadataSuffix) {
			if err := r.readDiskData(file.Name()); err != nil {
				return false, err
			}
		}
	}

	return len(r.diskData) > 0, nil
}

func (r *Replica) readDiskData(file string) error {
	var data disk
	if err := r.unmarshalFile(file, &data); err != nil {
		return err
	}

	name := file[:len(file)-len(metadataSuffix)]
	data.name = name
	r.diskData[name] = data
	return nil
}

func (r *Replica) unmarshalFile(file string, obj interface{}) error {
	p := path.Join(r.dir, file)
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	return dec.Decode(obj)
}

func (r *Replica) Close() error {
	r.Lock()
	defer r.Unlock()

	return r.close()
}

func (r *Replica) Delete() error {
	r.Lock()
	defer r.Unlock()

	for name := range r.diskData {
		r.rmDisk(name)
	}

	os.Remove(path.Join(r.dir, volumeMetaData))
	return nil
}

func (r *Replica) Snapshot(name string) error {
	r.Lock()
	defer r.Unlock()

	return r.createDisk(name)
}

func (r *Replica) Revert(name string) (*Replica, error) {
	r.Lock()
	defer r.Unlock()

	return r.revertDisk(name)
}

func (r *Replica) WriteAt(buf []byte, offset int64) (int, error) {
	if r.readOnly {
		return 0, fmt.Errorf("Can not write on read-only replica")
	}

	r.RLock()
	r.info.Dirty = true
	c, err := r.volume.WriteAt(buf, offset)
	r.RUnlock()
	return c, err
}

func (r *Replica) ReadAt(buf []byte, offset int64) (int, error) {
	r.RLock()
	c, err := r.volume.ReadAt(buf, offset)
	r.RUnlock()
	return c, err
}
