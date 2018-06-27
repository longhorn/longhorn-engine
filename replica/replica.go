package replica

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-engine/types"
	"github.com/rancher/longhorn-engine/util"
	"github.com/rancher/sparse-tools/sparse"
)

const (
	metadataSuffix     = ".meta"
	imgSuffix          = ".img"
	volumeMetaData     = "volume.meta"
	defaultSectorSize  = 4096
	headPrefix         = "volume-head-"
	headSuffix         = ".img"
	headName           = headPrefix + "%03d" + headSuffix
	diskPrefix         = "volume-snap-"
	diskSuffix         = ".img"
	diskName           = diskPrefix + "%s" + diskSuffix
	maximumChainLength = 250
)

var (
	diskPattern = regexp.MustCompile(`volume-head-(\d)+.img`)
)

type Replica struct {
	sync.RWMutex
	volume          diffDisk
	dir             string
	info            Info
	diskData        map[string]*disk
	diskChildrenMap map[string]map[string]bool
	activeDiskData  []*disk
	readOnly        bool

	revisionLock  sync.Mutex
	revisionCache int64
	revisionFile  *sparse.DirectFileIoProcessor
}

type Info struct {
	Size            int64
	Head            string
	Dirty           bool
	Rebuilding      bool
	Parent          string
	SectorSize      int64
	BackingFileName string
	BackingFile     *BackingFile `json:"-"`
}

type disk struct {
	Name        string
	Parent      string
	Removed     bool
	UserCreated bool
	Created     string
	Labels      map[string]string
}

type BackingFile struct {
	Size       int64
	SectorSize int64
	Name       string
	Disk       types.DiffDisk
}

type PrepareRemoveAction struct {
	Action string `json:"action"`
	Source string `json:"source"`
	Target string `json:"target"`
}

type DiskInfo struct {
	Name        string              `json:"name"`
	Parent      string              `json:"parent"`
	Children    map[string]struct{} `json:"children"`
	Removed     bool                `json:"removed"`
	UserCreated bool                `json:"usercreated"`
	Created     string              `json:"created"`
	Size        string              `json:"size"`
	Labels      map[string]string   `json:"labels"`
}

const (
	OpCoalesce = "coalesce" // Source is parent, target is child
	OpRemove   = "remove"
	OpReplace  = "replace"
)

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
		dir:             dir,
		activeDiskData:  make([]*disk, 1),
		diskData:        make(map[string]*disk),
		diskChildrenMap: map[string]map[string]bool{},
	}
	r.info.Size = size
	r.info.SectorSize = sectorSize
	r.info.BackingFile = backingFile
	if backingFile != nil {
		r.info.BackingFileName = backingFile.Name
	}
	r.volume.sectorSize = defaultSectorSize

	// Scan all the disks to build the disk map
	exists, err := r.readMetadata()
	if err != nil {
		return nil, err
	}

	if err := r.initRevisionCounter(); err != nil {
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
		if err := r.openLiveChain(); err != nil {
			return nil, err
		}
	} else if size <= 0 {
		return nil, os.ErrNotExist
	} else {
		if err := r.createDisk("000", false, util.Now(), nil); err != nil {
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

func GetSnapshotNameFromDiskName(diskName string) (string, error) {
	if !strings.HasPrefix(diskName, diskPrefix) || !strings.HasSuffix(diskName, diskSuffix) {
		return "", fmt.Errorf("Invalid snapshot disk name %v", diskName)
	}
	result := strings.TrimPrefix(diskName, diskPrefix)
	result = strings.TrimSuffix(result, diskSuffix)
	return result, nil
}

func IsHeadDisk(diskName string) bool {
	if strings.HasPrefix(diskName, headPrefix) && strings.HasSuffix(diskName, headSuffix) {
		return true
	}
	return false
}

func (r *Replica) diskPath(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return path.Join(r.dir, name)
}

func (r *Replica) insertBackingFile() {
	if r.info.BackingFile == nil {
		return
	}

	d := disk{Name: r.info.BackingFile.Name}
	r.activeDiskData = append([]*disk{{}, &d}, r.activeDiskData[1:]...)
	r.volume.files = append([]types.DiffDisk{nil, r.info.BackingFile.Disk}, r.volume.files[1:]...)
	r.diskData[d.Name] = &d
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
		if i == 0 {
			continue
		}
		if d.Name == name {
			return i
		}
	}
	return 0
}

func (r *Replica) RemoveDiffDisk(name string) error {
	r.Lock()
	defer r.Unlock()

	if name == r.info.Head {
		return fmt.Errorf("Can not delete the active differencing disk")
	}

	if err := r.removeDiskNode(name); err != nil {
		return err
	}

	if err := r.rmDisk(name); err != nil {
		return err
	}

	return nil
}

func (r *Replica) MarkDiskAsRemoved(name string) error {
	r.Lock()
	defer r.Unlock()

	disk := name

	_, exists := r.diskData[disk]
	if !exists {
		disk = GenerateSnapshotDiskName(name)
		_, exists = r.diskData[disk]
		if !exists {
			return fmt.Errorf("Can not find snapshot %v", disk)
		}
	}

	if disk == r.info.Head {
		return fmt.Errorf("Can not mark the active differencing disk as removed")
	}

	if err := r.markDiskAsRemoved(disk); err != nil {
		return fmt.Errorf("Fail to mark disk %v as removed: %v", disk, err)
	}

	return nil
}

func (r *Replica) hardlinkDisk(target, source string) error {
	if _, err := os.Stat(r.diskPath(source)); err != nil {
		return fmt.Errorf("Cannot find source of replacing: %v", source)
	}

	if _, err := os.Stat(r.diskPath(target)); err == nil {
		logrus.Infof("Old file %s exists, deleting", target)
		if err := os.Remove(r.diskPath(target)); err != nil {
			return fmt.Errorf("Fail to remove %s: %v", target, err)
		}
	}

	if err := os.Link(r.diskPath(source), r.diskPath(target)); err != nil {
		return fmt.Errorf("Fail to link %s to %s", source, target)
	}
	return nil
}

func (r *Replica) ReplaceDisk(target, source string) error {
	r.Lock()
	defer r.Unlock()

	if target == r.info.Head {
		return fmt.Errorf("Can not replace the active differencing disk")
	}

	if err := r.hardlinkDisk(target, source); err != nil {
		return err
	}

	if err := r.removeDiskNode(source); err != nil {
		return err
	}

	if err := r.rmDisk(source); err != nil {
		return err
	}

	// the target file handler need to be refreshed for the hard linked disk
	index := r.findDisk(target)
	if index <= 0 {
		return nil
	}
	if err := r.volume.files[index].Close(); err != nil {
		return err
	}
	newFile, err := r.openFile(r.activeDiskData[index].Name, 0)
	if err != nil {
		return err
	}
	r.volume.files[index] = newFile

	logrus.Infof("Done replacing %v with %v", target, source)

	return nil
}

func (r *Replica) removeDiskNode(name string) error {
	// If snapshot has no child, then we can safely delete it
	// And it's definitely not in the live chain
	children, exists := r.diskChildrenMap[name]
	if !exists {
		r.updateChildDisk(name, "")
		delete(r.diskData, name)
		return nil
	}

	// If snapshot has more than one child, we cannot really delete it
	if len(children) > 1 {
		return fmt.Errorf("Cannot remove snapshot %v with %v children",
			name, len(children))
	}

	// only one child from here
	var child string
	for child = range children {
	}
	r.updateChildDisk(name, child)
	if err := r.updateParentDisk(child, name); err != nil {
		return err
	}
	delete(r.diskData, name)

	index := r.findDisk(name)
	if index <= 0 {
		return nil
	}
	if err := r.volume.RemoveIndex(index); err != nil {
		return err
	}
	if len(r.activeDiskData)-2 == index {
		r.info.Parent = r.diskData[r.info.Head].Parent
	}
	r.activeDiskData = append(r.activeDiskData[:index], r.activeDiskData[index+1:]...)

	return nil
}

func (r *Replica) PrepareRemoveDisk(name string) ([]PrepareRemoveAction, error) {
	r.Lock()
	defer r.Unlock()

	disk := name

	data, exists := r.diskData[disk]
	if !exists {
		disk = GenerateSnapshotDiskName(name)
		data, exists = r.diskData[disk]
		if !exists {
			return nil, fmt.Errorf("Can not find snapshot %v", disk)
		}
	}

	if disk == r.info.Head {
		return nil, fmt.Errorf("Can not delete the active differencing disk")
	}

	if !data.Removed {
		return nil, fmt.Errorf("Disk %v hasn't been marked as removed", disk)
	}

	actions, err := r.processPrepareRemoveDisks(disk)
	if err != nil {
		return nil, err
	}
	return actions, nil
}

func (r *Replica) processPrepareRemoveDisks(disk string) ([]PrepareRemoveAction, error) {
	actions := []PrepareRemoveAction{}

	if _, exists := r.diskData[disk]; !exists {
		return nil, fmt.Errorf("Wrong disk %v doesn't exist", disk)
	}

	children := r.diskChildrenMap[disk]
	// 1) leaf node
	if children == nil {
		actions = append(actions, PrepareRemoveAction{
			Action: OpRemove,
			Source: disk,
		})
		return actions, nil
	}

	// 2) has only one child and is not head
	if len(children) == 1 {
		var child string
		// Get the only element in children
		for child = range children {
		}
		if child != r.info.Head {
			actions = append(actions,
				PrepareRemoveAction{
					Action: OpCoalesce,
					Source: disk,
					Target: child,
				},
				PrepareRemoveAction{
					Action: OpReplace,
					Source: disk,
					Target: child,
				})
			return actions, nil
		}
	}

	logrus.Infof("Currently snapshot %v doesn't meet criteria to be removed, skip it for now", disk)
	return actions, nil
}

func (r *Replica) Info() Info {
	return r.info
}

func (r *Replica) DisplayChain() ([]string, error) {
	r.RLock()
	defer r.RUnlock()

	result := make([]string, 0, len(r.activeDiskData))

	cur := r.info.Head
	for cur != "" {
		disk, ok := r.diskData[cur]
		if !ok {
			return nil, fmt.Errorf("Failed to find metadata for %s", cur)
		}
		if !disk.Removed {
			result = append(result, cur)
		}
		cur = r.diskData[cur].Parent
	}

	return result, nil
}

func (r *Replica) Chain() ([]string, error) {
	r.RLock()
	defer r.RUnlock()

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

	f, err := os.Create(r.diskPath(file + ".tmp"))
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

	return os.Rename(r.diskPath(file+".tmp"), r.diskPath(file))
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
	return sparse.NewDirectFileIoProcessor(r.diskPath(name), os.O_RDWR|flag, 06666, true)
}

func (r *Replica) createNewHead(oldHead, parent, created string) (types.DiffDisk, disk, error) {
	newHeadName, err := r.nextFile(diskPattern, headName, oldHead)
	if err != nil {
		return nil, disk{}, err
	}

	if _, err := os.Stat(r.diskPath(newHeadName)); err == nil {
		return nil, disk{}, fmt.Errorf("%s already exists", newHeadName)
	}

	f, err := r.openFile(newHeadName, os.O_TRUNC)
	if err != nil {
		return nil, disk{}, err
	}
	if err := syscall.Truncate(r.diskPath(newHeadName), r.info.Size); err != nil {
		return nil, disk{}, err
	}

	newDisk := disk{
		Parent:      parent,
		Name:        newHeadName,
		Removed:     false,
		UserCreated: false,
		Created:     created,
	}
	err = r.encodeToFile(&newDisk, newHeadName+metadataSuffix)
	return f, newDisk, err
}

func (r *Replica) linkDisk(oldname, newname string) error {
	if oldname == "" {
		return nil
	}

	dest := r.diskPath(newname)
	if _, err := os.Stat(dest); err == nil {
		logrus.Infof("Old file %s exists, deleting", dest)
		if err := os.Remove(dest); err != nil {
			return err
		}
	}

	if err := os.Link(r.diskPath(oldname), dest); err != nil {
		return err
	}

	return os.Link(r.diskPath(oldname+metadataSuffix), r.diskPath(newname+metadataSuffix))
}

func (r *Replica) markDiskAsRemoved(name string) error {
	disk, ok := r.diskData[name]
	if !ok {
		return fmt.Errorf("Cannot find disk %v", name)
	}
	if stat, err := os.Stat(r.diskPath(name)); err != nil || stat.IsDir() {
		return fmt.Errorf("Cannot find disk file %v", name)
	}
	if stat, err := os.Stat(r.diskPath(name + metadataSuffix)); err != nil || stat.IsDir() {
		return fmt.Errorf("Cannot find disk metafile %v", name+metadataSuffix)
	}
	disk.Removed = true
	r.diskData[name] = disk
	return r.encodeToFile(disk, name+metadataSuffix)
}

func (r *Replica) rmDisk(name string) error {
	if name == "" {
		return nil
	}

	lastErr := os.Remove(r.diskPath(name))
	if err := os.Remove(r.diskPath(name + metadataSuffix)); err != nil {
		lastErr = err
	}
	return lastErr
}

func (r *Replica) revertDisk(parent, created string) (*Replica, error) {
	if _, err := os.Stat(r.diskPath(parent)); err != nil {
		return nil, err
	}

	oldHead := r.info.Head
	f, newHeadDisk, err := r.createNewHead(oldHead, parent, created)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info := r.info
	info.Head = newHeadDisk.Name
	info.Dirty = true
	info.Parent = newHeadDisk.Parent

	if err := r.encodeToFile(&info, volumeMetaData); err != nil {
		r.encodeToFile(&r.info, volumeMetaData)
		return nil, err
	}

	// Need to execute before r.Reload() to update r.diskChildrenMap
	r.rmDisk(oldHead)

	rNew, err := r.Reload()
	if err != nil {
		return nil, err
	}
	return rNew, nil
}

func (r *Replica) createDisk(name string, userCreated bool, created string, labels map[string]string) error {
	if r.readOnly {
		return fmt.Errorf("Can not create disk on read-only replica")
	}

	if len(r.activeDiskData)+1 > maximumChainLength {
		return fmt.Errorf("Too many active disks: %v", len(r.activeDiskData)+1)
	}

	done := false
	oldHead := r.info.Head
	newSnapName := GenerateSnapshotDiskName(name)

	if oldHead == "" {
		newSnapName = ""
	}

	f, newHeadDisk, err := r.createNewHead(oldHead, newSnapName, created)
	if err != nil {
		return err
	}
	defer func() {
		if !done {
			r.rmDisk(newHeadDisk.Name)
			r.rmDisk(newSnapName)
			f.Close()
			return
		}
		r.rmDisk(oldHead)
	}()

	if err := r.linkDisk(r.info.Head, newSnapName); err != nil {
		return err
	}

	info := r.info
	info.Head = newHeadDisk.Name
	info.Dirty = true
	info.Parent = newSnapName

	if err := r.encodeToFile(&info, volumeMetaData); err != nil {
		return err
	}

	done = true
	r.diskData[newHeadDisk.Name] = &newHeadDisk
	if newSnapName != "" {
		r.addChildDisk(newSnapName, newHeadDisk.Name)
		r.diskData[newSnapName] = r.diskData[oldHead]
		r.diskData[newSnapName].UserCreated = userCreated
		r.diskData[newSnapName].Created = created
		r.diskData[newSnapName].Labels = labels
		if err := r.encodeToFile(r.diskData[newSnapName], newSnapName+metadataSuffix); err != nil {
			return err
		}

		r.updateChildDisk(oldHead, newSnapName)
		r.activeDiskData[len(r.activeDiskData)-1].Name = newSnapName
	}
	delete(r.diskData, oldHead)

	r.info = info
	r.volume.files = append(r.volume.files, f)
	r.activeDiskData = append(r.activeDiskData, &newHeadDisk)

	return nil
}

func (r *Replica) addChildDisk(parent, child string) {
	children, exists := r.diskChildrenMap[parent]
	if !exists {
		children = map[string]bool{}
	}
	children[child] = true
	r.diskChildrenMap[parent] = children
}

func (r *Replica) rmChildDisk(parent, child string) {
	children, exists := r.diskChildrenMap[parent]
	if !exists {
		return
	}
	if _, exists := children[child]; !exists {
		return
	}
	delete(children, child)
	if len(children) == 0 {
		delete(r.diskChildrenMap, parent)
		return
	}
	r.diskChildrenMap[parent] = children
}

func (r *Replica) updateChildDisk(oldName, newName string) {
	parent := r.diskData[oldName].Parent
	r.rmChildDisk(parent, oldName)
	if newName != "" {
		r.addChildDisk(parent, newName)
	}
}

func (r *Replica) updateParentDisk(name, oldParent string) error {
	child := r.diskData[name]
	if oldParent != "" {
		child.Parent = r.diskData[oldParent].Parent
	} else {
		child.Parent = ""
	}
	r.diskData[name] = child
	return r.encodeToFile(child, child.Name+metadataSuffix)
}

func (r *Replica) openLiveChain() error {
	chain, err := r.Chain()
	if err != nil {
		return err
	}

	if len(chain) > maximumChainLength {
		return fmt.Errorf("Live chain is too long: %v", len(chain))
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
	r.diskData = make(map[string]*disk)

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
	data.Name = name
	r.diskData[name] = &data
	if data.Parent != "" {
		r.addChildDisk(data.Parent, data.Name)
	}
	return nil
}

func (r *Replica) unmarshalFile(file string, obj interface{}) error {
	p := r.diskPath(file)
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
		if name != r.info.BackingFileName {
			r.rmDisk(name)
		}
	}

	os.Remove(r.diskPath(volumeMetaData))
	os.Remove(r.diskPath(revisionCounterFile))
	return nil
}

func (r *Replica) Snapshot(name string, userCreated bool, created string, labels map[string]string) error {
	r.Lock()
	defer r.Unlock()

	return r.createDisk(name, userCreated, created, labels)
}

func (r *Replica) Revert(name, created string) (*Replica, error) {
	r.Lock()
	defer r.Unlock()

	return r.revertDisk(name, created)
}

func (r *Replica) WriteAt(buf []byte, offset int64) (int, error) {
	if r.readOnly {
		return 0, fmt.Errorf("Can not write on read-only replica")
	}

	r.RLock()
	r.info.Dirty = true
	c, err := r.volume.WriteAt(buf, offset)
	r.RUnlock()
	if err != nil {
		return c, err
	}
	if err := r.increaseRevisionCounter(); err != nil {
		return c, err
	}
	return c, nil
}

func (r *Replica) ReadAt(buf []byte, offset int64) (int, error) {
	r.RLock()
	c, err := r.volume.ReadAt(buf, offset)
	r.RUnlock()
	return c, err
}

func (r *Replica) ListDisks() map[string]DiskInfo {
	r.RLock()
	defer r.RUnlock()

	result := map[string]DiskInfo{}
	for _, disk := range r.diskData {
		diskSize := strconv.FormatInt(r.getDiskSize(disk.Name), 10)
		diskInfo := DiskInfo{
			Name:        disk.Name,
			Parent:      disk.Parent,
			Removed:     disk.Removed,
			UserCreated: disk.UserCreated,
			Created:     disk.Created,
			Size:        diskSize,
			Labels:      disk.Labels,
		}
		// Avoid inconsisent entry
		if disk.Labels == nil {
			diskInfo.Labels = map[string]string{}
		}
		children := map[string]struct{}{}
		for child := range r.diskChildrenMap[disk.Name] {
			children[child] = struct{}{}
		}
		diskInfo.Children = children
		result[disk.Name] = diskInfo
	}
	return result
}

func (r *Replica) GetRemainSnapshotCounts() int {
	r.RLock()
	defer r.RUnlock()

	return maximumChainLength - len(r.activeDiskData)
}

func (r *Replica) getDiskSize(disk string) int64 {
	return util.GetFileActualSize(r.diskPath(disk))
}
