package replica

import (
	"context"
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

	"github.com/rancher/go-fibmap"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
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
	deltaPrefix        = "volume-delta-"
	deltaSuffix        = ".img"
	deltaName          = deltaPrefix + "%s" + deltaSuffix
	snapTmpSuffix      = ".snap_tmp"
	tmpFileSuffix      = ".tmp"

	expansionSnapshotInfix = "expand-%d"

	replicaExpansionLabelKey = "replica-expansion"

	// Special indexes inside r.volume.files
	backingFileIndex = byte(1) // Index of backing file if the replica has backing file
	nilFileIndex     = byte(0) // Index 0 is a nil file. When a sector is mapped to nilFileIndex, it means we don't know the location for this sector yet
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
	// activeDiskData is in grandparent, parent, child, etc order.
	// index 0 is nil or backing file and index n-1 is the active write layer
	activeDiskData []*disk
	readOnly       bool

	revisionLock            sync.Mutex
	revisionCache           int64
	revisionFile            *sparse.DirectFileIoProcessor
	revisionRefreshed       bool
	revisionCounterDisabled bool
}

type Info struct {
	Size            int64
	Head            string
	Dirty           bool
	Rebuilding      bool
	Error           string
	Parent          string
	SectorSize      int64
	BackingFilePath string
	BackingFile     *backingfile.BackingFile `json:"-"`
}

type disk struct {
	Name        string
	Parent      string
	Removed     bool
	UserCreated bool
	Created     string
	Labels      map[string]string
}

type PrepareRemoveAction struct {
	Action string `json:"action"`
	Source string `json:"source"`
	Target string `json:"target"`
}

type DiskInfo struct {
	Name        string            `json:"name"`
	Parent      string            `json:"parent"`
	Children    map[string]bool   `json:"children"`
	Removed     bool              `json:"removed"`
	UserCreated bool              `json:"usercreated"`
	Created     string            `json:"created"`
	Size        string            `json:"size"`
	Labels      map[string]string `json:"labels"`
}

const (
	OpCoalesce = "coalesce" // Source is parent, target is child
	OpRemove   = "remove"
	OpReplace  = "replace"
	OpPrune    = "prune" // Remove overlapping chunks from the source.
)

func OpenSnapshot(dir string, snapshotName string) (*Replica, error) {
	snapshotDiskName := GenerateSnapshotDiskName(snapshotName)
	volumeInfo, err := ReadInfo(dir)
	if err != nil {
		return nil, err
	}
	var backingFile *backingfile.BackingFile
	if volumeInfo.BackingFilePath != "" {
		backingFilePath := volumeInfo.BackingFilePath
		if _, err := os.Stat(backingFilePath); err != nil {
			return nil, err
		}

		backingFile, err = backingfile.OpenBackingFile(backingFilePath)
		if err != nil {
			return nil, err
		}
	}

	r, err := NewReadOnly(dir, snapshotDiskName, backingFile)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func ReadInfo(dir string) (Info, error) {
	var info Info
	err := (&Replica{dir: dir}).unmarshalFile(volumeMetaData, &info)
	return info, err
}

func New(size, sectorSize int64, dir string, backingFile *backingfile.BackingFile, disableRevCounter bool) (*Replica, error) {
	return construct(false, size, sectorSize, dir, "", backingFile, disableRevCounter)
}

func NewReadOnly(dir, head string, backingFile *backingfile.BackingFile) (*Replica, error) {
	// size and sectorSize don't matter because they will be read from metadata
	return construct(true, 0, 512, dir, head, backingFile, false)
}

func construct(readonly bool, size, sectorSize int64, dir, head string, backingFile *backingfile.BackingFile, disableRevCounter bool) (*Replica, error) {
	if size%sectorSize != 0 {
		return nil, fmt.Errorf("Size %d not a multiple of sector size %d", size, sectorSize)
	}

	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	r := &Replica{
		dir:                     dir,
		activeDiskData:          make([]*disk, 1),
		diskData:                make(map[string]*disk),
		diskChildrenMap:         map[string]map[string]bool{},
		readOnly:                readonly,
		revisionCounterDisabled: disableRevCounter,
	}
	r.info.Size = size
	r.info.SectorSize = sectorSize
	r.volume.sectorSize = defaultSectorSize

	// Scan all the disks to build the disk map
	exists, err := r.readMetadata()
	if err != nil {
		return nil, err
	}

	// The backing file path can be changed, need to update it after loading
	// the meta data.
	r.info.BackingFile = backingFile
	if backingFile != nil {
		r.info.BackingFilePath = backingFile.Path
	}

	if !r.revisionCounterDisabled {
		if err := r.initRevisionCounter(); err != nil {
			return nil, err
		}
	}

	// Reference r.info.Size because it may have changed from reading
	// metadata
	locationSize := r.info.Size / r.volume.sectorSize
	if size%defaultSectorSize != 0 {
		locationSize++
	}
	r.volume.location = make([]byte, locationSize)
	r.volume.files = []types.DiffDisk{nil}
	r.volume.size = r.info.Size

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
		if err := r.createDisk("000", false, util.Now(), nil, size); err != nil {
			return nil, err
		}
	}

	if err := r.isExtentSupported(); err != nil {
		return nil, errors.Wrap(err, "file extent is unsupported")
	}

	r.info.Parent = r.diskData[r.info.Head].Parent

	r.insertBackingFile()

	return r, r.writeVolumeMetaData(true, r.info.Rebuilding)
}

func GenerateSnapshotDiskName(name string) string {
	return fmt.Sprintf(diskName, name)
}

func GenerateSnapshotDiskMetaName(diskName string) string {
	return diskName + metadataSuffix
}

func GenerateSnapTempFileName(fileName string) string {
	return fileName + snapTmpSuffix
}

func GetSnapshotNameFromTempFileName(tmpFileName string) (string, error) {
	if !strings.HasSuffix(tmpFileName, snapTmpSuffix) {
		return "", fmt.Errorf("invalid snapshot tmp filename")
	}
	return strings.TrimSuffix(tmpFileName, snapTmpSuffix), nil
}

func GetSnapshotNameFromDiskName(diskName string) (string, error) {
	if !strings.HasPrefix(diskName, diskPrefix) || !strings.HasSuffix(diskName, diskSuffix) {
		return "", fmt.Errorf("invalid snapshot disk name %v", diskName)
	}
	result := strings.TrimPrefix(diskName, diskPrefix)
	result = strings.TrimSuffix(result, diskSuffix)
	return result, nil
}

func GenerateDeltaFileName(name string) string {
	return fmt.Sprintf(deltaName, name)
}

func GenerateExpansionSnapshotName(size int64) string {
	return fmt.Sprintf(expansionSnapshotInfix, size)
}

func GenerateExpansionSnapshotLabels(size int64) map[string]string {
	return map[string]string{
		replicaExpansionLabelKey: strconv.FormatInt(size, 10),
	}
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

func (r *Replica) isExtentSupported() error {
	filePath := r.diskPath(r.info.Head + metadataSuffix)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	defer file.Close()

	fiemapFile := fibmap.NewFibmapFile(file)
	if _, errno := fiemapFile.Fiemap(uint32(fileInfo.Size())); errno != 0 {
		return errno
	}
	return nil
}
func (r *Replica) insertBackingFile() {
	if r.info.BackingFile == nil {
		return
	}

	d := disk{Name: r.info.BackingFile.Path}
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
	newReplica, err := New(r.info.Size, r.info.SectorSize, r.dir, r.info.BackingFile, r.revisionCounterDisabled)
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

func (r *Replica) RemoveDiffDisk(name string, force bool) error {
	r.Lock()
	defer r.Unlock()

	if name == r.info.Head {
		return fmt.Errorf("Can not delete the active differencing disk")
	}

	if err := r.removeDiskNode(name, force); err != nil {
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
			logrus.Infof("Disk %v cannot be found, may has already been removed", disk)
			return nil
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

	if err := r.removeDiskNode(source, false); err != nil {
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

// removeDiskNode with force = true should be only used when preparing rebuild,
// since the live chain needs to be overwritten
func (r *Replica) removeDiskNode(name string, force bool) error {
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
		if !force {
			return fmt.Errorf("Cannot remove snapshot %v with %v children",
				name, len(children))
		}
		logrus.Warnf("force delete disk %v with multiple children. Randomly choose a child to inherit", name)
	}

	// only one child from here (or forced deletion)
	var child string
	for child = range children {
	}
	r.updateChildDisk(name, child)
	if err := r.updateParentDisk(child, name); err != nil {
		return err
	}
	delete(r.diskData, name)
	delete(r.diskChildrenMap, name)

	index := r.findDisk(name)
	if index <= 0 {
		return nil
	}
	if err := r.volume.RemoveIndex(index); err != nil {
		return err
	}
	// len(r.activeDiskData)-1 is the volume head, so "-2" is the parent of
	// the volume head, which means the volume head's parent would need to
	// be updated
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
			logrus.Infof("Disk %v cannot be found, may has already been removed", disk)
			return nil, nil
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
		} else {
			// Remove the overlapping chunks from the snapshot while keeping
			// the volume head unchanged.
			actions = append(actions,
				PrepareRemoveAction{
					Action: OpPrune,
					Source: disk,
					Target: child,
				})
		}
		return actions, nil
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

func (r *Replica) closeWithoutWritingMetaData() {
	for i, f := range r.volume.files {
		if f != nil && !r.isBackingFile(i) {
			f.Close()
		}
	}
}

func (r *Replica) close() error {
	r.closeWithoutWritingMetaData()
	return r.writeVolumeMetaData(false, r.info.Rebuilding)
}

func (r *Replica) encodeToFile(obj interface{}, file string) (err error) {
	if r.readOnly {
		return nil
	}

	tmpFileName := fmt.Sprintf("%s%s", file, tmpFileSuffix)

	defer func() {
		var rollbackErr error
		if err != nil {
			if _, err := os.Stat(r.diskPath(tmpFileName)); err == nil {
				if err := os.Remove(r.diskPath(tmpFileName)); err != nil {
					rollbackErr = err
				}
			}
		}
		err = types.GenerateFunctionErrorWithRollback(err, rollbackErr)
	}()

	f, err := os.Create(r.diskPath(tmpFileName))
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

	return os.Rename(r.diskPath(tmpFileName), r.diskPath(file))
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

func (r *Replica) createNewHead(oldHead, parent, created string, size int64) (f types.DiffDisk, newDisk disk, err error) {
	newHeadName, err := r.nextFile(diskPattern, headName, oldHead)
	if err != nil {
		return nil, disk{}, err
	}

	if _, err := os.Stat(r.diskPath(newHeadName)); err == nil {
		return nil, disk{}, fmt.Errorf("%s already exists", newHeadName)
	}

	defer func() {
		var rollbackErr error
		if err != nil {
			if _, err := os.Stat(r.diskPath(newHeadName)); err == nil {
				if err := os.Remove(r.diskPath(newHeadName)); err != nil {
					rollbackErr = err
				}
			}
			if _, err := os.Stat(r.diskPath(newHeadName + metadataSuffix)); err == nil {
				if err := os.Remove(r.diskPath(newHeadName + metadataSuffix)); err != nil {
					rollbackErr = types.CombineErrors(rollbackErr, err)
				}
			}
			err = types.GenerateFunctionErrorWithRollback(err, rollbackErr)
		}
	}()

	f, err = r.openFile(r.diskPath(newHeadName), os.O_TRUNC)
	if err != nil {
		return nil, disk{}, err
	}
	if err := syscall.Truncate(r.diskPath(newHeadName), size); err != nil {
		return nil, disk{}, err
	}

	newDisk = disk{
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

	destMetadata := r.diskPath(newname + metadataSuffix)
	logrus.Infof("Deleting old disk metadata file %v", destMetadata)
	if err := os.RemoveAll(destMetadata); err != nil {
		return errors.Wrapf(err, "failed to delete old disk metadata file %v", destMetadata)
	}

	dest := r.diskPath(newname)
	logrus.Infof("Deleting old disk file %v", dest)
	if err := os.RemoveAll(dest); err != nil {
		return errors.Wrapf(err, "failed to delete old disk file %v", dest)
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

func (r *Replica) revertDisk(parentDiskFileName, created string) (*Replica, error) {
	if _, err := os.Stat(r.diskPath(parentDiskFileName)); err != nil {
		return nil, err
	}

	if diskInfo, exists := r.diskData[parentDiskFileName]; !exists {
		return nil, fmt.Errorf("cannot revert to disk file %v since it's not in the disk chain", parentDiskFileName)
	} else if diskInfo.Removed {
		return nil, fmt.Errorf("cannot revert to disk file %v since it's already marked as removed", parentDiskFileName)
	}

	oldHead := r.info.Head
	f, newHeadDisk, err := r.createNewHead(oldHead, parentDiskFileName, created, r.info.Size)
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

func (r *Replica) createDisk(name string, userCreated bool, created string, labels map[string]string, size int64) (err error) {
	log := logrus.WithFields(logrus.Fields{"disk": name})
	log.Info("Starting to create disk")
	if r.readOnly {
		return fmt.Errorf("Can not create disk on read-only replica")
	}

	if len(r.activeDiskData)+1 > maximumChainLength {
		return fmt.Errorf("Too many active disks: %v", len(r.activeDiskData)+1)
	}

	oldHead := r.info.Head
	newSnapName := GenerateSnapshotDiskName(name)

	if oldHead == "" {
		newSnapName = ""
	}

	if newSnapName != "" {
		if _, ok := r.diskData[newSnapName]; ok {
			return fmt.Errorf("snapshot %v is already existing", newSnapName)
		}
	}

	f, newHeadDisk, err := r.createNewHead(oldHead, newSnapName, created, size)
	if err != nil {
		return err
	}
	defer func() {
		var rollbackErr error
		if err != nil {
			log.WithError(err).Errorf("failed to create disk %v, will do rollback", name)
			delete(r.diskData, newHeadDisk.Name)
			delete(r.diskData, newSnapName)
			delete(r.diskChildrenMap, newSnapName)
			rollbackErr = types.CombineErrors(
				r.rmDisk(newHeadDisk.Name),
				r.rmDisk(newSnapName),
				f.Close(),
				r.encodeToFile(&r.info, volumeMetaData),
			)
			err = types.WrapError(
				types.GenerateFunctionErrorWithRollback(err, rollbackErr),
				"failed to create new disk %v", name)
		} else {
			r.rmDisk(oldHead)
		}
	}()

	if err := r.linkDisk(r.info.Head, newSnapName); err != nil {
		return err
	}

	info := r.info
	info.Head = newHeadDisk.Name
	info.Dirty = true
	info.Parent = newSnapName
	info.Size = size

	if err := r.encodeToFile(&info, volumeMetaData); err != nil {
		return err
	}

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

	log.Info("Finished creating disk")
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
			r.volume.size = r.info.Size
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

func (r *Replica) CloseWithoutWritingMetaData() {
	r.Lock()
	defer r.Unlock()

	r.closeWithoutWritingMetaData()
}

func (r *Replica) Delete() error {
	r.Lock()
	defer r.Unlock()

	for name := range r.diskData {
		if name != r.info.BackingFilePath {
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

	return r.createDisk(name, userCreated, created, labels, r.info.Size)
}

func (r *Replica) Revert(name, created string) (*Replica, error) {
	r.Lock()
	defer r.Unlock()

	return r.revertDisk(name, created)
}

func (r *Replica) Expand(size int64) (err error) {
	r.Lock()
	defer r.Unlock()

	if r.info.Size > size {
		return fmt.Errorf("Cannot expand replica to a smaller size %v", size)
	} else if r.info.Size == size {
		logrus.Infof("Replica had been expanded to size %v", size)
		return nil
	}

	// Will create a new head with the expanded size and write the new size into the meta file
	if err := r.createDisk(
		GenerateExpansionSnapshotName(size), false, util.Now(),
		GenerateExpansionSnapshotLabels(size), size); err != nil {
		return err
	}
	r.volume.Expand(size)

	return nil
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

	if !r.revisionCounterDisabled {
		if err := r.increaseRevisionCounter(); err != nil {
			return c, err
		}
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
		// Avoid inconsistent entry
		if disk.Labels == nil {
			diskInfo.Labels = map[string]string{}
		}

		diskInfo.Children = r.diskChildrenMap[disk.Name]
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
	ret := util.GetFileActualSize(r.diskPath(disk))
	if ret == -1 {
		errMessage := fmt.Sprintf("Fail to get file %v size", r.diskPath(disk))
		if r.info.Error == "" {
			r.info.Error = errMessage
			logrus.Error(errMessage)
		}
	}
	return ret
}

func (r *Replica) GetReplicaStat() (int64, int64) {
	lastModifyTime, headFileSize, err :=
		util.GetHeadFileModifyTimeAndSize(r.diskPath(r.info.Head))
	if err != nil {
		if r.info.Error == "" {
			r.info.Error = err.Error()
			logrus.Error(err)
		}
	}

	return lastModifyTime, headFileSize
}

// Preload populates r.volume.location with correct values
func (r *Replica) Preload(includeBackingFileLayer bool) error {
	if includeBackingFileLayer && r.info.BackingFile != nil {
		r.volume.initializeSectorLocation(backingFileIndex)
	} else {
		r.volume.initializeSectorLocation(nilFileIndex)
	}
	return r.volume.preload()
}

func (r *Replica) GetDataLayout(ctx context.Context) (<-chan sparse.FileInterval, <-chan error, error) {
	// baseDiskIndex is the smallest index in r.volume.files that is needed to export
	// Note that index 0 in r.volume.files is nil. Index 1 is backing file if the volume has backing file.
	// Otherwise, index 1 is regular disk file.
	baseDiskIndex := 1

	const MaxInflightIntervals = MaxExtentsBuffer * 10
	fileIntervalChannel := make(chan sparse.FileInterval, MaxInflightIntervals)
	errChannel := make(chan error)

	go func() {
		defer close(fileIntervalChannel)
		defer close(errChannel)

		// The replica consists of data and and hole region.
		// Inside this function scope, let represent 0 as hole sector and 1 as data sector.
		// Then a replica could look like 010001111...
		const (
			sectorTypeHole = 0
			sectorTypeData = 1
		)
		getSectorType := func(diskIndex byte) int {
			if diskIndex >= byte(baseDiskIndex) {
				return sectorTypeData
			}
			return sectorTypeHole
		}
		getFileIntervalKind := func(sectorType int) sparse.FileIntervalKind {
			if sectorType == sectorTypeHole {
				return sparse.SparseHole
			}
			return sparse.SparseData
		}

		lastSectorType := sectorTypeHole
		lastOffset := int64(0)
		for sectorIndex, diskIndex := range r.volume.location {
			offset := int64(sectorIndex) * r.volume.sectorSize
			currentSectorType := getSectorType(diskIndex)
			if currentSectorType != lastSectorType {
				if lastOffset < offset {
					fileInterval := sparse.FileInterval{Kind: getFileIntervalKind(lastSectorType), Interval: sparse.Interval{Begin: lastOffset, End: offset}}
					select {
					case fileIntervalChannel <- fileInterval:
					case <-ctx.Done():
						return
					}
				}
				lastOffset = offset
				lastSectorType = currentSectorType
			}
		}

		if lastOffset < r.volume.size {
			fileInterval := sparse.FileInterval{Kind: getFileIntervalKind(lastSectorType), Interval: sparse.Interval{Begin: lastOffset, End: r.volume.size}}
			select {
			case fileIntervalChannel <- fileInterval:
			case <-ctx.Done():
				return
			}
		}
	}()

	return fileIntervalChannel, errChannel, nil
}
