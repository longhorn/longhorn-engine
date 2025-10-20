package replica

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/rancher/go-fibmap"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"

	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

const (
	volumeMetaData = "volume.meta"

	tmpFileSuffix = ".tmp"

	// Special indexes inside r.volume.files
	backingFileIndex = byte(1) // Index of backing file if the replica has backing file
	nilFileIndex     = byte(0) // Index 0 is a nil file. When a sector is mapped to nilFileIndex, it means we don't know the location for this sector yet
)

var (
	diskPattern = regexp.MustCompile(`volume-head-(\d)+.img`)
)

type Replica struct {
	sync.RWMutex
	ctx             context.Context
	volume          diffDisk
	dir             string
	info            Info
	diskData        map[string]*disk
	diskChildrenMap map[string]map[string]bool
	// activeDiskData is in grandparent, parent, child, etc order.
	// index 0 is nil or backing file and index n-1 is the active write layer
	activeDiskData []*disk
	readOnly       bool

	revisionCache           atomic.Int64
	revisionFile            *sparse.DirectFileIoProcessor
	revisionCounterDisabled bool
	revisionCounterReqChan  chan bool
	revisionCounterAckChan  chan error

	unmapMarkDiskChainRemoved bool

	snapshotMaxCount int
	snapshotMaxSize  int64
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
	snapshotDiskName := diskutil.GenerateSnapshotDiskName(snapshotName)
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

	r, err := NewReadOnly(context.Background(), dir, snapshotDiskName, backingFile)
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

func New(ctx context.Context, size, sectorSize int64, dir string, backingFile *backingfile.BackingFile, disableRevCounter, unmapMarkDiskChainRemoved bool, snapshotMaxCount int, SnapshotMaxSize int64) (*Replica, error) {
	return construct(ctx, false, size, sectorSize, dir, "", backingFile, disableRevCounter, unmapMarkDiskChainRemoved, snapshotMaxCount, SnapshotMaxSize)
}

func NewReadOnly(ctx context.Context, dir, head string, backingFile *backingfile.BackingFile) (*Replica, error) {
	// size and sectorSize don't matter because they will be read from metadata
	// snapshotMaxCount and SnapshotMaxSize don't matter because readonly replica can't create a new disk
	return construct(ctx, true, 0, diskutil.ReplicaSectorSize, dir, head, backingFile, false, false, types.MaximumTotalSnapshotCount, 0)
}

func construct(ctx context.Context, readonly bool, size, sectorSize int64, dir, head string, backingFile *backingfile.BackingFile, disableRevCounter, unmapMarkDiskChainRemoved bool, snapshotMaxCount int, snapshotMaxSize int64) (*Replica, error) {
	if size%sectorSize != 0 {
		return nil, fmt.Errorf("size %d not a multiple of sector size %d", size, sectorSize)
	}

	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	r := &Replica{
		ctx:                       ctx,
		dir:                       dir,
		activeDiskData:            make([]*disk, 1),
		diskData:                  make(map[string]*disk),
		diskChildrenMap:           map[string]map[string]bool{},
		readOnly:                  readonly,
		revisionCounterDisabled:   disableRevCounter,
		revisionCounterReqChan:    make(chan bool, 1024), // Buffered channel to avoid blocking
		revisionCounterAckChan:    make(chan error),
		unmapMarkDiskChainRemoved: unmapMarkDiskChainRemoved,
		snapshotMaxCount:          snapshotMaxCount,
		snapshotMaxSize:           snapshotMaxSize,
	}
	r.info.Size = size
	r.info.SectorSize = sectorSize
	r.volume.sectorSize = diskutil.VolumeSectorSize

	// Try to recover volume metafile if deleted or empty.
	if err := r.tryRecoverVolumeMetaFile(head); err != nil {
		return nil, err
	}

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
		if err := r.initRevisionCounter(ctx); err != nil {
			return nil, err
		}
	}

	// Reference r.info.Size because it may have changed from reading
	// metadata
	locationSize := r.info.Size / r.volume.sectorSize
	if size%diskutil.VolumeSectorSize != 0 {
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

func (r *Replica) diskPath(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return path.Join(r.dir, name)
}

func (r *Replica) isExtentSupported() error {
	filePath := r.diskPath(r.info.Head + diskutil.DiskMetadataSuffix)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	defer func() {
		if errClose := file.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close file %v", filePath)
		}
	}()

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
	newReplica, err := New(r.ctx, r.info.Size, r.info.SectorSize, r.dir, r.info.BackingFile, r.revisionCounterDisabled, r.unmapMarkDiskChainRemoved, r.snapshotMaxCount, r.snapshotMaxSize)
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
		return fmt.Errorf("cannot delete the active differencing disk")
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
		disk = diskutil.GenerateSnapshotDiskName(name)
		_, exists = r.diskData[disk]
		if !exists {
			logrus.Infof("Disk %v cannot be found, may has already been removed", disk)
			return nil
		}
	}

	if disk == r.info.Head {
		return fmt.Errorf("cannot mark the active differencing disk as removed")
	}

	if err := r.markDiskAsRemoved(disk); err != nil {
		return errors.Wrapf(err, "failed to mark disk %v as removed", disk)
	}

	return nil
}

func (r *Replica) hardlinkDisk(target, source string) error {
	if _, err := os.Stat(r.diskPath(source)); err != nil {
		return fmt.Errorf("cannot find source of replacing: %v", source)
	}

	if _, err := os.Stat(r.diskPath(target)); err == nil {
		logrus.Infof("Removing old file %s", target)
		if err := os.Remove(r.diskPath(target)); err != nil {
			return errors.Wrapf(err, "failed to remove %s", target)
		}
	}

	logrus.Infof("Hard linking %v to %v", source, target)
	if err := os.Link(r.diskPath(source), r.diskPath(target)); err != nil {
		return fmt.Errorf("failed to hard link %s to %s", source, target)
	}
	return nil
}

func (r *Replica) ReplaceDisk(target, source string) error {
	r.Lock()
	defer r.Unlock()

	if target == r.info.Head {
		return fmt.Errorf("cannot replace the active differencing disk")
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

	logrus.Infof("Done replacing disk %v with %v", source, target)

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
			return fmt.Errorf("cannot remove snapshot %v with %v children",
				name, len(children))
		}
		logrus.Warnf("Force delete disk %v with multiple children. Randomly choose a child to inherit", name)
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
		disk = diskutil.GenerateSnapshotDiskName(name)
		data, exists = r.diskData[disk]
		if !exists {
			logrus.Infof("Disk %v cannot be found, may has already been removed", disk)
			return nil, nil
		}
	}

	if disk == r.info.Head {
		return nil, fmt.Errorf("cannot delete the active differencing disk")
	}

	if !data.Removed {
		return nil, fmt.Errorf("disk %v hasn't been marked as removed", disk)
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
		return nil, fmt.Errorf("wrong disk %v doesn't exist", disk)
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
			return nil, fmt.Errorf("failed to find metadata for %s", cur)
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
			return nil, fmt.Errorf("failed to find metadata for %s", cur)
		}
		cur = r.diskData[cur].Parent
	}

	return result, nil
}

func (r *Replica) GetSnapshotSizeUsage() int64 {
	r.RLock()
	defer r.RUnlock()

	var (
		backingDiskName   string
		totalSnapshotSize int64
	)
	// index 0 is nil or backing file
	if r.activeDiskData[0] != nil {
		backingDiskName = r.activeDiskData[0].Name
	}
	for _, disk := range r.activeDiskData {
		// exclude volume head, backing disk, and removed disks
		if disk == nil || disk.Removed || disk.Name == backingDiskName || disk.Name == r.info.Head {
			continue
		}
		totalSnapshotSize += r.getDiskSize(disk.Name)
	}
	return totalSnapshotSize
}

func (r *Replica) writeVolumeMetaData(dirty, rebuilding bool) error {
	info := r.info
	info.Dirty = dirty
	info.Rebuilding = rebuilding
	_, err := r.encodeToFile(&info, volumeMetaData)
	return err
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
			if errClose := f.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close file")
			}
		}
	}
}

func (r *Replica) close() error {
	r.closeWithoutWritingMetaData()
	return r.writeVolumeMetaData(false, r.info.Rebuilding)
}

func (r *Replica) encodeToFile(obj interface{}, file string) (rollbackFunc func() error, err error) {
	if r.readOnly {
		return nil, nil
	}

	tmpFileName := fmt.Sprintf("%s%s", file, tmpFileSuffix)

	defer func() {
		// This rollback function will be executed either after the current function errors out,
		// or by the upper layer when the current function succeeds but the subsequent executions fail.
		//
		// It allows the upper caller to be atomic:
		// the upper layer either succeeds to execute all functions,
		// or fails in the middle then does rollback for the previous succeeded parts so that everything looks like unchanged.
		rollbackFunc = func() error {
			return os.RemoveAll(r.diskPath(tmpFileName))
		}

		if err != nil {
			err = types.GenerateFunctionErrorWithRollback(err, rollbackFunc())
			rollbackFunc = nil
		}
	}()

	f, err := os.Create(r.diskPath(tmpFileName))
	if err != nil {
		return rollbackFunc, errors.Wrapf(err, "failed to create the tmp file for file %v", r.diskPath(file))
	}

	if err := json.NewEncoder(f).Encode(&obj); err != nil {
		return rollbackFunc, err
	}

	if err := f.Close(); err != nil {
		return rollbackFunc, err
	}

	return rollbackFunc, os.Rename(r.diskPath(tmpFileName), r.diskPath(file))
}

func (r *Replica) nextFile(parsePattern *regexp.Regexp, pattern, parent string) (string, error) {
	if parent == "" {
		return fmt.Sprintf(pattern, 0), nil
	}

	matches := parsePattern.FindStringSubmatch(parent)
	if matches == nil {
		return "", fmt.Errorf("invalid name %s does not match pattern: %v", parent, parsePattern)
	}

	index, _ := strconv.Atoi(matches[1])
	return fmt.Sprintf(pattern, index+1), nil
}

func (r *Replica) openFile(name string, flag int) (types.DiffDisk, error) {
	return sparse.NewDirectFileIoProcessor(r.diskPath(name), os.O_RDWR|flag, 06666, true)
}

func (r *Replica) createNewHead(oldHead, parent, created string, size int64) (f types.DiffDisk, newDisk disk, rollbackFunc func() error, err error) {
	newHeadName, err := r.nextFile(diskPattern, diskutil.VolumeHeadDiskName, oldHead)
	if err != nil {
		return nil, disk{}, nil, err
	}

	if _, err := os.Stat(r.diskPath(newHeadName)); err == nil {
		return nil, disk{}, nil, fmt.Errorf("%s already exists", newHeadName)
	}

	f, err = r.openFile(r.diskPath(newHeadName), os.O_TRUNC)
	if err != nil {
		return nil, disk{}, nil, err
	}

	var subRollbackFunc func() error
	defer func() {
		// This rollback function will be executed either after the current function errors out,
		// or by the upper layer when the current function succeeds but the subsequent executions fail.
		//
		// It allows the upper caller to be atomic:
		// the upper layer either succeeds to execute all functions,
		// or fails in the middle then does rollback for the previous succeeded parts so that everything looks like unchanged.
		rollbackFunc = func() error {
			if errClose := f.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close file")
			}
			if subRollbackFunc != nil {
				return types.CombineErrors(subRollbackFunc(), r.rmDisk(newHeadName))
			}
			return r.rmDisk(newHeadName)
		}

		if err != nil {
			err = types.GenerateFunctionErrorWithRollback(err, rollbackFunc())
			rollbackFunc = nil
		}
	}()

	if err := syscall.Truncate(r.diskPath(newHeadName), size); err != nil {
		return nil, disk{}, rollbackFunc, err
	}

	newDisk = disk{
		Parent:      parent,
		Name:        newHeadName,
		Removed:     false,
		UserCreated: false,
		Created:     created,
	}
	subRollbackFunc, err = r.encodeToFile(&newDisk, newHeadName+diskutil.DiskMetadataSuffix)
	return f, newDisk, rollbackFunc, err
}

func (r *Replica) linkDisk(oldName, newName string) (rollbackFunc func() error, err error) {
	if oldName == "" {
		return nil, nil
	}

	defer func() {
		if err != nil {
			return
		}

		// This rollback function will be executed either after the current function errors out,
		// or by the upper layer when the current function succeeds but the subsequent executions fail.
		//
		// It allows the upper caller to be atomic:
		// the upper layer either succeeds to execute all functions,
		// or fails in the middle then does rollback for the previous succeeded parts so that everything looks like unchanged.
		rollbackFunc = func() error {
			return r.rmDisk(newName)
		}
	}()

	destMetadata := r.diskPath(newName + diskutil.DiskMetadataSuffix)
	logrus.Infof("Cleaning up new disk metadata file path %v before linking", destMetadata)
	if err := os.RemoveAll(destMetadata); err != nil {
		return rollbackFunc, errors.Wrapf(err, "failed to clean up new disk metadata file %v before linking", destMetadata)
	}

	destChecksum := r.diskPath(newName + diskutil.DiskChecksumSuffix)
	logrus.Infof("Cleaning up new disk checksum file %v before linking", destChecksum)
	if err := os.RemoveAll(destChecksum); err != nil {
		return rollbackFunc, errors.Wrapf(err, "failed to clean up new disk checksum file %v before linking", destChecksum)
	}

	dest := r.diskPath(newName)
	logrus.Infof("Cleaning up new disk file %v before linking", dest)
	if err := os.RemoveAll(dest); err != nil {
		return rollbackFunc, errors.Wrapf(err, "failed to clean up new disk file %v before linking", dest)
	}

	defer func() {
		if err != nil {
			err = types.GenerateFunctionErrorWithRollback(err, r.rmDisk(newName))
		}
	}()

	if err := os.Link(r.diskPath(oldName), dest); err != nil {
		return rollbackFunc, err
	}

	// Typically, this function links an old volume head to a new snapshot. And the volume head does not contain a checksum file.
	// Hence there is no need to link the checksum file here.

	return rollbackFunc, os.Link(r.diskPath(oldName+diskutil.DiskMetadataSuffix), r.diskPath(newName+diskutil.DiskMetadataSuffix))
}

func (r *Replica) markDiskAsRemoved(name string) error {
	disk, ok := r.diskData[name]
	if !ok {
		return fmt.Errorf("cannot find disk %v", name)
	}
	if stat, err := os.Stat(r.diskPath(name)); err != nil || stat.IsDir() {
		return fmt.Errorf("cannot find disk file %v", name)
	}
	if stat, err := os.Stat(r.diskPath(name + diskutil.DiskMetadataSuffix)); err != nil || stat.IsDir() {
		return fmt.Errorf("cannot find disk metafile %v", name+diskutil.DiskMetadataSuffix)
	}
	disk.Removed = true
	r.diskData[name] = disk
	_, err := r.encodeToFile(disk, name+diskutil.DiskMetadataSuffix)
	return err
}

func (r *Replica) rmDisk(name string) error {
	if name == "" {
		return nil
	}

	logrus.Infof("Removing disk %v", name)

	diskPath := r.diskPath(name)
	lastErr := os.RemoveAll(diskPath)
	if lastErr != nil {
		logrus.WithError(lastErr).Errorf("Failed to remove disk file %v", diskPath)
	}
	diskMetaPath := r.diskPath(name + diskutil.DiskMetadataSuffix)
	if err := os.RemoveAll(diskMetaPath); err != nil {
		lastErr = err
		logrus.WithError(lastErr).Errorf("Failed to remove disk metadata file %v", diskMetaPath)
	}
	diskChecksumPath := r.diskPath(name + diskutil.DiskChecksumSuffix)
	if err := os.RemoveAll(diskChecksumPath); err != nil {
		lastErr = err
		logrus.WithError(lastErr).Errorf("Failed to remove disk checksum file %v", diskChecksumPath)
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
	f, newHeadDisk, _, err := r.createNewHead(oldHead, parentDiskFileName, created, r.info.Size)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close file")
		}
	}()

	info := r.info
	info.Head = newHeadDisk.Name
	info.Dirty = true
	info.Parent = newHeadDisk.Parent

	if _, err := r.encodeToFile(&info, volumeMetaData); err != nil {
		if _, err = r.encodeToFile(&r.info, volumeMetaData); err != nil {
			return nil, err
		}
		return nil, err
	}

	// Need to execute before r.Reload() to update r.diskChildrenMap
	if err = r.rmDisk(oldHead); err != nil {
		return nil, err
	}

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
		return fmt.Errorf("cannot create disk on read-only replica")
	}

	if r.getRemainSnapshotCounts() <= 0 {
		return fmt.Errorf("too many active disks: %v", len(r.activeDiskData)-2+1)
	}

	oldHead := r.info.Head
	newSnapName := diskutil.GenerateSnapshotDiskName(name)

	if oldHead == "" {
		newSnapName = ""
	}

	if newSnapName != "" {
		if _, ok := r.diskData[newSnapName]; ok {
			return fmt.Errorf("snapshot %v is already existing", newSnapName)
		}
	}

	rollbackFuncList := []func() error{}
	defer func() {
		if err == nil {
			if errRm := r.rmDisk(oldHead); errRm != nil {
				logrus.WithError(errRm).Warnf("Failed to remove old head %v", oldHead)
			}
			return
		}

		var rollbackErr error
		log.WithError(err).Errorf("Failed to create disk %v, will do rollback", name)
		for _, rollbackFunc := range rollbackFuncList {
			if rollbackFunc == nil {
				continue
			}
			rollbackErr = types.CombineErrors(rollbackErr, rollbackFunc())
		}
		err = types.WrapError(
			types.GenerateFunctionErrorWithRollback(err, rollbackErr),
			"failed to create new disk %v", name)
	}()

	f, newHeadDisk, createNewHeadRollbackFunc, err := r.createNewHead(oldHead, newSnapName, created, size)
	if err != nil {
		return err
	}
	rollbackFuncList = append(rollbackFuncList, createNewHeadRollbackFunc)

	linkDiskRollbackFunc, err := r.linkDisk(r.info.Head, newSnapName)
	if err != nil {
		return err
	}
	rollbackFuncList = append(rollbackFuncList, linkDiskRollbackFunc)

	info := r.info
	info.Head = newHeadDisk.Name
	info.Dirty = true
	info.Parent = newSnapName
	info.Size = size

	volumeMetaEncodeRollbackFunc, err := r.encodeToFile(&info, volumeMetaData)
	if err != nil {
		return err
	}
	rollbackFuncList = append(rollbackFuncList, func() error {
		_, err := r.encodeToFile(&r.info, volumeMetaData)
		return types.CombineErrors(volumeMetaEncodeRollbackFunc(), err)
	})

	r.diskData[newHeadDisk.Name] = &newHeadDisk
	if newSnapName != "" {
		r.addChildDisk(newSnapName, newHeadDisk.Name)
		r.diskData[newSnapName] = r.diskData[oldHead]
		r.diskData[newSnapName].UserCreated = userCreated
		r.diskData[newSnapName].Created = created
		r.diskData[newSnapName].Labels = labels
		rollbackFuncList = append(rollbackFuncList, func() error {
			delete(r.diskData, newHeadDisk.Name)
			delete(r.diskData, newSnapName)
			delete(r.diskChildrenMap, newSnapName)
			return nil
		})

		snapMetaEncodeRollbackFunc, err := r.encodeToFile(r.diskData[newSnapName], newSnapName+diskutil.DiskMetadataSuffix)
		if err != nil {
			return err
		}
		rollbackFuncList = append(rollbackFuncList, snapMetaEncodeRollbackFunc)

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
	_, err := r.encodeToFile(child, child.Name+diskutil.DiskMetadataSuffix)
	return err
}

func (r *Replica) openLiveChain() error {
	chain, err := r.Chain()
	if err != nil {
		return err
	}

	if len(chain) > types.MaximumTotalSnapshotCount {
		return fmt.Errorf("live chain is too long: %v", len(chain))
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

	files, err := os.ReadDir(r.dir)
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
			r.volume.sectorSize = diskutil.VolumeSectorSize
			r.volume.size = r.info.Size
		} else if strings.HasSuffix(file.Name(), diskutil.DiskMetadataSuffix) {
			if err := r.readDiskData(file.Name()); err != nil {
				return false, err
			}
		}
	}

	return len(r.diskData) > 0, nil
}

func (r *Replica) tryRecoverVolumeMetaFile(head string) error {
	valid, err := r.checkValidVolumeMetaData()
	if err != nil {
		return err
	}
	if valid {
		return nil
	}

	if head != "" {
		r.info.Head = head
	}

	if r.info.Head == "" {
		files, err := os.ReadDir(r.dir)
		if err != nil {
			return err
		}
		for _, file := range files {
			if strings.Contains(file.Name(), types.VolumeHeadName) {
				r.info.Head = file.Name()
				break
			}
		}
	}

	logrus.Warnf("Recovering volume metafile %v, and replica info: %+v", r.diskPath(volumeMetaData), r.info)
	return r.writeVolumeMetaData(true, r.info.Rebuilding)
}

func (r *Replica) checkValidVolumeMetaData() (bool, error) {
	err := r.unmarshalFile(r.diskPath(volumeMetaData), &r.info)
	if err == nil {
		return true, nil
	}

	// recover metadata file that does not exist or is empty
	if os.IsNotExist(err) || errors.Is(err, io.EOF) {
		return false, nil
	}

	return false, err
}

func (r *Replica) readDiskData(file string) error {
	var data disk
	if err := r.unmarshalFile(file, &data); err != nil {
		return err
	}

	name := file[:len(file)-len(diskutil.DiskMetadataSuffix)]
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
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close file %v", p)
		}
	}()

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
			if err := r.rmDisk(name); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{"disk": name}).Warn("Failed to remove disk")
			}
		}
	}

	if err := os.Remove(r.diskPath(volumeMetaData)); err != nil {
		logrus.WithError(err).Warnf("Failed to remove %s", volumeMetaData)
	}
	if err := os.Remove(r.diskPath(revisionCounterFile)); err != nil {
		logrus.WithError(err).Warnf("Failed to remove %s", revisionCounterFile)
	}
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

func (r *Replica) GetUnmapMarkDiskChainRemoved() bool {
	r.RLock()
	defer r.RUnlock()

	return r.unmapMarkDiskChainRemoved
}

func (r *Replica) SetUnmapMarkDiskChainRemoved(enabled bool) {
	r.Lock()
	defer r.Unlock()

	r.unmapMarkDiskChainRemoved = enabled

	logrus.Infof("Set replica flag unmapMarkDiskChainRemoved to %v", enabled)
}

func (r *Replica) SetSnapshotMaxCount(count int) {
	r.Lock()
	defer r.Unlock()

	r.snapshotMaxCount = count

	logrus.Infof("Set replica flag snapshotMaxCount to %d", count)
}

func (r *Replica) SetSnapshotMaxSize(size int64) {
	r.Lock()
	defer r.Unlock()

	r.snapshotMaxSize = size

	logrus.Infof("Set replica flag SnapshotMaxSize to %d", size)
}

func (r *Replica) Expand(size int64) (err error) {
	if size%diskutil.VolumeSectorSize != 0 {
		return fmt.Errorf("failed to expend volume replica size %v, because it is not multiple of volume sector size %v", size, diskutil.VolumeSectorSize)
	}

	r.Lock()
	defer r.Unlock()

	if r.info.Size > size {
		return fmt.Errorf("cannot expand replica to a smaller size %v", size)
	} else if r.info.Size == size {
		logrus.Infof("Replica had been expanded to size %v", size)
		return nil
	}

	// Will create a new head with the expanded size and write the new size into the meta file
	if err := r.createDisk(
		diskutil.GenerateExpansionSnapshotName(size), false, util.Now(),
		diskutil.GenerateExpansionSnapshotLabels(size), size); err != nil {
		return err
	}
	r.volume.Expand(size)

	return nil
}

func (r *Replica) WriteAt(buf []byte, offset int64) (int, error) {
	if r.readOnly {
		return 0, fmt.Errorf("cannot write on read-only replica")
	}

	// Increase the revision counter optimistically in a separate goroutine since most of the time write operations will succeed.
	// Once the write operation fails, the revision counter will be wrongly increased by 1. It means that the revision counter is not accurate.
	// Actually, the revision counter is not accurate even without this optimistic increment since we cannot make data write operation and the revision counter increment atomic.
	go func() {
		if !r.revisionCounterDisabled {
			r.revisionCounterReqChan <- true
		}
	}()

	r.RLock()
	r.info.Dirty = true
	c, err := r.volume.WriteAt(buf, offset)
	r.RUnlock()
	if err != nil {
		return c, err
	}

	if !r.revisionCounterDisabled {
		err = <-r.revisionCounterAckChan
	}

	return c, err
}

func (r *Replica) ReadAt(buf []byte, offset int64) (int, error) {
	r.RLock()
	c, err := r.volume.ReadAt(buf, offset)
	r.RUnlock()
	return c, err
}

func (r *Replica) UnmapAt(length uint32, offset int64) (n int, err error) {
	defer func() {
		if err != nil {
			logrus.WithError(err).Errorf("Replica with dir %v failed to do unmap with offset %v and length %v", r.dir, offset, length)
		}
	}()
	if r.readOnly {
		return 0, fmt.Errorf("can not unmap on read-only replica")
	}

	go func() {
		if !r.revisionCounterDisabled {
			r.revisionCounterReqChan <- true
		}
	}()

	unmappedSize, err := func() (int, error) {
		r.Lock()
		defer r.Unlock()

		// Figure out the disk chain that can be unmapped.
		// The chain starts from the volume head,
		// and is followed by continuous removed disks.
		// For list `unmappableDisks`, the first entry is the volume head,
		// the second one is the parent of the first entry...
		unmappableDisks := []string{r.diskPath(r.activeDiskData[len(r.activeDiskData)-1].Name)}
		indexOfVolumeHeadParent := len(r.activeDiskData) - 2
		indexOfLastSnapshotDiskFile := 1
		if r.isBackingFile(int(backingFileIndex)) {
			indexOfLastSnapshotDiskFile = int(backingFileIndex) + 1
		}
		for idx := indexOfVolumeHeadParent; idx >= indexOfLastSnapshotDiskFile; idx-- {
			disk := r.activeDiskData[idx]
			if len(r.diskChildrenMap[disk.Name]) > 1 {
				break
			}
			if r.unmapMarkDiskChainRemoved {
				if err := r.markDiskAsRemoved(disk.Name); err != nil {
					return 0, errors.Wrapf(err, "failed to mark snapshot disk %v as removed before unmap", disk.Name)
				}
			}
			if disk.Removed {
				unmappableDisks = append(unmappableDisks, r.diskPath(disk.Name))
			}
		}
		r.info.Dirty = true
		return r.volume.UnmapAt(unmappableDisks, length, offset)
	}()
	if err != nil {
		return 0, err
	}

	if !r.revisionCounterDisabled {
		err = <-r.revisionCounterAckChan
	}

	return unmappedSize, err
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

func (r *Replica) GetSnapshotCount() (int, int) {
	r.RLock()
	defer r.RUnlock()

	return r.getSnapshotCountUsage(), r.getSnapshotCountTotal()
}

func (r *Replica) getSnapshotCountTotal() int {
	return len(r.diskData)
}

func (r *Replica) getSnapshotCountUsage() int {
	var (
		backingDiskName string
		snapshotCount   int
	)
	// index 0 is nil or backing file
	if r.activeDiskData[0] != nil {
		backingDiskName = r.activeDiskData[0].Name
	}

	for _, disk := range r.diskData {
		// exclude volume head, backing disk, and removed disks
		if disk == nil || disk.Removed || disk.Name == backingDiskName || disk.Name == r.info.Head {
			continue
		}
		snapshotCount++
	}
	return snapshotCount
}

func (r *Replica) GetRemainSnapshotCounts() int {
	r.RLock()
	defer r.RUnlock()

	return r.getRemainSnapshotCounts()
}

func (r *Replica) getRemainSnapshotCounts() int {
	return r.snapshotMaxCount - r.getSnapshotCountUsage()
}

func (r *Replica) getDiskSize(disk string) int64 {
	ret := util.GetFileActualSize(r.diskPath(disk))
	if ret == -1 {
		errMessage := fmt.Sprintf("Failed to get file %v size", r.diskPath(disk))
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
	isThereBackingFile := r.info.BackingFile != nil
	return r.volume.preload(isThereBackingFile)
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
