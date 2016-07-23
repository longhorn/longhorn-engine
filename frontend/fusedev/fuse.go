package fusedev

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/rancher/convoy/util"
	"golang.org/x/sys/unix"

	"github.com/rancher/longhorn/types"
)

type FuseFs struct {
	pathfs.FileSystem
	Volume string
	file   *FuseFile
}

type FuseFile struct {
	nodefs.File
	Backend    types.ReaderWriterAt
	Size       int64
	SectorSize int64
}

const (
	ImageSuffix = ".img"
	DevPath     = "/dev/longhorn/"
	MountBase   = "/tmp/longhorn-fuse-mount"

	RetryCounts   = 5
	RetryInterval = 1 * time.Second
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "fusedev"})
)

func newFuseFs(name string, size, sectorSize int64, rw types.ReaderWriterAt) *FuseFs {
	return &FuseFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		Volume:     name,
		file: &FuseFile{
			File:       nodefs.NewDefaultFile(),
			Backend:    rw,
			Size:       size,
			SectorSize: sectorSize,
		},
	}
}

func (fs *FuseFs) Start() error {
	newFs := pathfs.NewPathNodeFs(fs, nil)

	mountDir := fs.GetMountDir()
	if err := os.MkdirAll(mountDir, 0700); err != nil {
		log.Fatal("Cannot create directory ", mountDir)
	}
	server, _, err := nodefs.MountRoot(mountDir, newFs.Root(), nil)
	/*
		conn := nodefs.NewFileSystemConnector(newFs.Root(), nil)
		opts := &fuse.MountOptions{
			MaxBackground: 12,
			Options:       []string{"direct_io"},
		}
		server, err := fuse.NewServer(conn.RawFS(), fs.GetMountDir(), opts)
	*/
	if err != nil {
		return err
	}
	// This will be stopped when umount happens
	go server.Serve()

	if err := fs.createDev(); err != nil {
		return err
	}
	return nil
}

func (fs *FuseFs) Stop() error {
	if err := fs.removeDev(); err != nil {
		return err
	}
	if err := fs.umountFuse(); err != nil {
		return err
	}
	return nil
}

func (fs *FuseFs) getDev() string {
	return filepath.Join(DevPath, fs.Volume)
}

func (fs *FuseFs) getLoopbackDev() (string, error) {
	devs, err := fs.getLoopbackDevs()
	if err != nil {
		return "", err
	}
	if len(devs) != 1 {
		return "", fmt.Errorf("Found more than one loopback devices %v", devs)
	}
	return devs[0], nil
}

func (fs *FuseFs) getLoopbackDevs() ([]string, error) {
	filePath := fs.getImageFileFullPath()
	devs, err := util.ListLoopbackDevice(filePath)
	if err != nil {
		return nil, err
	}
	return devs, nil
}

func (fs *FuseFs) createDev() error {
	if err := os.MkdirAll(DevPath, 0700); err != nil {
		log.Fatalln("Cannot create directory ", DevPath)
	}

	dev := fs.getDev()

	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	found := false
	path := fs.getImageFileFullPath()
	for i := 0; i < 30; i++ {
		var err error
		matches, err := filepath.Glob(path)
		if len(matches) > 0 && err == nil {
			found = true
			break
		}

		logrus.Infof("Waiting for %s", path)
		time.Sleep(1 * time.Second)
	}

	if !found {
		return fmt.Errorf("Fail to wait for %v", path)
	}

	logrus.Infof("Attaching loopback device")
	_, err := util.AttachLoopbackDevice(path, false)
	if err != nil {
		return err
	}
	lodev, err := fs.getLoopbackDev()
	if err != nil {
		return err
	}
	logrus.Infof("Attached loopback device %v", lodev)

	stat := unix.Stat_t{}
	if err := unix.Stat(lodev, &stat); err != nil {
		return err
	}
	major := int(stat.Rdev / 256)
	minor := int(stat.Rdev % 256)
	logrus.Infof("Creating device %s %d:%d", dev, major, minor)
	if err := mknod(dev, major, minor); err != nil {
		return err
	}
	return nil
}

func (fs *FuseFs) removeDev() error {
	dev := fs.getDev()
	logrus.Infof("Removing device %s", dev)
	if _, err := os.Stat(dev); err == nil {
		if err := remove(dev); err != nil {
			return fmt.Errorf("Failed to removing device %s, %v", dev, err)
		}
	}

	lodevs, err := fs.getLoopbackDevs()
	if err != nil {
		return fmt.Errorf("Failed to get loopback device %v", err)
	}
	for _, lodev := range lodevs {
		logrus.Infof("Detaching loopback device %s", lodev)
		detached := false
		devs := []string{}
		for i := 0; i < RetryCounts; i++ {
			var err error

			if err := util.DetachLoopbackDevice(fs.getImageFileFullPath(), lodev); err != nil {
				return fmt.Errorf("Fail to detach loopback device %s: %v", lodev, err)
			}

			devs, err = util.ListLoopbackDevice(fs.getImageFileFullPath())
			if err != nil {
				return err
			}
			if len(devs) == 0 {
				detached = true
				break
			}
			logrus.Infof("Waitting for detaching loopback device", devs)
			time.Sleep(RetryInterval)
		}
		if !detached {
			return fmt.Errorf("Loopback device busy, cannot detach device, devices %v remains", devs)
		}
	}
	return nil
}

func (fs *FuseFs) umountFuse() error {
	if fs.isMounted() {
		dir := fs.GetMountDir()
		logrus.Infof("Umounting %s", dir)
		if err := unix.Unmount(dir, 0); err != nil {
			return fmt.Errorf("Fail to umounting %s: %v", dir, err)
		}
	}
	return nil
}

func (fs *FuseFs) isMounted() bool {
	path := fs.GetMountDir()
	if _, err := util.Execute("findmnt", []string{path}); err != nil {
		return false
	}
	return true
}

func (fs *FuseFs) GetMountDir() string {
	return filepath.Join(MountBase, fs.Volume)
}

func (fs *FuseFs) getImageFileName() string {
	return fs.Volume + ImageSuffix
}

func (fs *FuseFs) getImageFileFullPath() string {
	return filepath.Join(fs.GetMountDir(), fs.getImageFileName())
}

func (fs *FuseFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	switch name {
	case fs.getImageFileName():
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0644, Size: uint64(fs.file.Size),
		}, fuse.OK
	case "":
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *FuseFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	if name == "" {
		return []fuse.DirEntry{{Name: fs.getImageFileName(), Mode: fuse.S_IFREG}}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *FuseFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	if name != fs.getImageFileName() {
		return nil, fuse.ENOENT
	}
	return fs.file, fuse.OK
}

func (f *FuseFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	l := int64(len(dest))
	if off+l > f.Size {
		l = f.Size - off
	}
	_, err := f.Backend.ReadAt(dest[:l], off)
	if err != nil {
		log.Errorln("read failed: ", err.Error())
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(dest[:l]), fuse.OK
}

func (f *FuseFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	l := int64(len(data))
	if off+l > f.Size {
		l = f.Size - off
	}

	written, err := f.Backend.WriteAt(data[:l], off)
	if err != nil {
		log.Errorln("write failed: ", err.Error())
		return 0, fuse.EIO
	}
	return uint32(written), fuse.OK
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0600
	fileMode |= unix.S_IFBLK
	dev := int((major << 8) | (minor & 0xff) | ((minor & 0xfff00) << 12))

	return unix.Mknod(device, uint32(fileMode), dev)
}

func removeAsync(path string, done chan<- error) {
	logrus.Infof("Removing: %s", path)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.Errorf("Unable to remove: %v", path)
		done <- err
	}
	logrus.Debugf("Removed: %s", path)
	done <- nil
}

func remove(path string) error {
	done := make(chan error)
	go removeAsync(path, done)
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("Timeout trying to delete %s.", path)
	}
}
