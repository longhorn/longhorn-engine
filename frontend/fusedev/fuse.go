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
	lodev  string
	dev    string
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
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "fusedev"})
)

func start(name string, size, sectorSize int64, rw types.ReaderWriterAt) (*FuseFs, error) {
	fs := &FuseFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		Volume:     name,
		file: &FuseFile{
			File:       nodefs.NewDefaultFile(),
			Backend:    rw,
			Size:       size,
			SectorSize: sectorSize,
		},
	}
	newFs := pathfs.NewPathNodeFs(fs, nil)

	server, _, err := nodefs.MountRoot(fs.GetMountDir(), newFs.Root(), nil)
	/*
		conn := nodefs.NewFileSystemConnector(newFs.Root(), nil)
		opts := &fuse.MountOptions{
			MaxBackground: 12,
			Options:       []string{"direct_io"},
		}
		server, err := fuse.NewServer(conn.RawFS(), fs.GetMountDir(), opts)
	*/
	if err != nil {
		return nil, err
	}
	// This will be stopped when umount happens
	go server.Serve()

	if err := fs.createDev(); err != nil {
		return nil, err
	}
	return fs, nil

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

func (fs *FuseFs) createDev() error {
	if err := os.MkdirAll(DevPath, 0700); err != nil {
		log.Fatalln("Cannot create directory ", DevPath)
	}

	dev := filepath.Join(DevPath, fs.Volume)

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

	logrus.Infof("Attaching loopback device %s", fs.lodev)
	lodev, err := util.AttachLoopbackDevice(path, false)
	if err != nil {
		return err
	}

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
	fs.lodev = lodev
	fs.dev = dev
	return nil
}

func (fs *FuseFs) removeDev() error {
	if fs.dev == "" {
		return nil
	}

	logrus.Infof("Removing device %s", fs.dev)
	if _, err := os.Stat(fs.dev); err == nil {
		if err := remove(fs.dev); err != nil {
			return err
		}
	}
	fs.dev = ""

	logrus.Infof("Detaching loopback device %s", fs.lodev)
	if err := util.DetachLoopbackDevice(fs.getImageFileFullPath(), fs.lodev); err != nil {
		return err
	}
	fs.lodev = ""
	return nil
}

func (fs *FuseFs) umountFuse() error {
	if err := unix.Unmount(fs.GetMountDir(), 0); err != nil {
		return err
	}
	return nil
}

func (fs *FuseFs) GetMountDir() string {
	d := filepath.Join(MountBase, fs.Volume)
	if err := os.MkdirAll(d, 0700); err != nil {
		log.Fatal("Cannot create directory ", d)
	}
	return d
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
