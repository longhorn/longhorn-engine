package fusedev

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/convoy/util"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/rancher/longhorn/types"
)

type LonghornFs struct {
	Conn    *fuse.Conn
	Volume  string
	rawFile *RawFrontendFile
}

type RawFrontendFile struct {
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

func newLonghornFs(name string, size, sectorSize int64, rw types.ReaderWriterAt) *LonghornFs {
	return &LonghornFs{
		Volume: name,
		rawFile: &RawFrontendFile{
			Backend:    rw,
			Size:       size,
			SectorSize: sectorSize,
		},
	}
}

func (lf *LonghornFs) Start() error {
	var err error
	mountDir := lf.GetMountDir()
	if err := os.MkdirAll(mountDir, 0700); err != nil {
		return fmt.Errorf("Cannot create directory %v", mountDir)
	}
	lf.Conn, err = fuse.Mount(mountDir,
		fuse.FSName("longhorn"),
		fuse.Subtype("longhornfs"),
		fuse.VolumeName(lf.Volume),
	)
	if err != nil {
		log.Fatal("FUSE: Unable to mount: ", err)
	}

	go lf.startFs()

	if err := lf.createDev(); err != nil {
		return err
	}
	return nil
}

func (lf *LonghornFs) startFs() {
	if err := fs.Serve(lf.Conn, lf); err != nil {
		// Likely caused by umount, so ignore it for now
		if err == unix.EBADF {
			log.Warnf("Serve return ", err)
		} else {
			log.Fatal("FUSE: Unable to serve: ", err)
		}
	}

	<-lf.Conn.Ready
	if err := lf.Conn.MountError; err != nil {
		log.Fatal("FUSE: Mount error occured: ", err)
	}
}

func (lf *LonghornFs) Stop() error {
	if err := lf.initStop(); err != nil {
		return err
	}
	return nil
}

func (lf *LonghornFs) initStop() error {
	if lf.Conn != nil {
		defer lf.Conn.Close()
	}
	if err := lf.removeDev(); err != nil {
		return err
	}
	if err := lf.umountFuse(); err != nil {
		return err
	}
	return nil
}

func (lf *LonghornFs) getDev() string {
	return filepath.Join(DevPath, lf.Volume)
}

func (lf *LonghornFs) getLoopbackDev() (string, error) {
	devs, err := lf.getLoopbackDevs()
	if err != nil {
		return "", err
	}
	if len(devs) != 1 {
		return "", fmt.Errorf("Found more than one loopback devices %v", devs)
	}
	return devs[0], nil
}

func (lf *LonghornFs) getLoopbackDevs() ([]string, error) {
	filePath := lf.getImageFileFullPath()
	devs, err := util.ListLoopbackDevice(filePath)
	if err != nil {
		return nil, err
	}
	return devs, nil
}

func (lf *LonghornFs) createDev() error {
	if err := os.MkdirAll(DevPath, 0700); err != nil {
		log.Fatalln("Cannot create directory ", DevPath)
	}

	dev := lf.getDev()

	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	found := false
	path := lf.getImageFileFullPath()
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
	lodev, err := lf.getLoopbackDev()
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

func (lf *LonghornFs) removeDev() error {
	dev := lf.getDev()
	logrus.Infof("Removing device %s", dev)
	if _, err := os.Stat(dev); err == nil {
		if err := remove(dev); err != nil {
			return fmt.Errorf("Failed to removing device %s, %v", dev, err)
		}
	}

	lodevs, err := lf.getLoopbackDevs()
	if err != nil {
		return fmt.Errorf("Failed to get loopback device %v", err)
	}
	for _, lodev := range lodevs {
		logrus.Infof("Detaching loopback device %s", lodev)
		if err := util.DetachLoopbackDevice(lf.getImageFileFullPath(), lodev); err != nil {
			return fmt.Errorf("Fail to detach loopback device %s: %v", lodev, err)
		}

		detached := false
		devs := []string{}
		for i := 0; i < RetryCounts; i++ {
			var err error
			devs, err = util.ListLoopbackDevice(lf.getImageFileFullPath())
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

func (lf *LonghornFs) umountFuse() error {
	if lf.isMounted() {
		dir := lf.GetMountDir()
		logrus.Infof("Umounting %s", dir)
		if err := unix.Unmount(dir, 0); err != nil {
			return fmt.Errorf("Fail to umounting %s: %v", dir, err)
		}
	}
	return nil
}

func (lf *LonghornFs) isMounted() bool {
	path := lf.GetMountDir()
	if _, err := util.Execute("findmnt", []string{path}); err != nil {
		return false
	}
	return true
}

func (lf *LonghornFs) GetMountDir() string {
	return filepath.Join(MountBase, lf.Volume)
}

func (lf *LonghornFs) getImageFileName() string {
	return lf.Volume + ImageSuffix
}

func (lf *LonghornFs) getImageFileFullPath() string {
	return filepath.Join(lf.GetMountDir(), lf.getImageFileName())
}

func (lf *LonghornFs) Root() (fs.Node, error) {
	return lf, nil
}

func (lf *LonghornFs) Attr(cxt context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
}

func (lf *LonghornFs) Lookup(cxt context.Context, name string) (fs.Node, error) {
	if name == lf.getImageFileName() {
		return lf.rawFile, nil
	}
	return nil, fuse.ENOENT
}

func (lf *LonghornFs) ReadDirAll(cxt context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{
		{Inode: 2, Name: lf.getImageFileName(), Type: fuse.DT_File},
	}, nil
}

func (f *RawFrontendFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 2
	a.Mode = 0444
	a.Size = uint64(f.Size)
	return nil
}

func (f *RawFrontendFile) Open(cxt context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return f, nil
}

func (f *RawFrontendFile) Read(cxt context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	l := int64(req.Size)
	if req.Offset+l > f.Size {
		l = f.Size - req.Offset
	}
	_, err := f.Backend.ReadAt(resp.Data[:l], req.Offset)
	if err != nil {
		return fmt.Errorf("read failed: %v", err)
	}
	resp.Data = resp.Data[:l]
	return nil
}

func (f *RawFrontendFile) Write(cxt context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	l := int64(len(req.Data))
	if req.Offset+l > f.Size {
		l = f.Size - req.Offset
	}

	_, err := f.Backend.WriteAt(req.Data[:l], req.Offset)
	if err != nil {
		return fmt.Errorf("write failed: %v", err)
	}
	resp.Size = int(l)
	return nil
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
