package vfs

import (
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/fsops"
	"github.com/sirupsen/logrus"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "vfs"})
)

type BackupStoreDriver struct {
	destURL string
	path    string

	*fsops.FileSystemOperator
}

const (
	KIND = "vfs"

	VfsPath = "vfs.path"
)

func init() {
	if err := backupstore.RegisterDriver(KIND, initFunc); err != nil {
		panic(err)
	}
}

func initFunc(destURL string) (backupstore.BackupStoreDriver, error) {
	b := &BackupStoreDriver{}
	b.FileSystemOperator = fsops.NewFileSystemOperator(b)

	u, err := url.Parse(destURL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != KIND {
		return nil, fmt.Errorf("BUG: Why dispatch %v to %v?", u.Scheme, KIND)
	}

	if u.Host != "" {
		return nil, fmt.Errorf("VFS path must follow: vfs:///path/ format")
	}

	b.path = u.Path

	if b.path == "" {
		return nil, fmt.Errorf("Cannot find vfs path")
	}
	if _, err := b.List(""); err != nil {
		return nil, fmt.Errorf("VFS path %v doesn't exist or is not a directory", b.path)
	}

	b.destURL = KIND + "://" + b.path
	log.Debugf("Loaded driver for %v", b.destURL)
	return b, nil
}

func (v *BackupStoreDriver) LocalPath(path string) string {
	return filepath.Join(v.path, path)
}

func (v *BackupStoreDriver) Kind() string {
	return KIND
}

func (v *BackupStoreDriver) GetURL() string {
	return v.destURL
}
