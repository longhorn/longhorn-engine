package cifs

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	mount "k8s.io/mount-utils"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/backupstore/fsops"
	"github.com/longhorn/backupstore/util"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "cifs"})

	// Ref: https://github.com/longhorn/backupstore/pull/91
	defaultMountInterval = 1 * time.Second
	defaultMountTimeout  = 5 * time.Second
)

type BackupStoreDriver struct {
	destURL      string
	serverPath   string
	mountDir     string
	mountOptions []string

	username string
	password string

	*fsops.FileSystemOperator
}

const (
	KIND = "cifs"

	MaxCleanupLevel = 10
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
	if u.Host == "" {
		return nil, fmt.Errorf("CIFS path must follow format: cifs://<server-address>/<share-name>/")
	}
	if u.Path == "" {
		return nil, fmt.Errorf("cannot find CIFS path")
	}

	b.username = os.Getenv("CIFS_USERNAME")
	b.password = os.Getenv("CIFS_PASSWORD")
	b.serverPath = u.Host + u.Path
	b.destURL = KIND + "://" + b.serverPath
	b.mountDir = filepath.Join(util.MountDir, strings.TrimRight(strings.Replace(u.Host, ".", "_", -1), ":"), u.Path)

	cifsOptions, exist := u.Query()["cifsOptions"]
	if exist {
		b.mountOptions = util.SplitMountOptions(cifsOptions)
		log.Infof("Overriding CIFS mountOptions:  %v", b.mountOptions)
	} else {
		b.mountOptions = []string{"soft"}
	}

	if err := b.mount(); err != nil {
		return nil, errors.Wrapf(err, "cannot mount CIFS share %v, options %v", b.serverPath, b.mountOptions)
	}

	if _, err := b.List(""); err != nil {
		return nil, errors.Wrapf(err, "CIFS path %v doesn't exist or is not a directory", b.serverPath)
	}

	log.Infof("Loaded driver for %v", b.destURL)

	return b, nil
}

func (b *BackupStoreDriver) mount() error {
	mounter := mount.New("")

	mounted, err := util.EnsureMountPoint(KIND, b.mountDir, mounter, log)
	if err != nil {
		return err
	}
	if mounted {
		return nil
	}

	sensitiveMountOptions := []string{
		fmt.Sprintf("username=%v", b.username),
		fmt.Sprintf("password=%v", b.password),
	}

	log.Infof("Mounting CIFS share %v on mount point %v with options %+v", b.destURL, b.mountDir, b.mountOptions)

	return util.MountWithTimeout(mounter, "//"+b.serverPath, b.mountDir, KIND, b.mountOptions, sensitiveMountOptions,
		defaultMountInterval, defaultMountTimeout)
}

func (b *BackupStoreDriver) Kind() string {
	return KIND
}

func (b *BackupStoreDriver) GetURL() string {
	return b.destURL
}

func (b *BackupStoreDriver) LocalPath(path string) string {
	return filepath.Join(b.mountDir, path)
}
