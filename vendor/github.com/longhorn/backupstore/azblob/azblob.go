package azblob

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "azblob"})
)

// BackupStoreDriver defines the variables and method that backupstore will use.
type BackupStoreDriver struct {
	destURL string
	path    string
	service *service
}

const (
	// KIND defines the kind of backupstore driver
	KIND = "azblob"
)

func init() {
	if err := backupstore.RegisterDriver(KIND, initFunc); err != nil {
		panic(err)
	}
}

func initFunc(destURL string) (backupstore.BackupStoreDriver, error) {
	u, err := url.Parse(destURL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != KIND {
		return nil, fmt.Errorf("wrong driver dispatching %v to %v?", u.Scheme, KIND)
	}

	b := &BackupStoreDriver{}
	b.service, err = newService(u)
	if err != nil {
		return nil, err
	}

	b.path = u.Path
	if b.service.Container == "" || b.path == "" {
		return nil, fmt.Errorf("invalid URL. Must be either azblob://container@serviceurl/path/, or azblob://container/path")
	}

	b.path = strings.TrimLeft(b.path, "/")

	if _, err := b.List(""); err != nil {
		return nil, err
	}

	b.destURL = KIND + "://" + b.service.Container
	if b.service.EndpointSuffix != "" {
		b.destURL += "@" + b.service.EndpointSuffix
	}
	b.destURL += "/" + b.path

	log.Infof("Loaded driver for %v", b.destURL)
	return b, nil
}

// Kind returns the driver type
func (s *BackupStoreDriver) Kind() string {
	return KIND
}

// GetURL returns URL of the backup target
func (s *BackupStoreDriver) GetURL() string {
	return s.destURL
}

func (s *BackupStoreDriver) updatePath(path string) string {
	return filepath.Join(s.path, path)
}

// List return items that on the backup target including prefixes
func (s *BackupStoreDriver) List(listPath string) ([]string, error) {
	var result []string

	path := s.updatePath(listPath) + "/"
	contents, err := s.service.listBlobs(path, "/")
	if err != nil {
		return result, err
	}

	sizeC := len(*contents)
	if sizeC == 0 {
		return result, nil
	}

	result = []string{}
	for _, blob := range *contents {
		r := strings.TrimPrefix(blob, path)
		r = strings.TrimSuffix(r, "/")
		if r != "" {
			result = append(result, r)
		}
	}

	return result, nil
}

// FileExists checks if file exists on the backup target
func (s *BackupStoreDriver) FileExists(filePath string) bool {
	return s.FileSize(filePath) >= 0
}

// FileSize return content length of the filePath on the backup target
func (s *BackupStoreDriver) FileSize(filePath string) int64 {
	path := s.updatePath(filePath)
	head, err := s.service.getBlobProperties(path)
	if err != nil || head.ContentLength == nil {
		log.WithError(err).Errorf("Failed to get azblob properties: %v", path)
		return -1
	}
	return *head.ContentLength
}

// FileTime returns file last modified time on the backup target
func (s *BackupStoreDriver) FileTime(filePath string) time.Time {
	path := s.updatePath(filePath)
	blobProp, err := s.service.getBlobProperties(path)
	if err != nil || blobProp.ContentLength == nil {
		log.WithError(err).Errorf("Failed to get azblob properties: %v", path)
		return time.Time{}
	}
	return blobProp.LastModified.UTC()
}

// Remove deletes files on the backup target
func (s *BackupStoreDriver) Remove(path string) error {
	return s.service.deleteBlobs(s.updatePath(path))
}

func (s *BackupStoreDriver) Read(src string) (io.ReadCloser, error) {
	path := s.updatePath(src)
	rc, err := s.service.getBlob(path)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

// Write creates a item on the backup target from io stream
func (s *BackupStoreDriver) Write(dst string, rs io.ReadSeeker) error {
	path := s.updatePath(dst)
	return s.service.putBlob(path, rs)
}

// Upload creates a item on the backup target by opening source file
func (s *BackupStoreDriver) Upload(src, dst string) error {
	file, err := os.Open(src)
	if err != nil {
		log.WithError(err).Warnf("Failed to open file: %v", src)
		return nil
	}
	defer file.Close()
	path := s.updatePath(dst)
	return s.service.putBlob(path, file)
}

// Download gets a item data from the backup target
func (s *BackupStoreDriver) Download(src, dst string) error {
	if _, err := os.Stat(dst); err != nil {
		os.Remove(dst)
	}

	if err := os.MkdirAll(filepath.Dir(dst), os.ModeDir|0700); err != nil {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	path := s.updatePath(src)
	rc, err := s.service.getBlob(path)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, rc)
	return err
}
