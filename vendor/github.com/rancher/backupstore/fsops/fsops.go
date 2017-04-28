package fsops

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rancher/backupstore"
	"github.com/rancher/backupstore/util"
)

const (
	MaxCleanupLevel = 10
)

type FileSystemOps interface {
	LocalPath(path string) string
}

type FileSystemOperator struct {
	FileSystemOps
}

func NewFileSystemOperator(ops FileSystemOps) *FileSystemOperator {
	return &FileSystemOperator{ops}
}

func (f *FileSystemOperator) preparePath(file string) error {
	if err := os.MkdirAll(filepath.Dir(f.LocalPath(file)), os.ModeDir|0700); err != nil {
		return err
	}
	return nil
}

func (f *FileSystemOperator) FileSize(filePath string) int64 {
	file := f.LocalPath(filePath)
	st, err := os.Stat(file)
	if err != nil || st.IsDir() {
		return -1
	}
	return st.Size()
}

func (f *FileSystemOperator) FileExists(filePath string) bool {
	return f.FileSize(filePath) >= 0
}

func (f *FileSystemOperator) Remove(names ...string) error {
	for _, name := range names {
		if err := os.RemoveAll(f.LocalPath(name)); err != nil {
			return err
		}
		//Also automatically cleanup upper level directories
		dir := f.LocalPath(name)
		for i := 0; i < MaxCleanupLevel; i++ {
			dir = filepath.Dir(dir)
			// Don't clean above backupstore base
			if strings.HasSuffix(dir, backupstore.GetBackupstoreBase()) {
				break
			}
			// If directory is not empty, then we don't need to continue
			if err := os.Remove(dir); err != nil {
				break
			}
		}
	}
	return nil
}

func (f *FileSystemOperator) Read(src string) (io.ReadCloser, error) {
	file, err := os.Open(f.LocalPath(src))
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *FileSystemOperator) Write(dst string, rs io.ReadSeeker) error {
	tmpFile := dst + ".tmp"
	if f.FileExists(tmpFile) {
		f.Remove(tmpFile)
	}
	if err := f.preparePath(dst); err != nil {
		return err
	}
	file, err := os.Create(f.LocalPath(tmpFile))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, rs)
	if err != nil {
		return err
	}

	if f.FileExists(dst) {
		f.Remove(dst)
	}
	return os.Rename(f.LocalPath(tmpFile), f.LocalPath(dst))
}

func (f *FileSystemOperator) List(path string) ([]string, error) {
	out, err := util.Execute("ls", []string{"-1", f.LocalPath(path)})
	if err != nil {
		return nil, err
	}
	var result []string
	if len(out) == 0 {
		return result, nil
	}
	result = strings.Split(strings.TrimSpace(string(out)), "\n")
	return result, nil
}

func (f *FileSystemOperator) Upload(src, dst string) error {
	tmpDst := dst + ".tmp"
	if f.FileExists(tmpDst) {
		f.Remove(tmpDst)
	}
	if err := f.preparePath(dst); err != nil {
		return err
	}
	_, err := util.Execute("cp", []string{src, f.LocalPath(tmpDst)})
	if err != nil {
		return err
	}
	_, err = util.Execute("mv", []string{f.LocalPath(tmpDst), f.LocalPath(dst)})
	if err != nil {
		return err
	}
	return nil
}

func (f *FileSystemOperator) Download(src, dst string) error {
	_, err := util.Execute("cp", []string{f.LocalPath(src), dst})
	if err != nil {
		return err
	}
	return nil
}
