package file

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

func New() types.BackendFactory {
	return &Factory{}
}

type Factory struct {
}

type Wrapper struct {
	*os.File
	types.UnmapperAt
}

func (f *Wrapper) UnmapAt(length uint32, off int64) (int, error) {
	return 0, nil
}

func (f *Wrapper) Close() error {
	logrus.Infof("Closing: %s", f.Name())
	return f.File.Close()
}

func (f *Wrapper) Snapshot(name string, userCreated bool, created string, labels map[string]string) error {
	return nil
}

func (f *Wrapper) Expand(size int64) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(*types.Error); !ok {
				err = types.NewError(types.ErrorCodeFunctionFailedWithoutRollback,
					err.Error(), "")
			}
		}
	}()

	currentSize, err := f.Size()
	if err != nil {
		return err
	}
	if size < currentSize {
		return fmt.Errorf("cannot truncate to a smaller size %v for the backend type file", size)
	} else if size == currentSize {
		return nil
	}

	defer func() {
		if err != nil {
			err = types.WrapError(
				types.GenerateFunctionErrorWithRollback(err, f.Truncate(currentSize)),
				"failed to expand the file to size %v", size)
		}
	}()
	return f.Truncate(size)
}

func (f *Wrapper) Size() (int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (f *Wrapper) IsRevisionCounterDisabled() (bool, error) {
	return false, nil
}

func (f *Wrapper) GetLastModifyTime() (int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}

	return stat.ModTime().Unix(), nil
}

// GetHeadFileSize uses dummy head file size for file backend
func (f *Wrapper) GetHeadFileSize() (int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func (f *Wrapper) SectorSize() (int64, error) {
	return 4096, nil
}

func (f *Wrapper) RemainSnapshots() (int, error) {
	return 1, nil
}

func (f *Wrapper) GetRevisionCounter() (int64, error) {
	return 1, nil
}

func (f *Wrapper) SetRevisionCounter(counter int64) error {
	return nil
}

func (f *Wrapper) GetUnmapMarkSnapChainRemoved() (bool, error) {
	return false, nil
}

func (f *Wrapper) SetUnmapMarkSnapChainRemoved(enabled bool) error {
	return nil
}

func (f *Wrapper) ResetRebuild() error {
	return nil
}

func (ff *Factory) Create(volumeName, address string, dataServerProtocol types.DataServerProtocol, engineToReplicaTimeout time.Duration, nbdEnabled int) (types.Backend, error) {
	logrus.Infof("Creating file: %s", address)
	file, err := os.OpenFile(address, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		logrus.Infof("Failed to create file %s: %v", address, err)
		return nil, err
	}

	return &Wrapper{File: file}, nil
}

func (f *Wrapper) GetState() (string, error) {
	return "open", nil
}

func (f *Wrapper) GetMonitorChannel() types.MonitorChannel {
	return nil
}

func (f *Wrapper) PingResponse() error {
	return nil
}

func (f *Wrapper) StopMonitoring() {
}
