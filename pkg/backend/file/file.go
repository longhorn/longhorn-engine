package file

import (
	"fmt"
	"os"

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
		return fmt.Errorf("Cannot truncate to a smaller size %v for the backend type file", size)
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

func (ff *Factory) Create(address string) (types.Backend, error) {
	logrus.Infof("Creating file: %s", address)
	file, err := os.OpenFile(address, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		logrus.Infof("Failed to create file %s: %v", address, err)
		return nil, err
	}

	return &Wrapper{file}, nil
}

func (f *Wrapper) GetMonitorChannel() types.MonitorChannel {
	return nil
}

func (f *Wrapper) PingResponse() error {
	return nil
}

func (f *Wrapper) StopMonitoring() {
}
