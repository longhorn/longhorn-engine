package fusedev

import (
	"github.com/rancher/longhorn/types"
)

func New() types.Frontend {
	return &Fuse{}
}

type Fuse struct {
	fs *FuseFs
}

func (f *Fuse) Activate(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	f.fs = newFuseFs(name, size, sectorSize, rw)
	if err := f.Shutdown(); err != nil {
		return err
	}

	log.Infof("Activate FUSE frontend for %v, size %v, sector size %v", name, size, sectorSize)
	if err := f.fs.Start(); err != nil {
		return err
	}
	return nil
}

func (f *Fuse) Shutdown() error {
	log.Infof("Shutdown FUSE frontend for %v", f.fs.Volume)
	return f.fs.Stop()
}
