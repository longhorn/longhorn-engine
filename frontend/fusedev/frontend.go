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
	if f.fs != nil {
		log.Infof("FUSE frontend already activated for %v, shut it down first", name)
		if err := f.Shutdown(); err != nil {
			return err
		}
	}
	log.Infof("Activate FUSE frontend for %v, size %v, sector size %v", name, size, sectorSize)
	fs, err := start(name, size, sectorSize, rw)
	if err != nil {
		return err
	}
	f.fs = fs
	return nil
}

func (f *Fuse) Shutdown() error {
	if f.fs == nil {
		return nil
	}
	log.Infof("Shutdown FUSE frontend for %v", f.fs.Volume)
	if err := f.fs.Stop(); err != nil {
		return err
	}
	f.fs = nil
	return nil
}
