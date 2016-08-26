package fusedev

import (
	"github.com/rancher/longhorn/types"
)

func New() types.Frontend {
	return &Fuse{}
}

type Fuse struct {
	lf *LonghornFs
}

func (f *Fuse) Activate(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	if err := f.Shutdown(); err != nil {
		return err
	}

	f.lf = newLonghornFs(name, size, sectorSize, rw)
	log.Infof("Activate FUSE frontend for %v, size %v, sector size %v", name, size, sectorSize)
	if err := f.lf.Start(); err != nil {
		return err
	}
	return nil
}

func (f *Fuse) Shutdown() error {
	if f.lf != nil {
		log.Infof("Shutdown FUSE frontend for %v", f.lf.Volume)
		return f.lf.Stop()
	}
	return nil
}
