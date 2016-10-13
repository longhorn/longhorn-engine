package fusedev

import (
	"github.com/rancher/longhorn/types"
)

func New() types.Frontend {
	return &Fuse{}
}

type Fuse struct {
	lf   *LonghornFs
	isUp bool
}

func (f *Fuse) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	if err := f.Shutdown(); err != nil {
		return err
	}

	f.lf = newLonghornFs(name, size, sectorSize, rw)
	log.Infof("Activate FUSE frontend for %v, size %v, sector size %v", name, size, sectorSize)
	if err := f.lf.Start(); err != nil {
		return err
	}
	f.isUp = true
	return nil
}

func (f *Fuse) Shutdown() error {
	if f.lf != nil {
		log.Infof("Shutdown FUSE frontend for %v", f.lf.Volume)
		if err := f.lf.Stop(); err != nil {
			return err
		}
	}
	f.isUp = false
	return nil
}

func (f *Fuse) State() types.State {
	if f.isUp {
		return types.StateUp
	}
	return types.StateDown
}
