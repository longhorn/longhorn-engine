package replica

import (
	"os"

	"github.com/rancher/sparse-tools/directfio"
)

type directFile struct {
	*os.File
}

func (d *directFile) ReadAt(buf []byte, offset int64) (int, error) {
	return directfio.ReadAt(d.File, buf, offset)
}

func (d *directFile) WriteAt(buf []byte, offset int64) (int, error) {
	return directfio.WriteAt(d.File, buf, offset)
}
