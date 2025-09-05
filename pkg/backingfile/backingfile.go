package backingfile

import (
	"fmt"
	"os"

	"github.com/longhorn/sparse-tools/sparse"

	"github.com/longhorn/go-common-libs/backingimage"

	"github.com/longhorn/longhorn-engine/pkg/qcow"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type BackingFile struct {
	Size        int64
	VirtualSize int64
	SectorSize  int64
	Path        string
	Disk        types.DiffDisk
}

func OpenBackingFile(file string) (*BackingFile, error) {
	if file == "" {
		return nil, nil
	}
	file, err := util.ResolveBackingFilepath(file)
	if err != nil {
		return nil, err
	}

	imageToolExec := backingimage.NewQemuImgExecutor()
	imageInfo, err := imageToolExec.GetImageInfo(file)
	if err != nil {
		return nil, err
	}

	var f types.DiffDisk
	switch imageInfo.Format {
	case "qcow2":
		if f, err = qcow.Open(file); err != nil {
			return nil, err
		}
	case "raw":
		if f, err = sparse.NewDirectFileIoProcessor(file, os.O_RDONLY, 04444, false); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("format %v of the backing file %v is not supported", imageInfo.Format, file)
	}

	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size%diskutil.BackingImageSectorSize != 0 {
		return nil, fmt.Errorf("the backing file size %v should be a multiple of %v bytes since Longhorn uses directIO by default", size, diskutil.BackingImageSectorSize)
	}

	return &BackingFile{
		Path:        file,
		Disk:        f,
		Size:        size,
		VirtualSize: imageInfo.VirtualSize,
		SectorSize:  diskutil.BackingImageSectorSize,
	}, nil
}
