package backingfile

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/longhorn/sparse-tools/sparse"

	"github.com/longhorn/longhorn-engine/pkg/qcow"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type BackingFile struct {
	Size       int64
	SectorSize int64
	Path       string
	Disk       types.DiffDisk
}

func detectFileFormat(file string) (string, error) {

	/* Example command outputs
	   $ qemu-img info parrot.raw
	   image: parrot.raw
	   file format: raw
	   virtual size: 32M (33554432 bytes)
	   disk size: 2.2M

	   $ qemu-img info parrot.qcow2
	   image: parrot.qcow2
	   file format: qcow2
	   virtual size: 32M (33554432 bytes)
	   disk size: 2.3M
	   cluster_size: 65536
	   Format specific information:
	       compat: 1.1
	       lazy refcounts: false
	       refcount bits: 16
	       corrupt: false
	*/

	cmd := exec.Command("qemu-img", "info", file)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to check backing file format with command [qume-img info %v], output %s, stderr, %s, error %v",
			file, output.String(), stderr.String(), err)
	}

	scanner := bufio.NewScanner(strings.NewReader(output.String()))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "file format: ") {
			return strings.TrimPrefix(line, "file format: "), nil
		}
	}

	return "", fmt.Errorf("cannot find the file format in the output %s", output.String())
}

func OpenBackingFile(file string) (*BackingFile, error) {
	if file == "" {
		return nil, nil
	}
	file, err := util.ResolveBackingFilepath(file)
	if err != nil {
		return nil, err
	}

	format, err := detectFileFormat(file)
	if err != nil {
		return nil, err
	}

	var f types.DiffDisk
	switch format {
	case "qcow2":
		if f, err = qcow.Open(file); err != nil {
			return nil, err
		}
	case "raw":
		if f, err = sparse.NewDirectFileIoProcessor(file, os.O_RDONLY, 04444, false); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("format %v of the backing file %v is not supported", format, file)
	}

	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size%diskutil.BackingImageSectorSize != 0 {
		return nil, fmt.Errorf("the backing file size %v should be a multiple of %v bytes since Longhorn uses directIO by default", size, diskutil.BackingImageSectorSize)
	}

	return &BackingFile{
		Path:       file,
		Disk:       f,
		Size:       size,
		SectorSize: diskutil.BackingImageSectorSize,
	}, nil
}
