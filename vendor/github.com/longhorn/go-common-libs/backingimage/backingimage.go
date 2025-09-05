package backingimage

import (
	"encoding/json"
	"time"

	"github.com/longhorn/go-common-libs/exec"
)

const (
	QemuImgBinary = "qemu-img"
)

type ImageInfo struct {
	Format string `json:"format"`

	// physical image file size on disk
	ActualSize int64 `json:"actual-size"`
	// claimed size of the guest disk.
	// For qcow2 files, VirtualSize may be larger than the physical image file size on disk.
	// For raw files, `qemu-img info` will report VirtualSize as being the same as the ActualSize.
	VirtualSize int64 `json:"virtual-size"`
}

type QemuImgExecutor struct {
	exec exec.ExecuteInterface
}

func NewQemuImgExecutor() *QemuImgExecutor {
	return newQemuImgExecutor(exec.NewExecutor())
}

func newQemuImgExecutor(exec exec.ExecuteInterface) *QemuImgExecutor {
	return &QemuImgExecutor{exec: exec}
}

func (ex *QemuImgExecutor) Exec(envs []string, args ...string) (string, error) {
	return ex.exec.Execute(envs, QemuImgBinary, args, time.Minute)
}

func (ex *QemuImgExecutor) GetImageInfo(filePath string) (imgInfo ImageInfo, err error) {

	/* Example command outputs
	   $ qemu-img info --output=json SLE-Micro.x86_64-5.5.0-Default-qcow-GM.qcow2
	   {
	       "virtual-size": 21474836480,
	       "filename": "SLE-Micro.x86_64-5.5.0-Default-qcow-GM.qcow2",
	       "cluster-size": 65536,
	       "format": "qcow2",
	       "actual-size": 1001656320,
	       "format-specific": {
	           "type": "qcow2",
	           "data": {
	               "compat": "1.1",
	               "compression-type": "zlib",
	               "lazy-refcounts": false,
	               "refcount-bits": 16,
	               "corrupt": false,
	               "extended-l2": false
	           }
	       },
	       "dirty-flag": false
	   }

	   $ qemu-img info --output=json SLE-15-SP5-Full-x86_64-GM-Media1.iso
	   {
	       "virtual-size": 14548992000,
	       "filename": "SLE-15-SP5-Full-x86_64-GM-Media1.iso",
	       "format": "raw",
	       "actual-size": 14548996096,
	       "dirty-flag": false
	   }
	*/

	output, err := ex.Exec([]string{}, "info", "--output=json", filePath)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(output), &imgInfo)
	return
}
