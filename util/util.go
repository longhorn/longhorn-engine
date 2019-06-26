package util

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

const (
	GRPCHealthProbe = "/usr/bin/grpc_health_probe"
)

type Bitmap struct {
	base int32
	size int32
	data *roaring.Bitmap
	lock *sync.Mutex
}

// NewBitmap allocate a bitmap range from [start, end], notice the end is included
func NewBitmap(start, end int32) *Bitmap {
	size := end - start + 1
	data := roaring.New()
	if size > 0 {
		data.AddRange(0, uint64(size))
	}
	return &Bitmap{
		base: start,
		size: size,
		data: data,
		lock: &sync.Mutex{},
	}
}

func (b *Bitmap) AllocateRange(count int32) (int32, int32, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if count <= 0 {
		return 0, 0, nil
	}
	i := b.data.Iterator()
	bStart := int32(0)
	for bStart <= b.size {
		last := int32(-1)
		remains := count
		for i.HasNext() && remains > 0 {
			// first element
			if last < 0 {
				last = int32(i.Next())
				bStart = last
				remains--
				continue
			}
			next := int32(i.Next())
			// failed to find the available range
			if next-last > 1 {
				break
			}
			last = next
			remains--
		}
		if remains == 0 {
			break
		}
		if !i.HasNext() {
			return 0, 0, fmt.Errorf("cannot find an empty port range")
		}
	}
	bEnd := bStart + count - 1
	b.data.RemoveRange(uint64(bStart), uint64(bEnd)+1)
	return b.base + bStart, b.base + bEnd, nil
}

func (b *Bitmap) ReleaseRange(start, end int32) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if start == end && end == 0 {
		return nil
	}
	bStart := start - b.base
	bEnd := end - b.base
	if bStart < 0 || bEnd >= b.size {
		return fmt.Errorf("exceed range: %v-%v (%v-%v)", start, end, bStart, bEnd)
	}
	b.data.AddRange(uint64(bStart), uint64(bEnd)+1)
	return nil
}

func RemoveFile(file string) error {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		// file doesn't exist
		return nil
	}

	cmd := exec.Command("rm", file)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("fail to remove file %v: %v", file, err)
	}

	return nil
}

func GetURL(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func PrintJSON(obj interface{}) error {
	output, err := json.MarshalIndent(obj, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func GRPCServiceReadinessProbe(address string) bool {
	cmd := exec.Command(GRPCHealthProbe, "-addr", address)
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}
