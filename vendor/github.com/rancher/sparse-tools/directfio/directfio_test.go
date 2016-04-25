package directfio_test

import (
	"bytes"
	"os"
	"testing"

	"log"
	"time"

	"io/ioutil"

	fio "github.com/rancher/sparse-tools/directfio"
	"syscall"
)

func tempFileName() string {
	// Make a temporary file name
	f, err := ioutil.TempFile("", "fio_test")
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	defer f.Close()
	return f.Name()
}

func cleanup(path string) {
	// Cleanup
	err := os.Remove(path)
	if err != nil {
		log.Fatal("Failed to remove file", path, err)
	}
}

func fillData(data []byte, start int) {
	for i := 0; i < len(data); i++ {
		data[i] = byte(start + i)
	}
}

func TestDirectFileIO1(t *testing.T) {
	blocks := 4

	// Init data
	data1 := fio.AllocateAligned(blocks * fio.BlockSize)
	fillData(data1, 0)

	path := tempFileName()
	{
		// Write
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|syscall.O_DIRECT, 0644)
		if err != nil {
			t.Fatal("Failed to OpenFile for write", err)
		}
		defer f.Close()

		_, err = f.WriteAt(data1, int64(blocks*fio.BlockSize))
		if err != nil {
			t.Fatal("Failed to write", err)
		}
	}

	data2 := fio.AllocateAligned(blocks * fio.BlockSize)
	{
		// Read
		f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECT, 0)
		if err != nil {
			t.Fatal("Failed to OpenFile for read", err)
		}
		defer f.Close()

		_, err = f.ReadAt(data2, int64(blocks*fio.BlockSize))
		if err != nil {
			t.Fatal("Failed to read", err)
		}
	}

	// Check
	if !bytes.Equal(data1, data2) {
		t.Fatal("Read not the same as written")
	}
    
    cleanup(path)
}

func TestDirectFileIO2(t *testing.T) {
	blocks := 4

	// Init data
	data1 := make([]byte, blocks*fio.BlockSize)
	fillData(data1, 0)

	path := tempFileName()
	{
		// Write
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|syscall.O_DIRECT, 0644)
		if err != nil {
			t.Fatal("Failed to OpenFile for write", err)
		}
		defer f.Close()

		_, err = fio.WriteAt(f, data1, int64(blocks*fio.BlockSize))
		if err != nil {
			t.Fatal("Failed to write", err)
		}
	}

	data2 := make([]byte, blocks*fio.BlockSize)
	{
		// Read
		f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECT, 0)
		if err != nil {
			t.Fatal("Failed to OpenFile for read", err)
		}
		defer f.Close()

		_, err = fio.ReadAt(f, data2, int64(blocks*fio.BlockSize))
		if err != nil {
			t.Fatal("Failed to read", err)
		}
	}

	// Check
	if !bytes.Equal(data1, data2) {
		t.Fatal("Read not the same as written")
	}
    
    cleanup(path)
}

const fileSize = int64(512) /*MB*/ << 20

const FileMode = os.O_WRONLY | syscall.O_DIRECT

func write(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64) {
	data := fio.AllocateAligned(batchSize)
	fillData(data, 0)

	f, err := os.OpenFile(path, FileMode, 0)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()

	for pos := offset; pos < offset+size; pos += int64(batchSize) {
		_, err = f.WriteAt(data, pos)
		if err != nil {
			b.Fatal("Failed to write", err)
		}
	}
	done <- true
}

func writeUnaligned(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64) {
	data := make([]byte, batchSize)
	fillData(data, 0)

	f, err := os.OpenFile(path, FileMode, 0)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()

	for pos := offset; pos < offset+size; pos += int64(batchSize) {
		_, err = fio.WriteAt(f, data, pos)
		if err != nil {
			b.Fatal("Failed to write", err)
		}
	}
	done <- true
}

func writeTest(b *testing.B, path string, writers, batch int, write func(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64)) {
	done := make(chan bool, writers)
	chunkSize := fileSize / int64(writers)

	start := time.Now().Unix()
	ioSize := batch * fio.BlockSize
	for i := 0; i < writers; i++ {
		go write(b, path, done, ioSize, int64(i)*chunkSize, chunkSize)
	}
	for i := 0; i < writers; i++ {
		<-done
	}
	stop := time.Now().Unix()
	log.Println("writers=", writers, "batch=", batch, "(blocks)", "thruput=", fileSize/(1<<20)/(stop-start), "(MB/s)")
}

func BenchmarkWrite8(b *testing.B) {
	path := tempFileName()

	f, err := os.OpenFile(path, os.O_CREATE|FileMode, 0644)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()
	f.Truncate(fileSize)

	for batch := 32; batch >= 1; batch >>= 1 {
		log.Println("")
		for writers := 1; writers <= 8; writers <<= 1 {
			writeTest(b, path, writers, batch, write)
		}
	}
    
    cleanup(path)
}

func BenchmarkWrite8u(b *testing.B) {
	path := tempFileName()

	f, err := os.OpenFile(path, os.O_CREATE|FileMode, 0644)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()
	f.Truncate(fileSize)

	for batch := 32; batch >= 1; batch >>= 1 {
		log.Println("")
		for writers := 1; writers <= 8; writers <<= 1 {
			writeTest(b, path, writers, batch, writeUnaligned)
		}
	}
    
    cleanup(path)
}
