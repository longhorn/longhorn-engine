package directfio_test

import (
	"bytes"
	"os"
	"testing"

	"log"
	"time"

	"io/ioutil"

	"syscall"

	fio "github.com/rancher/sparse-tools/directfio"
)

func tempFilePath() string {
	// Make a temporary file path
	f, err := ioutil.TempFile("", "fio-test")
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	defer f.Close()
	return f.Name()
}

// tempBigFileName is for files that are substantial in isze (for benchmark and stress tests)
// created in current directory
func tempBigFilePath() string {
	// Make a temporary file path in current dir
	f, err := ioutil.TempFile(".", "fio-test")
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

	path := tempFilePath()
	defer cleanup(path)
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
}

func TestDirectFileIO2(t *testing.T) {
	blocks := 4

	// Init data
	data1 := make([]byte, blocks*fio.BlockSize)
	fillData(data1, 0)

	path := tempFilePath()
	defer cleanup(path)
	{
		// Write
		f, err := fio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
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
		f, err := fio.OpenFile(path, os.O_RDONLY, 0)
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
}

const fileSize = int64(1) /*GB*/ << 30

const FileMode = os.O_RDWR

func write(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64) {
	data := fio.AllocateAligned(batchSize)
	fillData(data, 0)

	f, err := os.OpenFile(path, syscall.O_DIRECT|FileMode, 0)
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

func read(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64) {
	data := fio.AllocateAligned(batchSize)

	f, err := os.OpenFile(path, syscall.O_DIRECT|FileMode, 0)
	if err != nil {
		b.Fatal("Failed to OpenFile for read", err)
	}
	defer f.Close()

	for pos := offset; pos < offset+size; pos += int64(batchSize) {
		_, err = f.ReadAt(data, pos)
		if err != nil {
			b.Fatal("Failed to read", err)
		}
	}
	done <- true
}

func writeUnaligned(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64) {
	data := make([]byte, batchSize)
	fillData(data, 0)

	f, err := fio.OpenFile(path, FileMode, 0)
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

func readUnaligned(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64) {
	data := make([]byte, batchSize)

	f, err := fio.OpenFile(path, FileMode, 0)
	if err != nil {
		b.Fatal("Failed to OpenFile for read", err)
	}
	defer f.Close()

	for pos := offset; pos < offset+size; pos += int64(batchSize) {
		_, err = fio.ReadAt(f, data, pos)
		if err != nil {
			b.Fatal("Failed to read", err)
		}
	}
	done <- true
}

func ioTest(title string, b *testing.B, path string, threads, batch int, io func(b *testing.B, path string, done chan<- bool, batchSize int, offset, size int64)) {
	done := make(chan bool, threads)
	chunkSize := fileSize / int64(threads)

	start := time.Now().UnixNano()
	ioSize := batch * fio.BlockSize
	for i := 0; i < threads; i++ {
		go io(b, path, done, ioSize, int64(i)*chunkSize, chunkSize)
	}
	for i := 0; i < threads; i++ {
		<-done
	}
	stop := time.Now().UnixNano()
	if len(title) > 0 {
		log.Println(title, ":", threads, "(threads) batch=", batch, "(blocks)", "thruput=", 1000000*fileSize/(1<<20)/((stop-start)/1000), "(MB/s)")
	}
}

func BenchmarkIO8(b *testing.B) {
	path := tempBigFilePath()

	defer cleanup(path)
	f, err := os.OpenFile(path, os.O_CREATE|FileMode, 0644)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()

	f.Truncate(fileSize)
	log.Println("")
	ioTest("pilot write", b, path, 8, 32, write)

	for batch := 32; batch >= 1; batch >>= 1 {
		log.Println("")
		for threads := 1; threads <= 8; threads <<= 1 {
			ioTest("write", b, path, threads, batch, write)
		}
		for threads := 1; threads <= 8; threads <<= 1 {
			ioTest(" read", b, path, threads, batch, read)
		}
	}

}

func BenchmarkIO8u(b *testing.B) {
	path := tempBigFilePath()

	defer cleanup(path)
	f, err := os.OpenFile(path, os.O_CREATE|FileMode, 0644)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()
	f.Truncate(fileSize)
	log.Println("")
	ioTest("pilot write", b, path, 8, 32, writeUnaligned)

	for batch := 32; batch >= 1; batch >>= 1 {
		log.Println("")
		for threads := 1; threads <= 8; threads <<= 1 {
			ioTest("unaligned write", b, path, threads, batch, writeUnaligned)
		}
		for threads := 1; threads <= 8; threads <<= 1 {
			ioTest(" unaligned read", b, path, threads, batch, readUnaligned)
		}
	}
}
