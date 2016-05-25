package sparse

import (
	"crypto/sha1"
	"os"

	"fmt"

	"sync"

	fio "github.com/rancher/sparse-tools/directfio"
	"github.com/rancher/sparse-tools/log"
)

// File I/O methods for direct or bufferend I/O
var fileOpen func(name string, flag int, perm os.FileMode) (*os.File, error)
var fileReadAt func(file *os.File, data []byte, offset int64) (int, error)
var fileWriteAt func(file *os.File, data []byte, offset int64) (int, error)

func fileBufferedOpen(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}
func fileBufferedReadAt(file *os.File, data []byte, offset int64) (int, error) {
	return file.ReadAt(data, offset)
}
func fileBufferedWriteAt(file *os.File, data []byte, offset int64) (int, error) {
	return file.WriteAt(data, offset)
}

// SetupFileIO Sets up direct file I/O or buffered for small unaligned files
func SetupFileIO(direct bool) {
	if direct {
		fileOpen = fio.OpenFile
		fileReadAt = fio.ReadAt
		fileWriteAt = fio.WriteAt
		log.Info("Mode: directfio")
	} else {
		fileOpen = fileBufferedOpen
		fileReadAt = fileBufferedReadAt
		fileWriteAt = fileBufferedWriteAt
		log.Info("Mode: buffered")
	}
}

func loadFileLayout(abortStream <-chan error, file *os.File, layoutStream chan<- FileInterval, errStream chan<- error) error {
	size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	go RetrieveLayoutStream(abortStream, file, Interval{0, size}, layoutStream, errStream)
	return nil
}

// IntervalSplitter limits file intervals to predefined batch size
func IntervalSplitter(spltterStream <-chan FileInterval, fileStream chan<- FileInterval) {
	const batch = 32 * Blocks
	for r := range spltterStream {
		if verboseServer {
			log.Debug("Interval Splitter:", r)
		}
		switch r.Kind {
		case SparseHole:
			// Process hole
			fileStream <- r
		case SparseData:
			// Process data in chunks
			for offset := r.Begin; offset < r.End; {
				size := batch
				if offset+size > r.End {
					size = r.End - offset
				}
				interval := Interval{offset, offset + size}
				if size == batch && interval.End%batch != 0 {
					interval.End = interval.End / batch * batch
				}
				log.Debug("Interval Splitter data:", interval)
				fileStream <- FileInterval{SparseData, interval}
				offset += interval.Len()
			}
		}
	}
	close(fileStream)
}

// HashedInterval FileInterval plus its data hash (to be sent to the client)
type HashedInterval struct {
	FileInterval
	Hash []byte
}

func (i HashedInterval) String() string {
	if len(i.Hash) > 0 {
		return fmt.Sprintf("%v #%2x%2x %2x%2x", i.FileInterval, i.Hash[0], i.Hash[1], i.Hash[2], i.Hash[3])
	}
	return fmt.Sprintf("%v #         ", i.FileInterval)
}

// HashedDataInterval FileInterval plus its hash and data
type HashedDataInterval struct {
	HashedInterval
	Data []byte
}

// DataInterval FileInterval plus its data
type DataInterval struct {
	FileInterval
	Data []byte
}

// FileReaderGroup starts specified number of readers
func FileReaderGroup(count int, salt []byte, fileStream <-chan FileInterval, path string, unorderedStream chan<- HashedDataInterval) {
	var wgroup sync.WaitGroup
	wgroup.Add(count)
	for i := 0; i < count; i++ {
		go FileReader(salt, fileStream, path, &wgroup, unorderedStream)
	}
	go func() {
		wgroup.Wait() // all the readers join here
		close(unorderedStream)
	}()
}

// FileWriterGroup starts specified number of writers
func FileWriterGroup(count int, fileStream <-chan DataInterval, path string) *sync.WaitGroup {
	var wgroup sync.WaitGroup
	wgroup.Add(count)
	for i := 0; i < count; i++ {
		go FileWriter(fileStream, path, &wgroup)
	}
	return &wgroup
}

// FileReader supports concurrent file reading
// multiple readres are allowed
func FileReader(salt []byte, fileStream <-chan FileInterval, path string, wgroup *sync.WaitGroup, unorderedStream chan<- HashedDataInterval) {
	// open file
	file, err := fileOpen(path, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal("Failed to open file for reading:", string(path), err)
	}
	defer file.Close()

	for r := range fileStream {
		switch r.Kind {
		case SparseHole:
			// Process hole
			var hash, data []byte
			unorderedStream <- HashedDataInterval{HashedInterval{r, hash}, data}

		case SparseData:
			// Read file data
			data := make([]byte, r.Len())
			status := true
			n, err := fileReadAt(file, data, r.Begin)
			if err != nil {
				status = false
				log.Error("File read error", status)
			} else if int64(n) != r.Len() {
				status = false
				log.Error("File read underrun")
			}
			hasher := sha1.New()
			hasher.Write(salt)
			hasher.Write(data)
			hash := hasher.Sum(nil)
			unorderedStream <- HashedDataInterval{HashedInterval{r, hash}, data}
		}
	}
	wgroup.Done() // indicate to other readers we are done
}

// FileWriter supports concurrent file reading
// add this writer to wgroup before invoking
func FileWriter(fileStream <-chan DataInterval, path string, wgroup *sync.WaitGroup) {
	// open file
	file, err := fileOpen(path, os.O_WRONLY, 0)
	if err != nil {
		log.Fatal("Failed to open file for wroting:", string(path), err)
	}
	defer file.Close()

	for r := range fileStream {
		switch r.Kind {
		case SparseHole:
			log.Debug("trimming...")
			err := PunchHole(file, r.Interval)
			if err != nil {
				log.Fatal("Failed to trim file")
			}

		case SparseData:
			log.Debug("writing data...")
			_, err = fileWriteAt(file, r.Data, r.Begin)
			if err != nil {
				log.Fatal("Failed to write file")
			}
		}
	}
	wgroup.Done()
}

// OrderIntervals puts back "out of order" read results
func OrderIntervals(prefix string, unorderedStream <-chan HashedDataInterval, orderedStream chan<- HashedDataInterval) {
	pos := int64(0)
	m := make(map[int64]HashedDataInterval) // out of order completions
	for r := range unorderedStream {
		if pos == r.Begin {
			// Handle "in order" range
			log.Debug(prefix, r)
			orderedStream <- r
			pos = r.End
		} else {
			// push "out of order"" range
			m[r.Begin] = r
		}

		// check the "out of order" stash for "in order"
		for pop, existsNext := m[pos]; len(m) > 0 && existsNext; pop, existsNext = m[pos] {
			// pop in order range
			log.Debug(prefix, pop)
			orderedStream <- pop
			delete(m, pos)
			pos = pop.End
		}
	}
	close(orderedStream)
}
