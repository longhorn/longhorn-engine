package sparse

import (
	"crypto/sha1"
	"os"

	"fmt"

	fio "github.com/rancher/sparse-tools/directfio"
	"github.com/rancher/sparse-tools/log"
)

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

// HashSalt is common client/server hash salt
var HashSalt = []byte("TODO: randomize and exchange between client/server")

// FileReader supports concurrent file reading
func FileReader(fileStream <-chan FileInterval, file *os.File, unorderedStream chan<- HashedDataInterval) {
	for r := range fileStream {
		switch r.Kind {
		case SparseHole:
			// Process hole
			// hash := sha1.New()
			// binary.PutVariant(data, r.Len)
			// fileHash.Write(data)
			var hash, data []byte
			unorderedStream <- HashedDataInterval{HashedInterval{r, hash}, data}

		case SparseData:
			// Read file data
			data := make([]byte, r.Len())
			status := true
			n, err := fio.ReadAt(file, data, r.Begin)
			if err != nil {
				status = false
				log.Error("File read error", status)
			} else if int64(n) != r.Len() {
				status = false
				log.Error("File read underrun")
			}
			hasher := sha1.New()
			hasher.Write(HashSalt)
			hasher.Write(data)
			hash := hasher.Sum(nil)
			unorderedStream <- HashedDataInterval{HashedInterval{r, hash}, data}
		}
	}
	close(unorderedStream)
}

// OrderIntervals puts back "out of order" read results
func OrderIntervals(prefix string, unorderedStream <-chan HashedDataInterval, orderedStream chan<- HashedDataInterval) {
	pos := int64(0)
	var m map[int64]HashedDataInterval // out of order completions
	for r := range unorderedStream {
		// Handle "in order" range
		if pos == r.Begin {
			log.Debug(prefix, r)
			orderedStream <- r
			pos = r.End
			continue
		}

		// push "out of order"" range
		m[r.Begin] = r

		// check the "out of order" stash for "in order"
		for pop, existsNext := m[pos]; existsNext; {
			// pop in order range
			log.Debug(prefix, pop)
			orderedStream <- pop
			delete(m, pos)
			pos = pop.End
		}
	}
	close(orderedStream)
}
