package sparse

import (
	"crypto/sha1"
	"net"
	"os"
	"strconv"

	fio "github.com/rancher/sparse-tools/directfio"
	"github.com/rancher/sparse-tools/log"
)

import "encoding/gob"

import "errors"

import "fmt"
import "time"

// TCPEndPoint tcp connection address
type TCPEndPoint struct {
	Host string
	Port int16
}

const connectionRetries = 5
const verboseClient = true

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, addr TCPEndPoint, remotePath string, timeout int) (hashLocal []byte, err error) {
	hashLocal = make([]byte, 0) // empty hash for errors
	file, err := fio.OpenFile(localPath, os.O_RDONLY, 0)
	if err != nil {
		log.Error("Failed to open local source file:", localPath)
		return
	}
	defer file.Close()

	size, errSize := file.Seek(0, os.SEEK_END)
	if errSize != nil {
		log.Error("Failed to get size of local source file:", localPath, errSize)
		return
	}

	conn := connect(addr.Host, strconv.Itoa(int(addr.Port)), timeout)
	if nil == conn {
		log.Error("Failed to connect to", addr)
		return
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	status := sendSyncRequest(encoder, decoder, remotePath, size)
	if !status {
		return
	}

	abortStream := make(chan error)
	layoutStream := make(chan FileInterval, 128)
	errStream := make(chan error)

	// Initiate interval loading...
	err = loadFileLayout(abortStream, file, layoutStream, errStream)
	if err != nil {
		log.Error("Failed to retrieve local file layout:", err)
		return
	}

	fileStream := make(chan FileInterval, 128)
	unorderedStream := make(chan HashedDataInterval, 128)
	orderedStream := make(chan HashedDataInterval, 128)

	go IntervalSplitter(layoutStream, fileStream)
	go FileReader(fileStream, file, unorderedStream)
	go OrderIntervals("src:", unorderedStream, orderedStream)

	// Get remote file intervals and their hashes
	netInStream := make(chan HashedInterval, 128)
	netInStreamDone := make(chan bool)
	go netDstReceiver(decoder, netInStream, netInStreamDone)

	return processDiff(abortStream, errStream, encoder, decoder, orderedStream, netInStream, netInStreamDone, file)
}

func connect(host, port string, timeout int) net.Conn {
	// connect to this socket
	endpoint := host + ":" + port
	raddr, err := net.ResolveTCPAddr("tcp", endpoint)
	if err != nil {
		log.Fatal("Connection address resolution error:", err)
	}
	timeStart := time.Now()
	timeStop := timeStart.Add(time.Duration(timeout) * time.Second)
	for timeNow := timeStart; timeNow.Before(timeStop); timeNow = time.Now() {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			return conn
		}
		log.Warn("Failed connection to", endpoint, "Retrying...")
		if timeNow != timeStart {
			// only sleep after the second attempt to speedup tests
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

func sendSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64) bool {
	err := encoder.Encode(requestHeader{requestMagic, syncRequestCode})
	if err != nil {
		log.Fatal("Client protocol encoder error:", err)
		return false
	}
	err = encoder.Encode(path)
	if err != nil {
		log.Fatal("Client protocol encoder error:", err)
		return false
	}
	err = encoder.Encode(size)
	if err != nil {
		log.Fatal("Client protocol encoder error:", err)
		return false
	}

	var ack bool
	err = decoder.Decode(&ack)
	if err != nil {
		log.Fatal("Client protocol decoder error:", err)
		return false
	}

	return ack
}

// Get remote hashed intervals
func netDstReceiver(decoder *gob.Decoder, netInStream chan<- HashedInterval, netInStreamDone chan<- bool) {
	status := true
	for {
		if verboseClient {
			log.Debug("Client.netDstReceiver decoding...")
		}
		var r HashedInterval
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatal("Cient protocol error:", err)
			status = false
			break
		}
		// interval := r.Interval
		if r.Kind == SparseIgnore {
			if verboseClient {
				log.Debug("Client.netDstReceiver got <eof>")
			}
			break
		}
		if verboseClient {
			switch r.Kind {
			case SparseData:
				log.Debug("Client.netDstReceiver got data", r.FileInterval, "hash[", len(r.Hash), "]")
			case SparseHole:
				log.Debug("Client.netDstReceiver got hole", r.FileInterval)
			}
		}
		netInStream <- r
	}
	close(netInStream)
	netInStreamDone <- status
}

// file reading chunk
type fileChunk struct {
	eof    bool // end of stream: stop reader
	header FileInterval
}

// network transfer chunk
type diffChunk struct {
	status bool // read file or network send error yield false
	header DataInterval
}

func processDiff(abortStream chan<- error, errStream <-chan error, encoder *gob.Encoder, decoder *gob.Decoder, local <-chan HashedDataInterval, remote <-chan HashedInterval, netInStreamDone <-chan bool, file *os.File) (hashLocal []byte, err error) {
	// Local:   __ _*
	// Remote:  *_ **
	hashLocal = make([]byte, 0) // empty hash for errors
	const concurrentReaders = 4
	netStream := make(chan diffChunk, 128)
	netStatus := make(chan netXferStatus)
	go networkSender(netStream, encoder, netStatus)
	// fileStream := make(chan fileChunk, 128)
	// fileStatus := make(chan bool)
	// for i := 0; i < concurrentReaders; i++ {
	// 	if 0 == i {
	// 		go fileReader(i, file, fileStream, netStream, fileStatus)
	// 	} else {
	// 		f, _ := os.Open(file.Name())
	// 		go fileReader(i, f, fileStream, netStream, fileStatus)
	// 	}
	// }
	fileHasher := sha1.New()
	fileHasher.Write(HashSalt)

	lrange := <-local
	rrange := <-remote
	for lrange.Len() != 0 {
		if rrange.Len() == 0 {
			// Copy local tail
			if verboseClient {
				logData("LHASH", lrange.Data)
			}
			hashFileData(fileHasher, lrange.Len(), lrange.Data)
			processFileInterval(lrange, HashedInterval{FileInterval{SparseHole, lrange.Interval}, make([]byte, 0)}, netStream)
			lrange = <-local
			continue
		}
		// Diff
		if verboseClient {
			log.Debug("Diff:", lrange.HashedInterval, rrange)
		}
		if lrange.Begin == rrange.Begin {
			if lrange.End > rrange.End {
				data := lrange.Data
				if len(data) > 0 {
					data = lrange.Data[:rrange.Len()]
				}
				subrange := HashedDataInterval{HashedInterval{FileInterval{lrange.Kind, rrange.Interval}, lrange.Hash}, data}
				if verboseClient {
					logData("LHASH", subrange.Data)
				}

				hashFileData(fileHasher, subrange.Len(), subrange.Data)
				processFileInterval(subrange, rrange, netStream)
				if len(data) > 0 {
					lrange.Data = lrange.Data[subrange.Len():]
				}
				lrange.Begin = rrange.End
				rrange = <-remote
				continue
			} else if lrange.End < rrange.End {
				if verboseClient {
					logData("LHASH", lrange.Data)
				}

				hashFileData(fileHasher, lrange.Len(), lrange.Data)
				processFileInterval(lrange, HashedInterval{FileInterval{rrange.Kind, lrange.Interval}, make([]byte, 0)}, netStream)
				rrange.Begin = lrange.End
				lrange = <-local
				continue
			}
			if verboseClient {
				logData("LHASH", lrange.Data)
			}
			hashFileData(fileHasher, lrange.Len(), lrange.Data)
			processFileInterval(lrange, rrange, netStream)
			lrange = <-local
			rrange = <-remote
		} else {
			// Should never happen
			log.Fatal("internal error")
			err = errors.New("internal error")
			return
		}
	}
	log.Info("Finished processing file diff")

	// // stop file readers
	// for i := 0; i < concurrentReaders; i++ {
	// 	fileStream <- fileChunk{true, FileInterval{SparseHole, Interval{0, 0}}}
	// 	<-fileStatus // wait for reader completion
	// }

	status := true
	err = <-errStream
	if err != nil {
		log.Error("Sync client file load aborted:", err)
		status = false
	}
	// make sure we finished consuming dst hashes
	status = <-netInStreamDone && status // netDstReceiver finished
	log.Info("Finished consuming remote file hashes, status=", status)

	// Send end of transmission
	netStream <- diffChunk{true, DataInterval{FileInterval{SparseIgnore, Interval{0, 0}}, make([]byte, 0)}}

	// get network sender status
	net := <-netStatus
	log.Info("Finished sending file diff of", net.byteCount, "(bytes), status=", net.status)
	if !net.status {
		err = errors.New("netwoek transfer failure")
		return
	}

	var statusRemote bool
	err = decoder.Decode(&statusRemote)
	if err != nil {
		log.Fatal("Cient protocol remote status error:", err)
		return
	}
	if !statusRemote {
		err = errors.New("failure on remote sync site")
		return
	}
	var hashRemote []byte
	err = decoder.Decode(&hashRemote)
	if err != nil {
		log.Fatal("Cient protocol remote hash error:", err)
		return
	}

	// Compare file hashes
	hashLocal = fileHasher.Sum(nil)
	if isHashDifferent(hashLocal, hashRemote) {
		log.Warn("hashLocal =", hashLocal)
		log.Warn("hashRemote=", hashRemote)
		err = errors.New("file hash divergence: storage error or block hash collision")
		return
	}
	return
}

func isHashDifferent(a, b []byte) bool {
	if len(a) != len(b) {
		return true
	}
	for i, val := range a {
		if val != b[i] {
			return true
		}
	}
	return false // hashes are equal
}

func processFileInterval(local HashedDataInterval, remote HashedInterval, netStream chan<- diffChunk) {
	if local.Interval != remote.Interval {
		log.Fatal("Sync.processFileInterval range internal error:", local.FileInterval, remote.FileInterval)
	}
	if local.Kind != remote.Kind {
		// Different intreval types, send the diff
		if local.Kind == SparseData && int64(len(local.Data)) != local.FileInterval.Len() {
			log.Fatal("Sync.processFileInterval data internal error:", local.FileInterval.Len(), len(local.Data))
		}
		netStream <- diffChunk{true, DataInterval{local.FileInterval, local.Data}}
		return
	}

	// The interval types are the same
	if SparseHole == local.Kind {
		// Process hole, no syncronization is required
		local.Kind = SparseIgnore
		netStream <- diffChunk{true, DataInterval{local.FileInterval, local.Data}}
		return
	}

	if local.Kind != SparseData {
		log.Fatal("Sync.processFileInterval kind internal error:", local.FileInterval)
	}
	// Data file interval
	if isHashDifferent(local.Hash, remote.Hash) {
		if int64(len(local.Data)) != local.FileInterval.Len() {
			log.Fatal("Sync.processFileInterval internal error:", local.FileInterval.Len(), len(local.Data))
		}
		netStream <- diffChunk{true, DataInterval{local.FileInterval, local.Data}}
		return
	}

	// No diff, just communicate we processed it
	//TODO: this apparently can be avoided but requires revision of the protocol
	local.Kind = SparseIgnore
	netStream <- diffChunk{true, DataInterval{local.FileInterval, make([]byte, 0)}}
}

// prints chan codes and lengths to trace
// - sequence and interleaving of chan processing
// - how much of the chan buffer is used
const traceChannelLoad = false

type netXferStatus struct {
	status    bool
	byteCount int64
}

func networkSender(netStream <-chan diffChunk, encoder *gob.Encoder, netStatus chan<- netXferStatus) {
	status := true
	byteCount := int64(0)
	for {
		chunk := <-netStream
		if 0 == chunk.header.Len() {
			// eof: last 0 len header
			if verboseClient {
				log.Debug("Client.networkSender <eof>")
			}
			err := encoder.Encode(chunk.header.FileInterval)
			if err != nil {
				log.Fatal("Client protocol encoder error:", err)
				status = false
			}
			break
		}

		if !status {
			// network error
			continue // discard the chunk
		}
		if !chunk.status {
			// read error
			status = false
			continue // discard the chunk
		}

		if traceChannelLoad {
			fmt.Fprint(os.Stderr, len(netStream), "n")
		}

		// Encode and send data to the network
		if verboseClient {
			log.Debug("Client.networkSender sending:", chunk.header.FileInterval)
		}
		err := encoder.Encode(chunk.header.FileInterval)
		if err != nil {
			log.Fatal("Client protocol encoder error:", err)
			status = false
			continue
		}
		if len(chunk.header.Data) == 0 {
			continue
		}
		if verboseClient {
			log.Debug("Client.networkSender sending data")
		}
		if int64(len(chunk.header.Data)) != chunk.header.FileInterval.Len() {
			log.Fatal("Client.networkSender sending data internal error:", chunk.header.FileInterval.Len(), len(chunk.header.Data))
		}
		err = encoder.Encode(chunk.header.Data)
		if err != nil {
			log.Fatal("Client protocol encoder error:", err)
			status = false
			continue
		}
		byteCount += int64(len(chunk.header.Data))
		if traceChannelLoad {
			fmt.Fprint(os.Stderr, "N\n")
		}
	}
	netStatus <- netXferStatus{status, byteCount}
}

// obsolete method
func fileReader(id int, file *os.File, fileStream <-chan fileChunk, netStream chan<- diffChunk, fileStatus chan<- bool) {
	idBeg := map[int]string{0: "a", 1: "b", 2: "c", 3: "d"}
	idEnd := map[int]string{0: "A", 1: "B", 2: "C", 3: "D"}
	for {
		chunk := <-fileStream
		if chunk.eof {
			break
		}
		if traceChannelLoad {
			fmt.Fprint(os.Stderr, len(fileStream), idBeg[id])
		}
		// Check interval type
		r := chunk.header
		if SparseData != r.Kind {
			log.Fatal("internal error: noles should be send directly to netStream")
		}

		// Read file data
		data := make([]byte, r.Len())
		status := true
		n, err := fio.ReadAt(file, data, r.Begin)
		if err != nil {
			log.Error("File read error")
			status = false
		} else if int64(n) != r.Len() {
			log.Error("File read underrun")
			status = false
		}

		// Send file data
		if traceChannelLoad {
			fmt.Fprint(os.Stderr, idEnd[id])
		}
		netStream <- diffChunk{status, DataInterval{r, data}}
	}
	log.Info("Finished reading file")
	fileStatus <- true
}
