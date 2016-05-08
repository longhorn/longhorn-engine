package sparse

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"hash"
	"net"
	"os"
	"strconv"
	"time"

	fio "github.com/rancher/sparse-tools/directfio"
	"github.com/rancher/sparse-tools/log"
)

// Server daemon
func Server(addr TCPEndPoint, timeout int) {
	server(addr, true /*serve single connection for now*/, timeout)
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(addr TCPEndPoint, timeout int) {
	server(addr, true, timeout)
}

const verboseServer = true

func server(addr TCPEndPoint, serveOnce /*test flag*/ bool, timeout int) {
	serverConnectionTimeout := time.Duration(timeout) * time.Second
	// listen on all interfaces
	EndPoint := addr.Host + ":" + strconv.Itoa(int(addr.Port))
	laddr, err := net.ResolveTCPAddr("tcp", EndPoint)
	if err != nil {
		log.Fatal("Connection listener address resolution error:", err)
	}
	ln, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal("Connection listener error:", err)
	}
	defer ln.Close()
	ln.SetDeadline(time.Now().Add(serverConnectionTimeout))
	log.Info("Sync server is up...")

	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			log.Fatal("Connection accept error:", err)
		}

		if serveOnce {
			// This is to avoid server listening port conflicts while running tests
			// exit after single connection request
			serveConnection(conn)
			break
		}

		go serveConnection(conn)
	}
	log.Info("Sync server exit.")
}

type requestCode int

const (
	requestMagic    requestCode = 31415926
	syncRequestCode requestCode = 1
)

type requestHeader struct {
	Magic requestCode
	Code  requestCode
}

func serveConnection(conn net.Conn) {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	var request requestHeader
	err := decoder.Decode(&request)
	if err != nil {
		log.Fatal("Protocol decoder error:", err)
		return
	}
	if requestMagic != request.Magic {
		log.Error("Bad request")
		return
	}

	switch request.Code {
	case syncRequestCode:
		var path string
		err := decoder.Decode(&path)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			return
		}
		var size int64
		err = decoder.Decode(&size)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			return
		}
		encoder := gob.NewEncoder(conn)
		serveSyncRequest(encoder, decoder, path, size)
	}
}

func serveSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64) {

	// Open destination file
	file, err := fio.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		file, err = os.Create(path)
		if err != nil {
			log.Error("Failed to create file:", string(path), err)
			encoder.Encode(false) // NACK request
			return
		}
	}
	defer file.Close()

	// Resize the file
	if err = file.Truncate(size); err != nil {
		log.Error("Failed to resize file:", string(path), err)
		encoder.Encode(false) // NACK request
		return
	}

	// open file
	fileRO, err := fio.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		log.Error("Failed to open file for reading:", string(path), err)
		encoder.Encode(false) // NACK request
		return
	}
	defer fileRO.Close()

	abortStream := make(chan error)
	layoutStream := make(chan FileInterval, 128)
	errStream := make(chan error)

	fileStream := make(chan FileInterval, 128)
	unorderedStream := make(chan HashedDataInterval, 128)
	orderedStream := make(chan HashedDataInterval, 128)
	netOutStream := make(chan HashedInterval, 128)
	netOutDoneStream := make(chan bool)

	netInStream := make(chan DataInterval, 128)
	fileWrittenStreamDone := make(chan bool)
	checksumStream := make(chan DataInterval, 128)
	resultStream := make(chan []byte)

	// Initiate interval loading...
	err = loadFileLayout(abortStream, fileRO, layoutStream, errStream)
	if err != nil {
		encoder.Encode(false) // NACK request
		return
	}
	encoder.Encode(true) // ACK request

	go IntervalSplitter(layoutStream, fileStream)
	go FileReader(fileStream, fileRO, unorderedStream)
	go OrderIntervals("dst:", unorderedStream, orderedStream)
	go Tee(orderedStream, netOutStream, checksumStream)

	// Send layout along with data hashes
	go netSender(netOutStream, encoder, netOutDoneStream)
	go netReceiver(decoder, file, netInStream, fileWrittenStreamDone) // receiver and checker
	go Validator(checksumStream, netInStream, resultStream)

	// Block till completion
	status := true
	err = <-errStream // Done with file loadaing, possibly aborted on error
	if err != nil {
		log.Error("Sync server file load aborted:", err)
		status = false
	}
	status = <-netOutDoneStream && status      // done sending dst hashes
	status = <-fileWrittenStreamDone && status // done writing dst file
	hash := <-resultStream                     // done processing diffs

	// reply to client with status
	log.Info("Sync sending server status=", status)
	err = encoder.Encode(status)
	if err != nil {
		log.Fatal("Protocol encoder error:", err)
		return
	}
	// reply with local hash
	err = encoder.Encode(hash)
	if err != nil {
		log.Fatal("Protocol encoder error:", err)
		return
	}
}

// Tee ordered intervals into the network and checksum checker
func Tee(orderedStream <-chan HashedDataInterval, netOutStream chan<- HashedInterval, checksumStream chan<- DataInterval) {
	for r := range orderedStream {
		netOutStream <- HashedInterval{r.FileInterval, r.Hash}
		checksumStream <- DataInterval{r.FileInterval, r.Data}
	}
	close(netOutStream)
	close(checksumStream)
}

func netSender(netOutStream <-chan HashedInterval, encoder *gob.Encoder, netOutDoneStream chan<- bool) {
	for r := range netOutStream {
		if verboseServer {
			log.Debug("Server.netSender: sending", r.FileInterval)
		}
		err := encoder.Encode(r)
		if err != nil {
			log.Fatal("Protocol encoder error:", err)
			netOutDoneStream <- false
			return
		}
	}

	rEOF := HashedInterval{FileInterval{SparseIgnore, Interval{}}, make([]byte, 0)}
	if rEOF.Len() != 0 {
		log.Fatal("Server.netSender internal error")
	}
	// err := encoder.Encode(HashedInterval{FileInterval{}, make([]byte, 0)})
	err := encoder.Encode(rEOF)
	if err != nil {
		log.Fatal("Protocol encoder error:", err)
		netOutDoneStream <- false
		return
	}
	if verboseServer {
		log.Debug("Server.netSender: finished sending hashes")
	}
	netOutDoneStream <- true
}

func netReceiver(decoder *gob.Decoder, file *os.File, netInStream chan<- DataInterval, fileWrittenStreamDone chan<- bool) {
	// receive & process data diff
	status := true
	for status {
		var delta FileInterval
		err := decoder.Decode(&delta)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			status = false
			break
		}
		log.Debug("receiving delta [", delta, "]")
		if 0 == delta.Len() {
			log.Debug("received end of transimission marker")
			break // end of diff
		}
		switch delta.Kind {
		case SparseData:
			// Receive data
			var data []byte
			err = decoder.Decode(&data)
			if err != nil {
				log.Fatal("Protocol data decoder error:", err)
				status = false
				break
			}
			if int64(len(data)) != delta.Len() {
				log.Fatal("Failed to receive data, expected=", delta.Len(), "received=", len(data))
				status = false
				break
			}
			// Push for vaildator processing
			netInStream <- DataInterval{delta, data}

			log.Debug("writing data...")
			_, err = fio.WriteAt(file, data, delta.Begin)
			if err != nil {
				log.Error("Failed to write file")
				status = false
				break
			}
		case SparseHole:
			// Push for vaildator processing
			netInStream <- DataInterval{delta, make([]byte, 0)}

			log.Debug("trimming...")
			err := PunchHole(file, delta.Interval)
			if err != nil {
				log.Error("Failed to trim file")
				status = false
				break
			}
		case SparseIgnore:
			// Push for vaildator processing
			netInStream <- DataInterval{delta, make([]byte, 0)}
			log.Debug("ignoring...")
		}
	}

	log.Debug("Server.netReceiver done, sync")
	close(netInStream)
	fileWrittenStreamDone <- status
}

func logData(prefix string, data []byte) {
	size := len(data)
	if size > 0 {
		log.Debug("\t", prefix, "of", size, "bytes", data[0], "...")
	} else {
		log.Debug("\t", prefix, "of", size, "bytes")
	}
}

func hashFileData(fileHasher hash.Hash, dataLen int64, data []byte) {
	// Hash hole length or data if any
	if len(data) == 0 {
		// hash hole
		hole := make([]byte, 8)
		binary.PutVarint(hole, dataLen)
		fileHasher.Write(hole)

	} else {
		fileHasher.Write(data)
	}
}

// Validator merges source and diff data; produces hash of the destination file
func Validator(checksumStream, netInStream <-chan DataInterval, resultStream chan<- []byte) {
	fileHasher := sha1.New()
	//TODO: error handling
	fileHasher.Write(HashSalt)
	r := <-checksumStream // original dst file data
	q := <-netInStream    // diff data
	for q.Len() != 0 || r.Len() != 0 {
		if r.Len() == 0 /*end of dst file*/ {
			// Hash diff data
			if verboseServer {
				logData("RHASH", q.Data)
			}
			hashFileData(fileHasher, q.Len(), q.Data)
			q = <-netInStream
		} else if q.Len() == 0 /*end of diff file*/ {
			// Hash original data
			if verboseServer {
				logData("RHASH", r.Data)
			}
			hashFileData(fileHasher, r.Len(), r.Data)
			r = <-checksumStream
		} else {
			qi := q.Interval
			ri := r.Interval
			if qi.Begin == ri.Begin {
				if qi.End > ri.End {
					log.Fatal("Server.Validator internal error, diff=", q.FileInterval, "local=", r.FileInterval)
				} else if qi.End < ri.End {
					// Hash diff data
					if verboseServer {
						log.Debug("Server.Validator: hashing diff", q.FileInterval, r.FileInterval)
					}
					if verboseServer {
						logData("RHASH", q.Data)
					}
					hashFileData(fileHasher, q.Len(), q.Data)
					r.Begin = q.End
					q = <-netInStream
				} else {
					if q.Kind == SparseIgnore {
						// Hash original data
						if verboseServer {
							log.Debug("Server.Validator: hashing original", r.FileInterval)
						}
						if verboseServer {
							logData("RHASH", r.Data)
						}
						hashFileData(fileHasher, r.Len(), r.Data)
					} else {
						// Hash diff data
						if verboseServer {
							log.Debug("Server.Validator: hashing diff", q.FileInterval)
						}
						if verboseServer {
							logData("RHASH", q.Data)
						}
						hashFileData(fileHasher, q.Len(), q.Data)
					}
					q = <-netInStream
					r = <-checksumStream
				}
			} else {
				log.Fatal("Server.Validator internal error, diff=", q.FileInterval, "local=", r.FileInterval)
			}
		}
	}

	if verboseServer {
		log.Debug("Server.Validator: finished")
	}
	resultStream <- fileHasher.Sum(nil)
}
