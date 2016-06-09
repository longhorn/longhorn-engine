package rpc

import "github.com/rancher/sparse-tools/stats"

const (
	TypeRead = iota
	TypeWrite
	TypeResponse
	TypeError
	TypeEOF

	messageSize     = (32 + 32 + 32 + 64) / 8 //TODO: unused?
	readBufferSize  = 8096
	writeBufferSize = 8096
)

type Message struct {
	Complete chan struct{}

	Seq          uint32
	Type         uint32
	Offset       int64
	Data         []byte
	transportErr error

	ID stats.OpID //Seq and ID can apparently be collapsed into one (ID)
}
