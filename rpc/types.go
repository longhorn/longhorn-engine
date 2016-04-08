package rpc

import "sync"

const (
	TypeRead = iota
	TypeWrite
	TypeResponse
	TypeError
	TypeEOF

	messageSize     = (32 + 32 + 32 + 64) / 8
	readBufferSize  = 8096
	writeBufferSize = 8096
)

type Message struct {
	sync.WaitGroup

	Seq    uint32
	Type   uint32
	Offset int64
	Data   []byte
}
