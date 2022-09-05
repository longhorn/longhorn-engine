package dataconn

import journal "github.com/longhorn/sparse-tools/stats"

const (
	TypeRead = iota
	TypeWrite
	TypeResponse
	TypeError
	TypeEOF
	TypeClose
	TypePing
	TypeUnmap

	messageSize     = (32 + 32 + 32 + 64) / 8 //TODO: unused?
	readBufferSize  = 8096
	writeBufferSize = 8096
)

const (
	MagicVersion = uint16(0x1b01) // LongHorn01
)

type Message struct {
	Complete chan struct{}

	MagicVersion uint16
	Seq          uint32
	Type         uint32
	Offset       int64
	Size         uint32
	Data         []byte
	transportErr error

	ID journal.OpID //Seq and ID can apparently be collapsed into one (ID)
}
