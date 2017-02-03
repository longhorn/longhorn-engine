package types

import "io"

const (
	WO  = Mode("WO")
	RW  = Mode("RW")
	ERR = Mode("ERR")

	StateUp   = State("Up")
	StateDown = State("Down")
)

type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

type DiffDisk interface {
	ReaderWriterAt
	io.Closer
	Fd() uintptr
}

type Backend interface {
	ReaderWriterAt
	io.Closer
	Snapshot(name string) error
	Size() (int64, error)
	SectorSize() (int64, error)
	RemainSnapshots() (int, error)
	GetRevisionCounter() (int64, error)
	SetRevisionCounter(counter int64) error
}

type BackendFactory interface {
	Create(address string) (Backend, error)
}

type Controller interface {
	AddReplica(address string) error
	RemoveReplica(address string) error
	SetReplicaMode(address string, mode Mode) error
	ListReplicas() []Replica
	Start(address ...string) error
	Shutdown() error
}

type Server interface {
	ReaderWriterAt
	Controller
}

type Mode string

type State string

type Replica struct {
	Address string
	Mode    Mode
}

type Frontend interface {
	Startup(name string, size, sectorSize int64, rw ReaderWriterAt) error
	Shutdown() error
	State() State
}
