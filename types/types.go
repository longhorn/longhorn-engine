package types

import "io"

const (
	WO  = Mode("WO")
	RW  = Mode("RW")
	ERR = Mode("ERR")

	StateUp   = State("up")
	StateDown = State("down")

	AWSAccessKey = "AWS_ACCESS_KEY_ID"
	AWSSecretKey = "AWS_SECRET_ACCESS_KEY"
	AWSEndPoint  = "AWS_ENDPOINTS"
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

type MonitorChannel chan error

type Backend interface {
	ReaderWriterAt
	io.Closer
	Snapshot(name string, userCreated bool, created string, labels map[string]string) error
	Size() (int64, error)
	SectorSize() (int64, error)
	RemainSnapshots() (int, error)
	GetRevisionCounter() (int64, error)
	SetRevisionCounter(counter int64) error
	GetMonitorChannel() MonitorChannel
	StopMonitoring()
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
	FrontendName() string
	Startup(name string, size, sectorSize int64, rw ReaderWriterAt) error
	Shutdown() error
	State() State
	Endpoint() string
}

type DataProcessor interface {
	ReaderWriterAt
	PingResponse() error
}

const (
	EventTypeVolume  = "volume"
	EventTypeReplica = "replica"
	EventTypeMetrics = "metrics"
)

type Metrics struct {
	Bandwidth    RWMetrics // in byte
	TotalLatency RWMetrics // in microsecond(us)
	IOPS         RWMetrics
}

type RWMetrics struct {
	Read  uint64
	Write uint64
}
