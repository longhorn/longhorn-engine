package dataconn

import (
	"fmt"
	"net"

	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/pojntfx/go-nbd/pkg/server"
)

type nbdServer struct {
	conn net.Conn
	s    *replica.Server
}

func NewNBDServer(conn net.Conn, s *replica.Server) *nbdServer {
	return &nbdServer{
		conn: conn,
		s:    s,
	}
}

func (s *nbdServer) Handle() {
	if err := server.Handle(
		s.conn,
		[]*server.Export{
			{
				Name:        "",
				Description: "",
				Backend:     s,
			},
		},
		&server.Options{
			ReadOnly:           false,
			MinimumBlockSize:   uint32(512),
			PreferredBlockSize: uint32(512),
			MaximumBlockSize:   uint32(512),
		}); err != nil {
		panic(err)
	}
}

func (b *nbdServer) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = b.s.ReadAt(p, off)
	return
}

func (b *nbdServer) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = b.s.WriteAt(p, off)
	return
}

func (b *nbdServer) Size() (int64, error) {
	_, info := b.s.Status()
	return info.Size, nil
}

func (b *nbdServer) Sync() error {
	return nil
}

func (b *nbdServer) Ping() error {
	state, info := b.s.Status()
	if state == types.ReplicaStateError {
		return fmt.Errorf("ping failure due to %v", info.Error)
	}
	if state != types.ReplicaStateOpen && state != types.ReplicaStateDirty && state != types.ReplicaStateRebuilding {
		return fmt.Errorf("ping failure: replica state %v", state)
	}
	return nil
}
