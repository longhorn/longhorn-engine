package dataconn

import (
	"fmt"
	"net"

	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/pojntfx/go-nbd/pkg/server"
)

type NbdServer struct {
	conn net.Conn
	s    *replica.Server
}

func NewNbdServer(conn net.Conn, s *replica.Server) *NbdServer {
	return &NbdServer{
		conn: conn,
		s:    s,
	}
}

func (s *NbdServer) Handle() {
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

func (s *NbdServer) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = s.s.ReadAt(p, off)
	return
}

func (s *NbdServer) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = s.s.WriteAt(p, off)
	return
}

func (s *NbdServer) Size() (int64, error) {
	_, info := s.s.Status()
	return info.Size, nil
}

func (s *NbdServer) Sync() error {
	return nil
}

func (s *NbdServer) Ping() error {
	state, info := s.s.Status()
	if state == types.ReplicaStateError {
		return fmt.Errorf("ping failure due to %v", info.Error)
	}
	if state != types.ReplicaStateOpen && state != types.ReplicaStateDirty && state != types.ReplicaStateRebuilding {
		return fmt.Errorf("ping failure: replica state %v", state)
	}
	return nil
}
