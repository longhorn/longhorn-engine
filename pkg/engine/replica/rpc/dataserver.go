package rpc

import (
	"net"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/engine/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/engine/replica"
)

type DataServer struct {
	address string
	s       *replica.Server
}

func NewDataServer(address string, s *replica.Server) *DataServer {
	return &DataServer{
		address: address,
		s:       s,
	}
}

func (s *DataServer) ListenAndServe() error {
	addr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			logrus.Errorf("failed to accept connection %v", err)
			continue
		}

		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		go func(conn net.Conn) {
			server := dataconn.NewServer(conn, s.s)
			server.Handle()
		}(conn)
	}
}
