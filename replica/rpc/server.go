package rpc

import (
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-engine/replica"
	"github.com/rancher/longhorn-engine/rpc"
)

type Server struct {
	address string
	s       *replica.Server
}

func New(address string, s *replica.Server) *Server {
	return &Server{
		address: address,
		s:       s,
	}
}

func (s *Server) ListenAndServe() error {
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
			server := rpc.NewServer(conn, s.s)
			server.Handle()
		}(conn)
	}
}
