package rpc

import (
	"fmt"
	"net"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

type DataServer struct {
	protocol types.DataServerProtocol
	address  string
	s        *replica.Server
}

func NewDataServer(protocol types.DataServerProtocol, address string, s *replica.Server) *DataServer {
	return &DataServer{
		protocol: protocol,
		address:  address,
		s:        s,
	}
}

func (s *DataServer) ListenAndServe() error {
	switch s.protocol {
	case types.DataServerProtocolTCP:
		return s.listenAndServeTCP()
	case types.DataServerProtocolUNIX:
		return s.listenAndServeUNIX()
	default:
		return fmt.Errorf("unsupported protocol: %v", s.protocol)
	}
}

func (s *DataServer) listenAndServeTCP() error {
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
			logrus.WithError(err).Error("failed to accept tcp connection")
			continue
		}

		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		go func(conn net.Conn) {
			server := dataconn.NewServer(conn, s.s)
			server.Handle()
		}(conn)
	}
}

func (s *DataServer) listenAndServeUNIX() error {
	unixAddr, err := net.ResolveUnixAddr("unix", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", unixAddr)
	if err != nil {
		return err
	}

	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			logrus.WithError(err).Error("failed to accept unix-domain-socket connection")
			continue
		}
		logrus.Infof("New connection from: %v", conn.RemoteAddr())
		go func(conn net.Conn) {
			server := dataconn.NewServer(conn, s.s)
			server.Handle()
		}(conn)
	}
}
