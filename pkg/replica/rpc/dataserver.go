package rpc

import (
	"net"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/replica"
)

type DataServer struct {
	address    string
	s          *replica.Server
	hostSuffix string
}

func NewDataServer(address string, s *replica.Server, hostSuffix string) *DataServer {

	return &DataServer{
		address:    address,
		s:          s,
		hostSuffix: hostSuffix,
	}
}

func (s *DataServer) verifyIP(addr net.Addr) bool {
	// If the host suffix wasn't provided, allow all connections.
	if len(s.hostSuffix) == 0 {
		return true
	}

	remoteIP, _, err := net.SplitHostPort(addr.String())

	domains, err := net.LookupAddr(remoteIP)

	if err != nil {
		logrus.Errorf("unable to resolve %v: %v ", remoteIP, err)

		return true
	}

	for _, domain := range domains {
		if strings.HasSuffix(domain, s.hostSuffix) {
			return true
		}
	}

	return false
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

		remoteAddr := conn.RemoteAddr()

		logrus.Infof("New connection from: %v", remoteAddr)

		if !s.verifyIP(remoteAddr) {
			logrus.Errorf("%v is not allowed to connect", remoteAddr)
			conn.Close()
			continue
		}

		server := dataconn.NewServer(conn, s.s)

		server.Handle()

		logrus.Infof("Lost connection from: %v", conn.RemoteAddr())
	}
}
