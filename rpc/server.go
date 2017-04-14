package rpc

import (
	"io"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-engine/types"
)

type Server struct {
	wire      *Wire
	responses chan *Message
	done      chan struct{}
	data      types.DataProcessor
}

func NewServer(conn net.Conn, data types.DataProcessor) *Server {
	return &Server{
		wire:      NewWire(conn),
		responses: make(chan *Message, 1024),
		done:      make(chan struct{}, 5),
		data:      data,
	}
}

func (s *Server) Handle() error {
	go s.write()
	defer func() {
		s.done <- struct{}{}
	}()
	return s.read()
}

func (s *Server) readFromWire(ret chan<- error) {
	msg, err := s.wire.Read()
	if err == io.EOF {
		ret <- err
		return
	} else if err != nil {
		logrus.Errorf("Failed to read: %v", err)
		ret <- err
		return
	}
	switch msg.Type {
	case TypeRead:
		go s.handleRead(msg)
	case TypeWrite:
		go s.handleWrite(msg)
	case TypePing:
		go s.handlePing(msg)
	}
	ret <- nil
}

func (s *Server) read() error {
	ret := make(chan error)
	for {
		go s.readFromWire(ret)

		select {
		case err := <-ret:
			if err != nil {
				return err
			}
			continue
		case <-s.done:
			logrus.Debugf("RPC server stopped")
			return nil
		}
	}
}

func (s *Server) Stop() {
	s.done <- struct{}{}
}

func (s *Server) handleRead(msg *Message) {
	msg.Data = make([]byte, msg.Size)
	c, err := s.data.ReadAt(msg.Data, msg.Offset)
	s.pushResponse(c, msg, err)
}

func (s *Server) handleWrite(msg *Message) {
	c, err := s.data.WriteAt(msg.Data, msg.Offset)
	s.pushResponse(c, msg, err)
}

func (s *Server) handlePing(msg *Message) {
	err := s.data.PingResponse()
	s.pushResponse(0, msg, err)
}

func (s *Server) pushResponse(count int, msg *Message, err error) {
	msg.MagicVersion = MagicVersion
	msg.Size = uint32(len(msg.Data))
	if msg.Type == TypeWrite {
		msg.Data = nil
	}

	msg.Type = TypeResponse
	if err == io.EOF {
		msg.Type = TypeEOF
		msg.Data = msg.Data[:count]
		msg.Size = uint32(len(msg.Data))
	} else if err != nil {
		msg.Type = TypeError
		msg.Data = []byte(err.Error())
		msg.Size = uint32(len(msg.Data))
	}
	s.responses <- msg
}

func (s *Server) write() {
	for {
		select {
		case msg := <-s.responses:
			if err := s.wire.Write(msg); err != nil {
				logrus.Errorf("Failed to write: %v", err)
			}
		case <-s.done:
			msg := &Message{
				Type: TypeClose,
			}
			//Best effort to notify client to close connection
			s.wire.Write(msg)
			break
		}
	}
}
