package rpc

import (
	"io"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

type Server struct {
	wire      *Wire
	responses chan *Message
	done      chan struct{}
	data      types.ReaderWriterAt
}

func NewServer(conn net.Conn, data types.ReaderWriterAt) *Server {
	return &Server{
		wire:      NewWire(conn),
		responses: make(chan *Message, 1024),
		done:      make(chan struct{}),
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

func (s *Server) read() error {
	for {
		msg, err := s.wire.Read()
		if err == io.EOF {
			return err
		} else if err != nil {
			logrus.Errorf("Failed to read: %v", err)
			return err
		}
		switch msg.Type {
		case TypeRead:
			go s.handleRead(msg)
		case TypeWrite:
			go s.handleWrite(msg)
		case TypePing:
			go s.handlePong(msg)
		}
	}
}

func (s *Server) handleRead(msg *Message) {
	c, err := s.data.ReadAt(msg.Data, msg.Offset)
	s.pushResponse(c, msg, err)
}

func (s *Server) handleWrite(msg *Message) {
	c, err := s.data.WriteAt(msg.Data, msg.Offset)
	s.pushResponse(c, msg, err)
}

func (s *Server) handlePong(msg *Message) {
	s.pushResponse(0, msg, nil)
}

func (s *Server) pushResponse(count int, msg *Message, err error) {
	msg.Type = TypeResponse
	if err == io.EOF {
		msg.Data = msg.Data[:count]
		msg.Type = TypeEOF
	} else if err != nil {
		msg.Type = TypeError
		msg.Data = []byte(err.Error())
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
			break
		}
	}
}
