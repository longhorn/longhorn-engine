package dataconn

import (
	"errors"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

const (
	queueLength = 4196
)

// Client replica client
type Client struct {
	end            chan struct{}
	requests       chan *Message
	send           chan *Message
	responses      chan *Message
	seq            uint32
	messages       [queueLength]*Message
	SeqChan        chan uint32
	wires          []*Wire
	peerAddr       string
	sharedTimeouts types.SharedTimeouts
}

// NewClient replica client
func NewClient(conns []net.Conn, sharedTimeouts types.SharedTimeouts) *Client {
	var wires []*Wire
	for _, conn := range conns {
		wires = append(wires, NewWire(conn))
	}

	c := &Client{
		wires:          wires,
		peerAddr:       conns[0].RemoteAddr().String(),
		end:            make(chan struct{}, 1024),
		requests:       make(chan *Message, 1024),
		send:           make(chan *Message, 1024),
		responses:      make(chan *Message, 1024),
		messages:       [queueLength]*Message{},
		SeqChan:        make(chan uint32, queueLength),
		sharedTimeouts: sharedTimeouts,
	}
	for i := uint32(0); i < queueLength; i++ {
		c.SeqChan <- i
	}
	c.write()
	c.read()
	return c
}

// TargetID operation target ID
func (c *Client) TargetID() string {
	return c.peerAddr
}

// WriteAt replica client
func (c *Client) WriteAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeWrite, buf, uint32(len(buf)), offset)
}

// UnmapAt replica client
func (c *Client) UnmapAt(length uint32, offset int64) (int, error) {
	return c.operation(TypeUnmap, nil, length, offset)
}

// SetError replica client transport error
func (c *Client) SetError(err error) {
	c.responses <- &Message{
		transportErr: err,
	}
}

// ReadAt replica client
func (c *Client) ReadAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeRead, buf, uint32(len(buf)), offset)
}

// Ping replica client
func (c *Client) Ping() error {
	_, err := c.operation(TypePing, nil, 0, 0)
	return err
}

func (c *Client) operation(op uint32, buf []byte, length uint32, offset int64) (int, error) {
	msg := Message{
		Complete: make(chan struct{}, 1),
		Type:     op,
		Offset:   offset,
		Size:     length,
		Data:     nil,
	}

	if op == TypeWrite {
		msg.Data = buf
	}

	c.handleRequest(&msg)

	<-msg.Complete
	// Only copy the message if a read is requested
	if op == TypeRead && (msg.Type == TypeResponse || msg.Type == TypeEOF) {
		copy(buf, msg.Data)
	}
	if msg.Type == TypeError {
		return 0, errors.New(string(msg.Data))
	}
	if msg.Type == TypeEOF {
		return int(msg.Size), io.EOF
	}

	c.SeqChan <- msg.Seq

	return int(msg.Size), nil
}

// Close replica client
func (c *Client) Close() {
	for _, wire := range c.wires {
		wire.Close()
	}
	c.end <- struct{}{}
}

func (c *Client) nextSeq() uint32 {
	c.seq++
	return c.seq
}

func (c *Client) replyError(req *Message, err error) {
	req.Type = TypeError
	req.Data = []byte(err.Error())
	req.Complete <- struct{}{}
}

func (c *Client) handleRequest(req *Message) {
	req.MagicVersion = MagicVersion

	req.Seq = <-c.SeqChan

	c.messages[req.Seq] = req
	c.send <- req
}

func (c *Client) handleResponse(resp *Message) {
	req := c.messages[resp.Seq]

	req.Type = resp.Type
	req.Size = resp.Size
	req.Data = resp.Data
	req.Complete <- struct{}{}

}

func (c *Client) write() {
	for _, wire := range c.wires {
		go func(w *Wire) {
			for msg := range c.send {
				if err := w.Write(msg); err != nil {
					c.responses <- &Message{
						transportErr: err,
					}
				}
			}
		}(wire)
	}
}

func (c *Client) read() {
	for _, wire := range c.wires {
		go func(w *Wire) {
			for {
				msg, err := w.Read()
				if err != nil {
					logrus.WithError(err).Errorf("Error reading from wire %v", c.peerAddr)
					c.responses <- &Message{
						transportErr: err,
					}
					break
				}
				c.handleResponse(msg)
			}
		}(wire)
	}
}
