package rpc

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
)

var (
	//ErrRWTimeout r/w operation timeout
	ErrRWTimeout = errors.New("r/w timeout")

	opRetries      = 4
	opReadTimeout  = 15 * time.Second // client read
	opWriteTimeout = 15 * time.Second // client write
)

//Client replica client
type Client struct {
	end       chan struct{}
	requests  chan *Message
	send      chan *Message
	responses chan *Message
	seq       uint32
	messages  map[uint32]*Message
	wire      *Wire
	err       error
}

//NewClient replica client
func NewClient(conn net.Conn) *Client {
	c := &Client{
		wire:      NewWire(conn),
		end:       make(chan struct{}, 1024),
		requests:  make(chan *Message, 1024),
		send:      make(chan *Message, 1024),
		responses: make(chan *Message, 1024),
		messages:  map[uint32]*Message{},
	}
	go c.loop()
	go c.write()
	go c.read()
	return c
}

//WriteAt replica client
func (c *Client) WriteAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeWrite, buf, offset)
}

//SetError replica client transport error
func (c *Client) SetError(err error) {
	c.responses <- &Message{
		transportErr: err,
	}
}

//ReadAt replica client
func (c *Client) ReadAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeRead, buf, offset)
}

func (c *Client) operation(op uint32, buf []byte, offset int64) (int, error) {
	retry := 0
	for {
		msg := Message{
			Complete: make(chan struct{}, 1),
			Type:     op,
			Offset:   offset,
			Data:     buf,
		}

		timeout := func(op uint32) <-chan time.Time {
			switch op {
			case TypeRead:
				return time.After(opReadTimeout)
			}
			return time.After(opWriteTimeout)
		}(msg.Type)

		c.requests <- &msg

		select {
		case <-msg.Complete:
			if msg.Type == TypeError {
				return 0, errors.New(string(msg.Data))
			}
			if msg.Type == TypeEOF {
				return len(msg.Data), io.EOF
			}
			return len(msg.Data), nil
		case <-timeout:
			switch msg.Type {
			case TypeRead:
				logrus.Errorln("Read timeout: seq=", msg.Seq, "size=", len(msg.Data)/1024, "(kB)")
				if retry < opRetries {
					retry++
					logrus.Errorln("Retry", retry, "seq=", msg.Seq, "size=", len(msg.Data)/1024, "(kB)")
				} else {
					c.SetError(ErrRWTimeout)
					return 0, ErrRWTimeout
				}
			case TypeWrite:
				logrus.Errorln("Write timeout: seq=", msg.Seq, "size=", len(msg.Data)/1024, "(kB)")
				c.SetError(ErrRWTimeout)
				return 0, ErrRWTimeout
			}
		}
	}
}

//Close replica client
func (c *Client) Close() {
	c.wire.Close()
	c.end <- struct{}{}
}

func (c *Client) loop() {
	defer close(c.send)

	for {
		select {
		case <-c.end:
			return
		case req := <-c.requests:
			c.handleRequest(req)
		case resp := <-c.responses:
			c.handleResponse(resp)
		}
	}
}

func (c *Client) nextSeq() uint32 {
	c.seq++
	return c.seq
}

func (c *Client) replyError(req *Message) {
	delete(c.messages, req.Seq)
	req.Type = TypeError
	req.Data = []byte(c.err.Error())
	req.Complete <- struct{}{}
}

func (c *Client) handleRequest(req *Message) {
	if c.err != nil {
		c.replyError(req)
		return
	}

	req.Seq = c.nextSeq()
	c.messages[req.Seq] = req
	c.send <- req
}

func (c *Client) handleResponse(resp *Message) {
	if resp.transportErr != nil {
		c.err = resp.transportErr
		// Terminate all in flight
		for _, msg := range c.messages {
			c.replyError(msg)
		}
		return
	}

	if req, ok := c.messages[resp.Seq]; ok {
		if c.err != nil {
			c.replyError(req)
			return
		}

		delete(c.messages, resp.Seq)
		// can probably optimize away this copy
		if len(resp.Data) > 0 {
			copy(req.Data, resp.Data)
		}
		req.Type = resp.Type
		req.Complete <- struct{}{}
	}
}

func (c *Client) write() {
	for msg := range c.send {
		if err := c.wire.Write(msg); err != nil {
			c.responses <- &Message{
				transportErr: err,
			}
		}
	}
}

func (c *Client) read() {
	for {
		msg, err := c.wire.Read()
		if err != nil {
			logrus.Errorf("Error reading from wire: %v", err)
			c.responses <- &Message{
				transportErr: err,
			}
			break
		}
		c.responses <- msg
	}
}
