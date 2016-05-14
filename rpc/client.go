package rpc

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
)

var (
	ErrPingTimeout = errors.New("Ping timeout")
)

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

func (c *Client) WriteAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeWrite, buf, offset)
}

func (c *Client) ReadAt(buf []byte, offset int64) (int, error) {
	return c.operation(TypeRead, buf, offset)
}

func (c *Client) Ping() error {
	ret := make(chan error, 1)
	go func() {
		_, err := c.operation(TypePing, nil, 0)
		ret <- err
	}()

	select {
	case err := <-ret:
		return err
	case <-time.After(2 * time.Second):
		return ErrPingTimeout
	}
}

func (c *Client) checkPing() {
	// don't ping if we know the connection is already bad
	if c.err != nil {
		return
	}

	if err := c.Ping(); err != nil {
		logrus.Errorf("Error monitoring wire: %v", err)
		c.responses <- &Message{
			transportErr: err,
		}
	}
}

func (c *Client) operation(op uint32, buf []byte, offset int64) (int, error) {
	msg := Message{
		Type:   op,
		Offset: offset,
		Data:   buf,
	}
	msg.Add(1)
	c.requests <- &msg
	msg.Wait()

	if msg.Type == TypeError {
		return 0, errors.New(string(msg.Data))
	}

	if msg.Type == TypeEOF {
		return len(msg.Data), io.EOF
	}

	return len(msg.Data), nil
}

func (c *Client) Close() {
	c.wire.Close()
	c.end <- struct{}{}
}

func (c *Client) loop() {
	defer close(c.send)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go c.checkPing()
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
	req.Done()
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
		logrus.Errorf("Got transport error: %v", c.err)
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
		req.Done()
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
