package dataconn

import (
	"errors"
	"io"
	"math"
	"net"
	"time"

	"github.com/sirupsen/logrus"

	journal "github.com/longhorn/sparse-tools/stats"
)

var (
	//ErrRWTimeout r/w operation timeout
	ErrRWTimeout   = errors.New("r/w timeout")
	ErrPingTimeout = errors.New("Ping timeout")

	opTimeout = 8 * time.Second
)

//Client replica client
type Client struct {
	end          chan struct{}
	requests     chan *Message
	send         chan *Message
	responses    chan *Message
	seq          uint32
	messages     map[uint32]*Message
	wire         *Wire
	peerAddr     string
	err          error
	timeoutTimer *time.Timer
	iops         int
}

//NewClient replica client
func NewClient(conn net.Conn) *Client {
	c := &Client{
		wire:      NewWire(conn),
		peerAddr:  conn.RemoteAddr().String(),
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

//TargetID operation target ID
func (c *Client) TargetID() string {
	return c.peerAddr
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

//Ping replica client
func (c *Client) Ping() error {
	_, err := c.operation(TypePing, nil, 0)
	return err
}

func (c *Client) operation(op uint32, buf []byte, offset int64) (int, error) {
	msg := Message{
		Complete: make(chan struct{}, 1),
		Type:     op,
		Offset:   offset,
		Size:     uint32(len(buf)),
		Data:     nil,
	}

	if op == TypeWrite {
		msg.Data = buf
	}

	c.requests <- &msg

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
	return int(msg.Size), nil
}

//Close replica client
func (c *Client) Close() {
	c.wire.Close()
	c.end <- struct{}{}
}

func (c *Client) loop() {
	// Don't fire the timer initially.  It will be set to a shorter time
	// upon the first message, so it's set to the maximum duration.
	c.timeoutTimer = time.NewTimer(time.Duration(math.MaxInt64))

	defer close(c.send)

	for {
		select {
		case <-c.timeoutTimer.C:
			c.handleTimeout()
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
	journal.RemovePendingOp(req.ID, false)
	delete(c.messages, req.Seq)
	req.Type = TypeError
	req.Data = []byte(c.err.Error())
	req.Complete <- struct{}{}
}

func (c *Client) handleRequest(req *Message) {
	switch req.Type {
	case TypeRead:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpRead, int(req.Size))
	case TypeWrite:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpWrite, int(req.Size))
	case TypePing:
		req.ID = journal.InsertPendingOp(time.Now(), c.TargetID(), journal.OpPing, 0)
	}
	if c.err != nil {
		c.replyError(req)
		return
	}

	req.MagicVersion = MagicVersion
	req.Seq = c.nextSeq()
	c.messages[req.Seq] = req

	if req.Type == TypeRead || req.Type == TypeWrite {
		// Start a new timer when sending a new message
		// when there are no other reads or writes in
		// progress.
		if c.iops == 0 {
			if !c.timeoutTimer.Stop() {
				if len(c.timeoutTimer.C) > 0 {
					<-c.timeoutTimer.C
				}
			}
			c.timeoutTimer.Reset(opTimeout)
		}

		c.iops++
	}

	c.send <- req
}

// Timeout logic:
// When the first I/O operation starts, start the timer for opTimeout second.
// When an I/O operation completes, stop the timer.
// If there are more I/O operations in progress, restart the timer.

func (c *Client) handleTimeout() {
	c.err = ErrRWTimeout

	logrus.Errorf("R/W Timeout. No response received in %v.\n", opTimeout)

	// Terminate all in flight
	for _, msg := range c.messages {
		c.replyError(msg)
	}

	journal.PrintLimited(1000) //flush automatically upon timeout
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

		journal.RemovePendingOp(req.ID, true)
		delete(c.messages, resp.Seq)

		if req.Type == TypeRead || req.Type == TypeWrite {
			// We received a response to an I/O operation
			// Stop the timer.
			if !c.timeoutTimer.Stop() {
				if len(c.timeoutTimer.C) > 0 {
					<-c.timeoutTimer.C
				}
			}
			c.iops--

			// If there are other reads and writes in
			// progress, wait opTimeout seconds for any
			// response to an I/O operation.
			if c.iops > 0 {
				c.timeoutTimer.Reset(opTimeout)
			}
		}

		req.Type = resp.Type
		req.Size = resp.Size
		req.Data = resp.Data
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
