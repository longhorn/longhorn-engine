package dataconn

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
)

type Wire struct {
	conn      net.Conn
	writer    *bufio.Writer
	reader    io.Reader
	byteOrder binary.ByteOrder
}

func NewWire(conn net.Conn) *Wire {
	var byteOrder binary.ByteOrder = binary.LittleEndian
	if runtime.GOARCH == "s390x" {
		byteOrder = binary.BigEndian
	}
	return &Wire{
		conn:      conn,
		writer:    bufio.NewWriterSize(conn, writeBufferSize),
		reader:    bufio.NewReaderSize(conn, readBufferSize),
		byteOrder: byteOrder,
	}
}

func (w *Wire) Write(msg *Message) error {
	if err := binary.Write(w.writer, w.byteOrder, msg.MagicVersion); err != nil {
		return err
	}
	if err := binary.Write(w.writer, w.byteOrder, msg.Seq); err != nil {
		return err
	}
	if err := binary.Write(w.writer, w.byteOrder, msg.Type); err != nil {
		return err
	}
	if err := binary.Write(w.writer, w.byteOrder, msg.Offset); err != nil {
		return err
	}
	if err := binary.Write(w.writer, w.byteOrder, msg.Size); err != nil {
		return err
	}
	if err := binary.Write(w.writer, w.byteOrder, uint32(len(msg.Data))); err != nil {
		return err
	}
	if len(msg.Data) > 0 {
		if _, err := w.writer.Write(msg.Data); err != nil {
			return err
		}
	}
	return w.writer.Flush()
}

func (w *Wire) Read() (*Message, error) {
	var (
		msg    Message
		length uint32
	)

	if err := binary.Read(w.reader, w.byteOrder, &msg.MagicVersion); err != nil {
		return nil, err
	}

	if msg.MagicVersion != MagicVersion {
		return nil, fmt.Errorf("wrong API version received: 0x%x", &msg.MagicVersion)
	}

	if err := binary.Read(w.reader, w.byteOrder, &msg.Seq); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, w.byteOrder, &msg.Type); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, w.byteOrder, &msg.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, w.byteOrder, &msg.Size); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, w.byteOrder, &length); err != nil {
		return nil, err
	}
	if length > 0 {
		msg.Data = make([]byte, length)
		if _, err := io.ReadFull(w.reader, msg.Data); err != nil {
			return nil, err
		}
	}

	return &msg, nil
}

func (w *Wire) Close() error {
	return w.conn.Close()
}
