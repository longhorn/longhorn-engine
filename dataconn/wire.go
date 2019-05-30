package dataconn

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Wire struct {
	conn   net.Conn
	writer *bufio.Writer
	reader io.Reader
}

func NewWire(conn net.Conn) *Wire {
	return &Wire{
		conn:   conn,
		writer: bufio.NewWriterSize(conn, writeBufferSize),
		reader: bufio.NewReaderSize(conn, readBufferSize),
	}
}

func (w *Wire) Write(msg *Message) error {
	if err := binary.Write(w.writer, binary.LittleEndian, msg.MagicVersion); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, msg.Seq); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, msg.Type); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, msg.Offset); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, msg.Size); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(msg.Data))); err != nil {
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

	if err := binary.Read(w.reader, binary.LittleEndian, &msg.MagicVersion); err != nil {
		return nil, err
	}

	if msg.MagicVersion != MagicVersion {
		return nil, fmt.Errorf("Wrong API version received: 0x%x", &msg.MagicVersion)
	}

	if err := binary.Read(w.reader, binary.LittleEndian, &msg.Seq); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, binary.LittleEndian, &msg.Type); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, binary.LittleEndian, &msg.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, binary.LittleEndian, &msg.Size); err != nil {
		return nil, err
	}
	if err := binary.Read(w.reader, binary.LittleEndian, &length); err != nil {
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
