package server

import (
	"bytes"
	"encoding/binary"
	"context"
	"errors"
	"io"
	"net"
	"time"

    "golang.org/x/sync/errgroup"

	"github.com/pojntfx/go-nbd/pkg/backend"
	"github.com/pojntfx/go-nbd/pkg/protocol"
)

var (
	ErrInvalidMagic     = errors.New("invalid magic")
	ErrInvalidBlocksize = errors.New("invalid blocksize")
)

const (
	maximumPacketSize = 32 * 1024 * 1024 // Support for a 32M maximum packet size is expected: https://sourceforge.net/p/nbd/mailman/message/35081223/
)

type Export struct {
	Name        string
	Description string

	Backend backend.Backend
}

type Options struct {
	ReadOnly           bool
	MinimumBlockSize   uint32
	PreferredBlockSize uint32
	MaximumBlockSize   uint32
}

func Handle(conn net.Conn, exports []*Export, options *Options) error {
	if options == nil {
		options = &Options{
			ReadOnly: false,
		}
	}

	if options.MinimumBlockSize == 0 {
		options.MinimumBlockSize = 1
	}

	if options.PreferredBlockSize == 0 {
		options.PreferredBlockSize = 4096
	}

	if options.MaximumBlockSize == 0 {
		options.MaximumBlockSize = maximumPacketSize
	}

	// Negotiation
	if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationNewstyleHeader{
		OldstyleMagic:  protocol.NEGOTIATION_MAGIC_OLDSTYLE,
		OptionMagic:    protocol.NEGOTIATION_MAGIC_OPTION,
		HandshakeFlags: protocol.NEGOTIATION_HANDSHAKE_FLAG_FIXED_NEWSTYLE,
	}); err != nil {
		return err
	}

	_, err := io.CopyN(io.Discard, conn, 4) // Discard client flags (uint32)
	if err != nil {
		return err
	}

	var export *Export
n:
	for {
		var optionHeader protocol.NegotiationOptionHeader
		if err := binary.Read(conn, binary.BigEndian, &optionHeader); err != nil {
			return err
		}

		if optionHeader.OptionMagic != protocol.NEGOTIATION_MAGIC_OPTION {
			return ErrInvalidMagic
		}

		switch optionHeader.ID {
		case protocol.NEGOTIATION_ID_OPTION_INFO, protocol.NEGOTIATION_ID_OPTION_GO:
			var exportNameLength uint32
			if err := binary.Read(conn, binary.BigEndian, &exportNameLength); err != nil {
				return err
			}

			exportName := make([]byte, exportNameLength)
			if _, err := io.ReadFull(conn, exportName); err != nil {
				return err
			}

			for _, candidate := range exports {
				if candidate.Name == string(exportName) {
					export = candidate

					break
				}
			}

			if export == nil {
				if length := int64(optionHeader.Length) - 4 - int64(exportNameLength); length > 0 { // Discard the option's data, minus the export name length and export name we've already read
					_, err := io.CopyN(io.Discard, conn, length)
					if err != nil {
						return err
					}
				}

				if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
					ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       protocol.NEGOTIATION_TYPE_REPLY_ERR_UNKNOWN,
					Length:     0,
				}); err != nil {
					return err
				}

				break
			}

			size, err := export.Backend.Size()
			if err != nil {
				return err
			}

			{
				var informationRequestCount uint16
				if err := binary.Read(conn, binary.BigEndian, &informationRequestCount); err != nil {
					return err
				}

				_, err := io.CopyN(io.Discard, conn, 2*int64(informationRequestCount)) // Discard information requests (uint16s)
				if err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, protocol.NegotiationReplyInfo{
					Type:              protocol.NEGOTIATION_TYPE_INFO_EXPORT,
					Size:              uint64(size),
					TransmissionFlags: 0b1000_0001_0000_0000,
				}); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
					ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       protocol.NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, protocol.NegotiationReplyNameHeader{
					Type: protocol.NEGOTIATION_TYPE_INFO_NAME,
				}); err != nil {
					return err
				}

				if _, err := info.Write([]byte(exportName)); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
					ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       protocol.NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, protocol.NegotiationReplyDescriptionHeader{
					Type: protocol.NEGOTIATION_TYPE_INFO_DESCRIPTION,
				}); err != nil {
					return err
				}

				if err := binary.Write(info, binary.BigEndian, []byte(export.Description)); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
					ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       protocol.NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, protocol.NegotiationReplyBlockSize{
					Type:               protocol.NEGOTIATION_TYPE_INFO_BLOCKSIZE,
					MinimumBlockSize:   options.MinimumBlockSize,
					PreferredBlockSize: options.PreferredBlockSize,
					MaximumBlockSize:   options.MaximumBlockSize,
				}); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
					ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       protocol.NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
				ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       protocol.NEGOTIATION_TYPE_REPLY_ACK,
				Length:     0,
			}); err != nil {
				return err
			}

			if optionHeader.ID == protocol.NEGOTIATION_ID_OPTION_GO {
				break n
			}
		case protocol.NEGOTIATION_ID_OPTION_ABORT:
			if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
				ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       protocol.NEGOTIATION_TYPE_REPLY_ACK,
				Length:     0,
			}); err != nil {
				return err
			}

			return nil
		case protocol.NEGOTIATION_ID_OPTION_LIST:
			{
				info := &bytes.Buffer{}

				for _, export := range exports {
					exportName := []byte(export.Name)

					if err := binary.Write(info, binary.BigEndian, uint32(len(exportName))); err != nil {
						return err
					}

					if err := binary.Write(info, binary.BigEndian, exportName); err != nil {
						return err
					}
				}

				if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
					ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       protocol.NEGOTIATION_TYPE_REPLY_SERVER,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
				ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       protocol.NEGOTIATION_TYPE_REPLY_ACK,
				Length:     0,
			}); err != nil {
				return err
			}
		default:
			_, err := io.CopyN(io.Discard, conn, int64(optionHeader.Length)) // Discard the unknown option's data
			if err != nil {
				return err
			}

			if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
				ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       protocol.NEGOTIATION_TYPE_REPLY_ERR_UNSUPPORTED,
				Length:     0,
			}); err != nil {
				return err
			}
		}
	}

	// Transmission
	ctx, _ := context.WithCancel(context.Background())
	group, _ := errgroup.WithContext(ctx)
	responseCh := make(chan *Response, 1024)
	group.Go(func() error {
		return Writer(ctx, conn, responseCh)
	})
	group.Go(func() error {
		return Reader(ctx, conn, export, options, responseCh)
	})

	if err := group.Wait(); err != nil {
		return err
	}
	return nil
}

type Response struct {
	Handle  uint64
	Data    []byte
	Offset  uint64
	Error   uint32
}

func Reader(ctx context.Context, conn net.Conn, export *Export, options *Options, responseCh chan<- *Response) error {
	defer conn.Close()

	var requestHeader protocol.TransmissionRequestHeader

	for {
		if err := conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			return err
		}
		if err := binary.Read(conn, binary.BigEndian, &requestHeader); err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return err
			}

		}

		if requestHeader.RequestMagic != protocol.TRANSMISSION_MAGIC_REQUEST {
			return ErrInvalidMagic
		}

		if requestHeader.Length > maximumPacketSize {
			return ErrInvalidBlocksize
		}

		switch requestHeader.Type {
		case protocol.TRANSMISSION_TYPE_REQUEST_READ:
			resp := &Response{
				Handle: requestHeader.Handle,
				Data:   make([]byte, requestHeader.Length),
				Offset: requestHeader.Offset,
			}
			go HandleRead(export, responseCh, resp)
		case protocol.TRANSMISSION_TYPE_REQUEST_WRITE:
			if options.ReadOnly {
				_, err := io.CopyN(io.Discard, conn, int64(requestHeader.Length)) // Discard the write command's data
				if err != nil {
					return err
				}

				responseCh <- &Response{
					Handle: requestHeader.Handle,
					Error: 	protocol.TRANSMISSION_ERROR_EPERM,
				}
				break
			}

			resp := &Response{
				Handle: requestHeader.Handle,
				Data:   make([]byte, requestHeader.Length),
				Offset: requestHeader.Offset,
			}
			n, err := io.ReadAtLeast(conn, resp.Data, int(requestHeader.Length))
			if err != nil {
				return err
			}
			if n != int(requestHeader.Length) {
				resp.Data = resp.Data[:n]
			}
			go HandleWrite(export, responseCh, resp)
		case protocol.TRANSMISSION_TYPE_REQUEST_DISC:
			if !options.ReadOnly {
				if err := export.Backend.Sync(); err != nil {
					return err
				}
			}

			return nil
		default:
			_, err := io.CopyN(io.Discard, conn, int64(requestHeader.Length)) // Discard the unknown command's data
			if err != nil {
				return err
			}

			responseCh <- &Response{
				Handle: requestHeader.Handle,
				Error: 	protocol.TRANSMISSION_ERROR_EINVAL,
			}
		}
	}
	return nil
}

func Writer(ctx context.Context, conn net.Conn, responseCh <-chan *Response) error {
	defer conn.Close()

	header := protocol.TransmissionReplyHeader{
		ReplyMagic: protocol.TRANSMISSION_MAGIC_REPLY,
		Error:      0,
		Handle:     0,
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case resp := <-responseCh:
			header.Handle = resp.Handle
			header.Error = resp.Error
			if err := binary.Write(conn, binary.BigEndian, header); err != nil {
				return err
			}
			if resp.Error == 0 && resp.Data != nil {
				if _, err := conn.Write(resp.Data); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func HandleRead(export *Export, responses chan<- *Response, resp *Response) {
	n, err := export.Backend.ReadAt(resp.Data, int64(resp.Offset))
	if err != nil {
		resp.Error = protocol.TRANSMISSION_ERROR_EIO
		responses <- resp
		return
	}
	if n != len(resp.Data) {
		resp.Data = resp.Data[:n]
	}
	responses <- resp
}

func HandleWrite(export *Export, responses chan<- *Response, resp *Response) {
	if _, err := export.Backend.WriteAt(resp.Data, int64(resp.Offset)); err != nil {
		resp.Error = protocol.TRANSMISSION_ERROR_EIO
		responses <- resp
		return
	}
	resp.Data = nil
	responses <- resp
}

