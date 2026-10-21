package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// maxPendingHeaderLength matches Redis's limit for headers that have not yet
// received a newline. Bulk payloads are read separately and do not use this limit.
const maxPendingHeaderLength = 64 << 10

type Reader struct {
	rd  *bufio.Reader
	raw io.Reader
}

// newAggregateReader must not lose lookahead when a builder returns. Existing
// buffers remain owned by the caller; unbuffered sources are read exactly.
func newAggregateReader(rd io.Reader) *Reader {
	switch rd.(type) {
	case *bufio.Reader, *bufio.ReadWriter:
		return NewReader(rd)
	default:
		return &Reader{raw: rd}
	}
}

func (rd *Reader) input() io.Reader {
	if rd.raw != nil {
		return rd.raw
	}
	return rd.rd
}

func NewReader(rd io.Reader) *Reader {
	if buffered, ok := rd.(*bufio.Reader); ok {
		return &Reader{rd: buffered}
	}
	if buffered, ok := rd.(*bufio.ReadWriter); ok {
		return &Reader{rd: buffered.Reader}
	}

	return &Reader{rd: bufio.NewReader(rd)}
}

// Read reads one RESP frame. Top-level attributes are returned separately;
// attributes inside aggregates stay attached to the element they describe.
func (rd *Reader) Read() (Value, error) {
	header, err := rd.readHeader()
	if err != nil {
		return nil, err
	}

	switch header[0] {
	case MAGIC_SIMPLE_STRING:
		return BuildSimpleString(header)
	case MAGIC_SIMPLE_ERROR:
		return BuildSimpleError(header)
	case MAGIC_INTEGER:
		return BuildInteger(header)
	case MAGIC_BULK_STRING:
		return BuildBulkString(header, rd.input())
	case MAGIC_ARRAY:
		return BuildArray(header, rd.input())
	case MAGIC_NULL:
		if len(header) != 3 {
			return nil, errInvalidHeader
		}
		return Null{}, nil
	case MAGIC_BOOLEAN:
		return BuildBoolean(header)
	case MAGIC_DOUBLE:
		return BuildDouble(header)
	case MAGIC_BIG_NUMBER:
		return BuildBigNumber(header)
	case MAGIC_BULK_ERROR:
		return BuildBulkError(header, rd.input())
	case MAGIC_VERBATIM_STRING:
		return BuildVerbatimString(header, rd.input())
	case MAGIC_MAP:
		return BuildMap(header, rd.input())
	case MAGIC_ATTRIBUTE:
		return BuildAttribute(header, rd.input())
	case MAGIC_SET:
		return BuildSet(header, rd.input())
	case MAGIC_PUSH:
		return BuildPush(header, rd.input())
	default:
		return nil, fmt.Errorf("%w: unknown type %q", errInvalidHeader, header[0])
	}
}

func (rd *Reader) readHeader() ([]byte, error) {
	header, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if err := validateHeader(header); err != nil {
		return nil, err
	}
	return header, nil
}

// readLine bounds unfinished lines without imposing RESP framing on inline commands.
func (rd *Reader) readLine() ([]byte, error) {
	var header []byte
	var b [1]byte
	for {
		if rd.raw != nil {
			if _, err := io.ReadFull(rd.raw, b[:]); err != nil {
				if errors.Is(err, io.EOF) && len(header) != 0 {
					return nil, errUnexpectedEOF
				}
				return nil, err
			}
			if b[0] != '\n' && len(header) == maxPendingHeaderLength {
				return nil, errHeaderTooLong
			}
			header = append(header, b[0])
			if b[0] == '\n' {
				return header, nil
			}
			continue
		}
		// Refill only when necessary, inspecting each available fragment rather
		// than waiting for ReadBytes or ReadSlice to find a newline.
		if _, err := rd.rd.Peek(1); err != nil {
			if errors.Is(err, io.EOF) && len(header) != 0 {
				return nil, errUnexpectedEOF
			}
			return nil, err
		}
		fragment, _ := rd.rd.Peek(rd.rd.Buffered())
		newline := bytes.IndexByte(fragment, '\n')
		if newline >= 0 {
			fragment = fragment[:newline+1]
		} else if len(fragment) > maxPendingHeaderLength-len(header) {
			return nil, errHeaderTooLong
		}
		header = append(header, fragment...)
		rd.rd.Discard(len(fragment))

		if newline >= 0 {
			return header, nil
		}
	}
}

// readElement keeps attribute frames attached to the following aggregate element.
func (rd *Reader) readElement() (Value, error) {
	var attributes []Attribute
	for {
		value, err := rd.Read()
		if errors.Is(err, io.EOF) {
			return nil, errUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}
		if attribute, ok := value.(Attribute); ok {
			attributes = append(attributes, attribute)
			continue
		}
		if len(attributes) != 0 {
			return attributedValue{attributes: attributes, Value: value}, nil
		}
		return value, nil
	}
}
