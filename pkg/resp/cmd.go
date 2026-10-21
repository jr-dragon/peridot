package resp

import (
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	// cmdArgsCacheCount is defined as 8 because the "SET" redis
	// command limits in 8 arguments.
	//
	// For variadic commands (eg. "MSET", "LPUSH", ...), it should
	// alllocs by make([]BulkString, 0, length).
	cmdArgsCacheCount = 8
)

type Command struct {
	rd *Reader

	// argsCache avoids allocating a new argument slice with make on every read.
	argsCache [cmdArgsCacheCount]BulkString

	// byteCache avoids allocating a new byte slice with make on every read.
	// Its size is 4 KiB minus the space occupied by rd and argsCache, keeping
	// the entire Command aligned to 4 KiB.
	byteCache [1<<12 - 200]byte
}

func NewCommand(rd io.Reader) *Command {
	return &Command{rd: NewReader(rd)}
}

// Read is a fast path of reading []BulkString from Redis Commands.
//
// The (*Reader).Read() returns (Value, error), but Value is an
// interface which allocs memory on heap and costs on casting.
// This reciever returns ([]BulkString, error) directly instead.
// The returned slice may be reused by the next Read. Copy it before retaining
// it; individual BulkString values can be retained without copying their data.
func (c *Command) Read() ([]BulkString, error) {
	// The borrowed argument slice expires at the next Read, including inline
	// requests and EOF. Clear references before potentially blocking for input.
	clear(c.argsCache[:])
	for {
		first, err := c.rd.rd.Peek(1)
		if err != nil {
			return nil, err
		}
		multibulk := first[0] == MAGIC_ARRAY
		header, err := c.rd.readLine()
		if err != nil {
			if errors.Is(err, errHeaderTooLong) {
				reason := "too big inline request"
				if multibulk {
					reason = "too big mbulk count string"
				}
				return nil, commandProtocolError(reason, err)
			}
			return nil, err
		}

		if !multibulk {
			args, err := parseInline(header)
			if err != nil {
				return nil, commandProtocolError("unbalanced quotes in request", err)
			}
			if len(args) == 0 {
				continue
			}
			return args, nil
		}

		if err := validateHeader(header); err != nil {
			return nil, commandProtocolError("invalid multibulk length", err)
		}
		sz, err := parseSignedLength(header[1 : len(header)-2])
		if err != nil || sz > math.MaxInt32 {
			return nil, commandProtocolError("invalid multibulk length", errInvalidHeader)
		}
		if sz <= 0 {
			continue
		}

		var args []BulkString
		if sz <= cmdArgsCacheCount {
			args = c.argsCache[:0:int(sz)]
		} else {
			args = make([]BulkString, 0, int(min(sz, 1024)))
		}

		for range sz {
			h, err := c.rd.readLine()
			if err != nil {
				if errors.Is(err, errHeaderTooLong) {
					return nil, commandProtocolError("too big bulk count string", err)
				}
				if err == io.EOF {
					return nil, errUnexpectedEOF
				}
				return nil, err
			}
			if h[0] != MAGIC_BULK_STRING {
				return nil, commandProtocolError(fmt.Sprintf("expected '$', got '%c'", h[0]), errInvalidHeader)
			}
			arg, err := c.buildBulkString(h, c.rd.rd)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}
		return args, nil
	}
}

func (c *Command) buildBulkString(header []byte, rd io.Reader) (BulkString, error) {
	if err := validateHeader(header); err != nil {
		return BulkString{}, commandProtocolError("invalid bulk length", err)
	}
	sz, err := parseLength(header[1 : len(header)-2])
	if err != nil || sz > maxBulkLength {
		return BulkString{}, commandProtocolError("invalid bulk length", errInvalidHeader)
	}

	var buf []byte
	if sz+2 <= len(c.byteCache) {
		buf = c.byteCache[:sz+2]
	} else {
		buf = make([]byte, sz+2)
	}

	if _, err := io.ReadFull(rd, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return BulkString{}, errUnexpectedEOF
		}
		return BulkString{}, err
	}
	if len(buf) < 2 || buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return BulkString{}, errUnexpectedSentinel
	}

	return BulkString{data: string(buf[:len(buf)-2])}, nil
}
