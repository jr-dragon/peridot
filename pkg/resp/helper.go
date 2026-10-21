package resp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	maxBulkLength          = 512 << 20
	aggrBufferLowWatermark = 1 << 10
	aggrBufferGrowSize     = 4 << 10
	numericBufferMinSize   = 24
)

// numericBuffer returns spare buffer storage for strconv's append functions.
// It reserves at least 24 bytes, enough for any base-10 int64 or float64 text.
func numericBuffer(buf *bytes.Buffer) []byte {
	if buf.Available() < numericBufferMinSize {
		buf.Grow(numericBufferMinSize)
	}

	return buf.AvailableBuffer()
}

// growAggrBuffer maintains a 1 KiB low-water mark for aggregate writes. When
// space falls below it, Grow reserves at least 4 KiB to amortize allocations.
func growAggrBuffer(buf *bytes.Buffer) {
	if buf.Available() < aggrBufferLowWatermark {
		buf.Grow(aggrBufferGrowSize)
	}
}

func readBlob(header []byte, rd io.Reader) ([]byte, error) {
	if err := validateHeader(header); err != nil {
		return nil, err
	}

	sz, err := parseLength(header[1 : len(header)-2])
	if err != nil {
		return nil, err
	}
	if sz > maxBulkLength {
		return nil, fmt.Errorf("%w: length too long", errInvalidHeader)
	}
	if rd == nil {
		return nil, errUnexpectedEOF
	}

	buf := make([]byte, sz+2)
	if _, err := io.ReadFull(rd, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errUnexpectedEOF
		}
		return nil, err
	}
	if len(buf) < 2 || buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return nil, errUnexpectedSentinel
	}

	return buf[:len(buf)-2], nil
}

func readMapEntries(header []byte, rd io.Reader) ([]MapEntry, error) {
	if err := validateHeader(header); err != nil {
		return nil, err
	}

	length, err := parseLength(header[1 : len(header)-2])
	if err != nil {
		return nil, err
	}

	entries := make([]MapEntry, 0, min(length, 1024))
	if length == 0 {
		return entries, nil
	}
	if rd == nil {
		return entries, errUnexpectedEOF
	}

	vrd := newAggregateReader(rd)
	for range length {
		key, err := vrd.readElement()
		if err != nil {
			return entries, err
		}
		value, err := vrd.readElement()
		if err != nil {
			return entries, err
		}
		entries = append(entries, MapEntry{Key: key, Value: value})
	}

	return entries, nil
}

func readValues(header []byte, rd io.Reader) ([]Value, error) {
	if err := validateHeader(header); err != nil {
		return nil, err
	}

	length, err := parseLength(header[1 : len(header)-2])
	if err != nil {
		return nil, err
	}

	values := make([]Value, 0, min(length, 1024))
	if length == 0 {
		return values, nil
	}
	if rd == nil {
		return values, errUnexpectedEOF
	}

	vrd := newAggregateReader(rd)
	for range length {
		value, err := vrd.readElement()
		if err != nil {
			return values, err
		}
		values = append(values, value)
	}

	return values, nil
}

func parseLength(header []byte) (int, error) {
	n, err := parseSignedLength(header)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, errNegativeLength
	}
	if int64(int(n)) != n {
		return 0, errInvalidHeader
	}
	return int(n), nil
}

// parseSignedLength accepts the canonical decimal int64 syntax used by Redis
// string2ll: no plus sign, leading zeroes, negative zero, or whitespace.
func parseSignedLength(header []byte) (int64, error) {
	digits := header
	if len(digits) > 0 && digits[0] == '-' {
		digits = digits[1:]
	}
	if len(digits) == 0 || (digits[0] == '0' && (len(digits) != 1 || len(header) != 1)) {
		return 0, errInvalidHeader
	}
	for _, b := range digits {
		if b < '0' || b > '9' {
			return 0, errInvalidHeader
		}
	}
	l, err := strconv.ParseInt(string(header), 10, 64)
	if err != nil {
		return 0, errInvalidHeader
	}
	return l, nil
}

func validateHeader(header []byte) error {
	if len(header) < 3 || !bytes.HasSuffix(header, []byte(SENTINEL)) || bytes.ContainsAny(header[:len(header)-2], "\r\n") {
		return fmt.Errorf("%w: %q", errInvalidHeader, header)
	}
	return nil
}

func validateType(header []byte, magic byte) error {
	if err := validateHeader(header); err != nil {
		return err
	}
	if header[0] != magic {
		return errInvalidHeader
	}
	return nil
}

func writeSimpleText(buf *bytes.Buffer, text []byte) {
	if !bytes.ContainsAny(text, "\r\n") {
		buf.Write(text)
		return
	}
	for _, b := range text {
		if b == '\r' || b == '\n' {
			b = ' '
		}
		buf.WriteByte(b)
	}
}
