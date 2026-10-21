package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

var benchmarkReaderResult Value

func TestReadersRejectOversizedUnterminatedHeaders(t *testing.T) {
	const limit = 64 << 10
	for _, tc := range []struct {
		name    string
		prefix  string
		command bool
	}{
		{name: "inline command", prefix: "PING ", command: true},
		{name: "command array", prefix: "*", command: true},
		{name: "command bulk", prefix: "*1\r\n$", command: true},
		{name: "value header", prefix: "+"},
		{name: "nested value header", prefix: "*1\r\n+"},
	} {
		for _, bufferSize := range []int{16, 4096, 128 << 10} {
			t.Run(fmt.Sprintf("%s/buffer=%d", tc.name, bufferSize), func(t *testing.T) {
				// No newline or EOF follows the over-limit byte. The parser must
				// reject the header before asking the source for any more data.
				end := errors.New("read beyond oversized header")
				prefix := tc.prefix
				headerStart := strings.LastIndex(prefix, "\n") + 1
				wire := prefix + strings.Repeat("x", limit+1-(len(prefix)-headerStart))
				source := io.MultiReader(strings.NewReader(wire), failingReader{err: end})
				rd := bufio.NewReaderSize(source, bufferSize)
				var err error
				if tc.command {
					_, err = NewCommand(rd).Read()
				} else {
					_, err = NewReader(rd).Read()
				}
				if !errors.Is(err, ErrProtocol) || errors.Is(err, end) {
					t.Fatalf("Read() = %v, want protocol error before further input", err)
				}
			})
		}
	}
}

func TestReaderPendingHeaderLimitBoundary(t *testing.T) {
	const limit = 64 << 10
	for _, size := range []int{limit - 1, limit} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			prefix := "+" + strings.Repeat("x", size-1)
			// Redis allows exactly 64 KiB of unfinished header data while
			// waiting for the terminating newline.
			rd := NewReader(io.MultiReader(strings.NewReader(prefix), strings.NewReader("\r\n+NEXT\r\n")))
			value, err := rd.Read()
			if err != nil {
				t.Fatal(err)
			}
			assertMarshaledValue(t, value, prefix+"\r\n")
			next, err := rd.Read()
			if err != nil {
				t.Fatal(err)
			}
			assertMarshaledValue(t, next, "+NEXT\r\n")
		})
	}
}

func TestReader_Read(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "simple string", payload: []byte("+OK\r\n")},
		{name: "simple error", payload: []byte("-ERR unknown error\r\n")},
		{name: "integer", payload: []byte(":12345\r\n")},
		{name: "bulk string", payload: []byte("$5\r\nhello\r\n")},
		{name: "array", payload: []byte("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n")},
		{name: "null", payload: []byte("_\r\n")},
		{name: "boolean", payload: []byte("#t\r\n")},
		{name: "double", payload: []byte(",1.5\r\n")},
		{name: "big number", payload: []byte("(3492890328409238509324850943850943825024385\r\n")},
		{name: "bulk error", payload: []byte("!11\r\nERR failure\r\n")},
		{name: "verbatim string", payload: []byte("=8\r\ntxt:text\r\n")},
		{name: "map", payload: []byte("%1\r\n+key\r\n:1\r\n")},
		{name: "attribute", payload: []byte("|1\r\n+ttl\r\n:10\r\n")},
		{name: "set", payload: []byte("~2\r\n:1\r\n:2\r\n")},
		{name: "push", payload: []byte(">2\r\n+message\r\n$4\r\ndata\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewReader(bytes.NewBuffer(tt.payload)).Read()
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			assertMarshaledValue(t, got, string(tt.payload))
		})
	}
}

func BenchmarkReader_Read(b *testing.B) {
	wire := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	source := bytes.NewReader(wire)
	reader := NewReader(source)
	var (
		value Value
		err   error
	)

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		source.Reset(wire)
		reader.rd.Reset(source)
		value, err = reader.Read()
	}

	if err != nil {
		b.Fatalf("Read() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
	benchmarkReaderResult = value
}
