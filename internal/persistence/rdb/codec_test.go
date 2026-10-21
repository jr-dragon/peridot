package rdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/storage"
)

func TestCRCJones(t *testing.T) {
	if got := checksum(0, []byte("123456789")); got != 0xe9c6d914c4b8d9ca {
		t.Fatalf("checksum = %x", got)
	}
}
func TestRoundTripAndCorruption(t *testing.T) {
	entries := map[string]storage.Entry{"": {Value: ""}, "\x00\xff": {Value: "\r\n\x00\xff", ExpiresAt: 4102444800000}, "big": {Value: strings.Repeat("x", 65536)}}
	var b bytes.Buffer
	if err := Write(context.Background(), &b, entries); err != nil {
		t.Fatal(err)
	}
	raw := b.Bytes()
	got, err := Load(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for k, e := range entries {
		if got[k].Value != e.Value || got[k].ExpiresAt != e.ExpiresAt {
			t.Fatal(k, got[k])
		}
	}
	for _, cut := range []int{0, 1, 8, 9, 10, 20, len(raw) - 9, len(raw) - 8, len(raw) - 1} {
		if _, err := Load(bytes.NewReader(raw[:cut])); !errors.Is(err, ErrFormat) {
			t.Fatalf("cut %d: %v", cut, err)
		}
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(raw)-1] ^= 1
	if _, err := Load(bytes.NewReader(corrupt)); !errors.Is(err, ErrFormat) {
		t.Fatal("accepted bad checksum", err)
	}
	clear(corrupt[len(corrupt)-8:])
	if _, err := Load(bytes.NewReader(corrupt)); err != nil {
		t.Fatal("disabled checksum rejected", err)
	}
	// Load must leave an AOF tail unread even when supplied an unbuffered reader.
	reader := bytes.NewReader(append(bytes.Clone(raw), []byte("tail")...))
	if _, err := Load(reader); err != nil {
		t.Fatal(err)
	}
	tail, _ := io.ReadAll(reader)
	if string(tail) != "tail" {
		t.Fatal(string(tail))
	}
}
func fixture(body []byte) []byte {
	b := append([]byte("REDIS0015"), body...)
	b = append(b, 255)
	return binary.LittleEndian.AppendUint64(b, checksum(0, b))
}
func TestEncodedStringsAndHistoricalExpiration(t *testing.T) {
	body := []byte{254, 0, 252, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 'k', 0xc0, 0xf4}
	state, err := Load(bytes.NewReader(fixture(body)))
	if err != nil || !state["k"].HasExpiration || state["k"].ExpiresAt != 0 || state["k"].Value != "-12" {
		t.Fatal(state, err)
	}
	for _, tc := range []struct {
		encoded []byte
		want    string
	}{
		{[]byte{0xc1, 0xff, 0x7f}, "32767"}, {[]byte{0xc2, 0, 0, 0, 0x80}, "-2147483648"},
		{[]byte{0xc3, 5, 8, 1, 'a', 'b', 0x80, 1}, "abababab"},
	} {
		body := append([]byte{0, 1, 'k'}, tc.encoded...)
		state, err := Load(bytes.NewReader(fixture(body)))
		if err != nil || state["k"].Value != tc.want {
			t.Fatal(state, err)
		}
	}
}
func TestRejectUnsupportedAndMalformed(t *testing.T) {
	for name, body := range map[string][]byte{
		"other-db": {254, 1}, "list": {1}, "function": {245}, "key-metadata": {243},
		"dangling-expiration": {253, 1, 0, 0, 0}, "bad-length": {0, 0x82},
		"encoded-db": {254, 0xc0}, "unknown-string-encoding": {0, 0xc4},
		"oversized-string": {0, 0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		"duplicate":        {0, 1, 'k', 1, 'v', 0, 1, 'k', 1, 'v'},
		"lzf-underflow":    {0, 1, 'k', 0xc3, 2, 5, 0x60, 0},
		"lzf-truncated":    {0, 1, 'k', 0xc3, 1, 1, 0},
		"lzf-overflow":     {0, 1, 'k', 0xc3, 2, 0, 0, 'x'},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(bytes.NewReader(fixture(body))); !errors.Is(err, ErrFormat) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }
func TestWriteFailures(t *testing.T) {
	if err := Write(context.Background(), shortWriter{}, nil); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Write(ctx, io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := Write(context.Background(), io.Discard, map[string]storage.Entry{"k": {Value: 1}}); !errors.Is(err, ErrFormat) {
		t.Fatal(err)
	}
}
