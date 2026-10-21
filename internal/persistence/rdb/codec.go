// Package rdb reads and writes the Redis RDB string subset (database zero).
package rdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/jr-dragon/peridot/internal/storage"
)

var ErrFormat = errors.New("rdb: invalid or unsupported snapshot")

// String retains an explicit expiration, including epoch zero, during AOF replay.
type String struct {
	Value         string
	ExpiresAt     int64
	HasExpiration bool
}

// Redis uses CRC-64/Jones, reflected, with an initial and final XOR of zero.
var crcTable = func() [256]uint64 {
	var table [256]uint64
	for i := range table {
		c := uint64(i)
		for range 8 {
			if c&1 != 0 {
				c = c>>1 ^ 0x95ac9329ac4bc9b5
			} else {
				c >>= 1
			}
		}
		table[i] = c
	}
	return table
}()

func checksum(crc uint64, p []byte) uint64 {
	for _, b := range p {
		crc = crcTable[byte(crc)^b] ^ crc>>8
	}
	return crc
}

type decoder struct {
	r   io.Reader
	crc uint64
}

func (d *decoder) read(n int) ([]byte, error) {
	p := make([]byte, n)
	_, err := io.ReadFull(d.r, p)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFormat, err)
	}
	d.crc = checksum(d.crc, p)
	return p, nil
}
func (d *decoder) length(encoded bool) (uint64, bool, error) {
	p, err := d.read(1)
	if err != nil {
		return 0, false, err
	}
	switch p[0] >> 6 {
	case 0:
		return uint64(p[0]), false, nil
	case 1:
		b, err := d.read(1)
		if err != nil {
			return 0, false, err
		}
		return uint64(p[0]&63)<<8 | uint64(b[0]), false, nil
	case 2:
		switch p[0] {
		case 0x80:
			b, err := d.read(4)
			if err != nil {
				return 0, false, err
			}
			return uint64(binary.BigEndian.Uint32(b)), false, nil
		case 0x81:
			b, err := d.read(8)
			if err != nil {
				return 0, false, err
			}
			return binary.BigEndian.Uint64(b), false, nil
		}
	case 3:
		if encoded {
			return uint64(p[0] & 63), true, nil
		}
	}
	return 0, false, ErrFormat
}
func (d *decoder) str() (string, error) {
	n, enc, err := d.length(true)
	if err != nil {
		return "", err
	}
	if !enc {
		if n > 512<<20 {
			return "", fmt.Errorf("%w: string exceeds 512 MiB", ErrFormat)
		}
		// Grow with the actual input rather than allocating an untrusted length.
		var b []byte
		for n > 0 {
			p, err := d.read(int(min(n, 64<<10)))
			if err != nil {
				return "", err
			}
			b = append(b, p...)
			n -= uint64(len(p))
		}
		return string(b), nil
	}
	switch n {
	case 0, 1, 2:
		b, err := d.read(1 << n)
		if err != nil {
			return "", err
		}
		var value int64
		switch n {
		case 0:
			value = int64(int8(b[0]))
		case 1:
			value = int64(int16(binary.LittleEndian.Uint16(b)))
		case 2:
			value = int64(int32(binary.LittleEndian.Uint32(b)))
		}
		return strconv.FormatInt(value, 10), nil
	case 3:
		compressed, _, err := d.length(false)
		if err != nil {
			return "", err
		}
		expanded, _, err := d.length(false)
		if err != nil {
			return "", err
		}
		if compressed > 512<<20 || expanded > 512<<20 {
			return "", ErrFormat
		}
		// Reject truncated data before allocating the expanded buffer.
		var b []byte
		for compressed > 0 {
			p, err := d.read(int(min(compressed, 64<<10)))
			if err != nil {
				return "", err
			}
			b = append(b, p...)
			compressed -= uint64(len(p))
		}
		return lzf(b, int(expanded))
	default:
		return "", fmt.Errorf("%w: string encoding %d", ErrFormat, n)
	}
}

// lzf decodes literal runs and overlapping backward references.
func lzf(input []byte, size int) (string, error) {
	out := make([]byte, 0, min(size, 64<<10))
	for i := 0; i < len(input); {
		ctrl := int(input[i])
		i++
		if ctrl < 32 {
			n := ctrl + 1
			if n > len(input)-i || n > size-len(out) {
				return "", ErrFormat
			}
			out = append(out, input[i:i+n]...)
			i += n
		} else {
			n := ctrl >> 5
			if n == 7 {
				if i == len(input) {
					return "", ErrFormat
				}
				n += int(input[i])
				i++
			}
			if i == len(input) {
				return "", ErrFormat
			}
			distance := (ctrl&31)<<8 + int(input[i]) + 1
			i++
			n += 2
			if distance > len(out) || n > size-len(out) {
				return "", ErrFormat
			}
			for range n {
				out = append(out, out[len(out)-distance])
			}
		}
	}
	if len(out) != size {
		return "", ErrFormat
	}
	return string(out), nil
}

// Load consumes exactly one RDB, leaving any trailing AOF commands unread.
// Expiration is evaluated only after all subsequent incremental logs are replayed.
func Load(r io.Reader) (map[string]String, error) {
	d := decoder{r: r}
	header, err := d.read(9)
	if err != nil {
		return nil, err
	}
	version, err := strconv.Atoi(string(header[5:]))
	if string(header[:5]) != "REDIS" || err != nil || version < 5 || version > 15 {
		return nil, fmt.Errorf("%w: header", ErrFormat)
	}
	state := make(map[string]String)
	var deadline int64
	var expires, metadata bool
	for {
		b, err := d.read(1)
		if err != nil {
			return nil, err
		}
		switch b[0] {
		case 255:
			if metadata {
				return nil, ErrFormat
			}
			expected := d.crc
			b := make([]byte, 8)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, fmt.Errorf("%w: checksum: %w", ErrFormat, err)
			}
			stored := binary.LittleEndian.Uint64(b)
			if stored != 0 && stored != expected {
				return nil, fmt.Errorf("%w: checksum mismatch", ErrFormat)
			}
			return state, nil
		case 254:
			if metadata {
				return nil, ErrFormat
			}
			db, _, err := d.length(false)
			if err != nil {
				return nil, err
			}
			if db != 0 {
				return nil, fmt.Errorf("%w: database %d", ErrFormat, db)
			}
		case 251:
			if metadata {
				return nil, ErrFormat
			}
			for range 2 {
				if _, _, err := d.length(false); err != nil {
					return nil, err
				}
			}
		case 250:
			if metadata {
				return nil, ErrFormat
			}
			for range 2 {
				if _, err := d.str(); err != nil {
					return nil, err
				}
			}
		case 252, 253:
			if expires {
				return nil, ErrFormat
			}
			size := 8
			if b[0] == 253 {
				size = 4
			}
			p, err := d.read(size)
			if err != nil {
				return nil, err
			}
			if size == 8 {
				deadline = int64(binary.LittleEndian.Uint64(p))
			} else {
				deadline = int64(binary.LittleEndian.Uint32(p)) * 1000
			}
			expires, metadata = true, true
		case 248:
			if _, _, err := d.length(false); err != nil {
				return nil, err
			}
			metadata = true
		case 249:
			if _, err := d.read(1); err != nil {
				return nil, err
			}
			metadata = true
		case 0:
			key, err := d.str()
			if err != nil {
				return nil, err
			}
			value, err := d.str()
			if err != nil {
				return nil, err
			}
			if _, ok := state[key]; ok {
				return nil, fmt.Errorf("%w: duplicate key", ErrFormat)
			}
			state[key] = String{value, deadline, expires}
			deadline, expires, metadata = 0, false, false
		default:
			return nil, fmt.Errorf("%w: object type or opcode %d", ErrFormat, b[0])
		}
	}
}

type encoder struct {
	w   io.Writer
	crc uint64
	err error
}

func (e *encoder) write(p []byte) {
	if e.err != nil {
		return
	}
	n, err := e.w.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	e.err = err
	e.crc = checksum(e.crc, p)
}
func (e *encoder) length(n uint64) {
	switch {
	case n < 64:
		e.write([]byte{byte(n)})
	case n < 16384:
		e.write([]byte{byte(n>>8) | 64, byte(n)})
	case n <= 1<<32-1:
		e.write(binary.BigEndian.AppendUint32([]byte{0x80}, uint32(n)))
	default:
		e.write(binary.BigEndian.AppendUint64([]byte{0x81}, n))
	}
}
func (e *encoder) str(s string) { e.length(uint64(len(s))); e.write([]byte(s)) }

// Write emits uncompressed strings, absolute millisecond deadlines and a checksum.
func Write(ctx context.Context, w io.Writer, state map[string]storage.Entry) error {
	e := encoder{w: w}
	e.write([]byte("REDIS0015"))
	e.write([]byte{254, 0})
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := state[key]
		value, ok := entry.Value.(string)
		if !ok {
			return fmt.Errorf("%w: unsupported value type", ErrFormat)
		}
		if len(key) > 512<<20 || len(value) > 512<<20 {
			return ErrFormat
		}
		if entry.ExpiresAt != 0 {
			e.write(binary.LittleEndian.AppendUint64([]byte{252}, uint64(entry.ExpiresAt)))
		}
		e.write([]byte{0})
		e.str(key)
		e.str(value)
		if e.err != nil {
			return e.err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.write([]byte{255})
	sum := e.crc
	e.write(binary.LittleEndian.AppendUint64(nil, sum))
	return e.err
}
