// Package aof encodes and replays Redis-compatible append-only command streams.
package aof

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/jr-dragon/peridot/pkg/resp"
)

var ErrFormat = errors.New("aof: invalid or unsupported log")

// String is recovered string state. HasExpiration distinguishes epoch zero
// from a missing expiration while replaying historical commands.
type String struct {
	Value         string
	ExpiresAt     int64
	HasExpiration bool
}

// Encode returns one binary-safe RESP command, without retaining arguments.
func Encode(args ...string) []byte {
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.NewBulkString(arg)
	}
	return resp.NewArray(values).Marshal()
}

// Replacement encodes a resolved string replacement with an absolute deadline.
func Replacement(key, value string, expiresAt int64) []byte {
	if expiresAt != 0 {
		return Encode("SET", key, value, "PXAT", strconv.FormatInt(expiresAt, 10))
	}
	return Encode("SET", key, value)
}

// Load validates the complete log before returning private recovered state.
// Expiration is deliberately not evaluated against the recovery clock.
func Load(input io.Reader) (map[string]String, error) {
	state := make(map[string]String)
	if err := Replay(input, state); err != nil {
		return nil, err
	}
	return state, nil
}

// Replay applies one complete segment to private recovery state. The caller must
// discard state on error and defer expiration until every segment has replayed.
func Replay(input io.Reader, state map[string]String) error {
	r := bufio.NewReaderSize(input, 64<<10)
	inTransaction := false
	for record := 1; ; record++ {
		first, err := r.Peek(1)
		if err == io.EOF {
			if inTransaction {
				return fmt.Errorf("%w: incomplete transaction", ErrFormat)
			}
			return nil
		}
		if err != nil {
			return err
		}
		if first[0] == '#' {
			if _, err := line(r); err != nil {
				return fmt.Errorf("%w: incomplete annotation: %w", ErrFormat, err)
			}
			continue
		}
		args, err := readCommand(r)
		if err == nil {
			switch upperASCII(args[0]) {
			case "MULTI":
				if len(args) != 1 || inTransaction {
					err = ErrFormat
				} else {
					inTransaction = true
				}
			case "EXEC":
				if len(args) != 1 || !inTransaction {
					err = ErrFormat
				} else {
					inTransaction = false
				}
			default:
				err = apply(state, args)
			}
		}
		if err != nil {
			return fmt.Errorf("%w: record %d: %w", ErrFormat, record, err)
		}
	}
}

func line(r *bufio.Reader) ([]byte, error) {
	b, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(b) < 2 || !bytes.HasSuffix(b, []byte("\r\n")) {
		return nil, ErrFormat
	}
	return b[:len(b)-2], nil
}

func length(r *bufio.Reader, prefix byte, limit int64) (int64, error) {
	b, err := line(r)
	if err != nil {
		return 0, err
	}
	if len(b) < 2 || b[0] != prefix {
		return 0, ErrFormat
	}
	for _, digit := range b[1:] {
		if digit < '0' || digit > '9' {
			return 0, ErrFormat
		}
	}
	n, err := strconv.ParseInt(string(b[1:]), 10, 64)
	if err != nil || n < 0 || n > limit {
		return 0, ErrFormat
	}
	return n, nil
}

func readCommand(r *bufio.Reader) ([]string, error) {
	n, err := length(r, '*', 1<<31-1)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, ErrFormat
	}
	args := make([]string, 0, min(n, 16))
	for range n {
		size, err := length(r, '$', 512<<20)
		if err != nil {
			return nil, err
		}
		b := make([]byte, size+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		if !bytes.HasSuffix(b, []byte("\r\n")) {
			return nil, ErrFormat
		}
		args = append(args, string(b[:size]))
	}
	return args, nil
}

func apply(state map[string]String, args []string) error {
	bad := func() error { return fmt.Errorf("%w: unsupported command or arguments", ErrFormat) }
	switch upperASCII(args[0]) {
	case "SELECT":
		if len(args) != 2 || args[1] != "0" {
			return bad()
		}
	case "SET", "SETNX":
		if len(args) < 3 {
			return bad()
		}
		value := String{Value: args[2]}
		if upperASCII(args[0]) == "SETNX" && len(args) != 3 {
			return bad()
		}
		var condition, match, expiration, ttl string
		for i := 3; i < len(args); i++ {
			option := args[i]
			if nul := strings.IndexByte(option, 0); nul >= 0 {
				option = option[:nul]
			}
			option = upperASCII(option)
			switch option {
			case "GET":
			case "NX", "XX":
				// Redis may retain conditional flags in AOF. Only applied writes
				// are logged, so their resolved replacement is unconditional.
				if condition != "" && condition != option {
					return bad()
				}
				condition = option
			case "IFEQ", "IFNE", "IFDEQ", "IFDNE":
				if condition != "" && condition != option {
					return bad()
				}
				condition = option
				if i+1 == len(args) {
					return bad()
				}
				i++
				match = args[i]
			case "KEEPTTL":
				if expiration != "" && expiration != option {
					return bad()
				}
				expiration = option
				old := state[args[1]]
				value.ExpiresAt, value.HasExpiration = old.ExpiresAt, old.HasExpiration
			case "PXAT", "EXAT":
				if expiration != "" && expiration != option {
					return bad()
				}
				expiration = option
				if i+1 == len(args) {
					return bad()
				}
				i++
				ttl = args[i]
			default:
				return bad()
			}
		}
		// Redis validates only the final argument of a repeated option. PXAT
		// commands can retain even the overridden invalid arguments in the AOF.
		if expiration == "PXAT" || expiration == "EXAT" {
			n, err := integer(ttl)
			if err != nil || n <= 0 {
				return bad()
			}
			if expiration == "EXAT" {
				if n > math.MaxInt64/1000 {
					return bad()
				}
				n *= 1000
			}
			value.ExpiresAt, value.HasExpiration = n, true
		}
		// Digest length is checked only for an existing historical key. In
		// particular, IFDNE on an absent key succeeds with any digest length.
		if _, exists := state[args[1]]; exists && (condition == "IFDEQ" || condition == "IFDNE") && len(match) != 16 {
			return bad()
		}
		state[args[1]] = value
	case "DEL", "UNLINK":
		if len(args) < 2 {
			return bad()
		}
		for _, key := range args[1:] {
			delete(state, key)
		}
	case "PEXPIREAT":
		if len(args) < 3 {
			return bad()
		}
		var nx, xx, gt, lt bool
		for _, option := range args[3:] {
			option, _, _ = strings.Cut(option, "\x00")
			switch upperASCII(option) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			case "GT":
				gt = true
			case "LT":
				lt = true
			default:
				return bad()
			}
		}
		if nx && (xx || gt || lt) || gt && lt {
			return bad()
		}
		n, err := integer(args[2])
		if err != nil {
			return bad()
		}
		if value, ok := state[args[1]]; ok {
			// As with conditional SET, retained flags describe an already
			// applied write. Restore its absolute deadline without today's clock.
			value.ExpiresAt, value.HasExpiration = n, true
			state[args[1]] = value
		}
	case "PERSIST":
		if len(args) != 2 {
			return bad()
		}
		if value, ok := state[args[1]]; ok {
			value.ExpiresAt, value.HasExpiration = 0, false
			state[args[1]] = value
		}
	default:
		return bad()
	}
	return nil
}

func upperASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - ('a' - 'A')
		}
	}
	return string(b)
}

func integer(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, ErrFormat
	}
	return n, nil
}
