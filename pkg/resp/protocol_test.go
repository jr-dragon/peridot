package resp

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestReaderRejectsInvalidFrames(t *testing.T) {
	for _, wire := range []string{
		"?unknown\r\n", "PING\r\n", "+bad\rtext\r\n", "-ERR bad\rtext\r\n",
		"_extra\r\n", "#x\r\n", "#T\r\n", ":9223372036854775808\r\n",
		":-9223372036854775809\r\n", "$+1\r\na\r\n", "*+0\r\n",
		"!+0\r\n\r\n", "=+4\r\ntxt:\r\n", "%+0\r\n", "~+0\r\n", "|+0\r\n", ">+0\r\n",
		",0x1p2\r\n", ",1_000\r\n", ",Infinity\r\n", ",NaN\r\n", ",.5\r\n", ",1.\r\n",
		",1e\r\n", ",1e9999\r\n", "(1.5\r\n", "(1_000\r\n",
		"*1\r\n%-1\r\n", "*1\r\n~-1\r\n", "*1\r\n!-1\r\n",
		"$536870913\r\n", "$9223372036854775807\r\n",
	} {
		t.Run(strconv.Quote(wire), func(t *testing.T) {
			if _, err := NewReader(strings.NewReader(wire)).Read(); !errors.Is(err, ErrProtocol) {
				t.Fatalf("Read() error = %v, want protocol error", err)
			}
		})
	}
}

func TestReaderEOF(t *testing.T) {
	for _, wire := range []string{"+OK", "$3\r\nab", "*1\r\n", "%1\r\n+key\r\n", "*1\r\n|0\r\n"} {
		t.Run(strconv.Quote(wire), func(t *testing.T) {
			_, err := NewReader(strings.NewReader(wire)).Read()
			if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, ErrProtocol) || errors.Is(err, io.EOF) {
				t.Fatalf("Read() error = %v, want protocol error wrapping unexpected EOF only", err)
			}
		})
	}
	if _, err := NewReader(strings.NewReader("")).Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("empty input error = %v, want EOF", err)
	}
}

func TestNullLengthsAreOnlyAllowedInReplies(t *testing.T) {
	for _, magic := range []string{"$", "*"} {
		value, err := NewReader(strings.NewReader(magic + "-1\r\n")).Read()
		if err != nil {
			t.Fatal(err)
		}
		assertMarshaledValue(t, value, magic+"-1\r\n")
		for _, length := range []string{"-2", "-oops", "-", "-01", "-0", "-999999999999999999999"} {
			if _, err := NewReader(strings.NewReader(magic + length + "\r\n")).Read(); !errors.Is(err, ErrProtocol) {
				t.Errorf("reply %s%s: error = %v, want protocol error", magic, length, err)
			}
		}
	}
	for _, length := range []string{"-1", "-2", "-oops", "-", "-0", "-999999999999999999999"} {
		if _, err := NewCommand(strings.NewReader("*1\r\n$" + length + "\r\n")).Read(); !errors.Is(err, ErrProtocol) {
			t.Errorf("command bulk length %s: error = %v, want protocol error", length, err)
		}
	}
}

func TestAttributesDoNotCountAsAggregateElements(t *testing.T) {
	for _, wire := range []string{
		"*2\r\n|1\r\n+ttl\r\n:10\r\n:1\r\n:2\r\n",
		"%1\r\n|0\r\n+key\r\n|0\r\n|1\r\n+ttl\r\n:10\r\n:2\r\n",
		"~1\r\n|0\r\n:1\r\n",
		"*1\r\n*1\r\n|0\r\n:1\r\n",
	} {
		t.Run(strconv.Quote(wire), func(t *testing.T) {
			rd := NewReader(strings.NewReader(wire + "+NEXT\r\n"))
			value, err := rd.Read()
			if err != nil {
				t.Fatal(err)
			}
			assertMarshaledValue(t, value, wire)
			next, err := rd.Read()
			if err != nil {
				t.Fatal(err)
			}
			assertMarshaledValue(t, next, "+NEXT\r\n")
		})
	}
}

func TestBuildFunctionsRejectInvalidFraming(t *testing.T) {
	builders := []struct {
		magic byte
		build func([]byte) error
	}{
		{'+', func(h []byte) error { _, err := BuildSimpleString(h); return err }},
		{'-', func(h []byte) error { _, err := BuildSimpleError(h); return err }},
		{':', func(h []byte) error { _, err := BuildInteger(h); return err }},
		{'$', func(h []byte) error { _, err := BuildBulkString(h, nil); return err }},
		{'*', func(h []byte) error { _, err := BuildArray(h, nil); return err }},
		{'#', func(h []byte) error { _, err := BuildBoolean(h); return err }},
		{',', func(h []byte) error { _, err := BuildDouble(h); return err }},
		{'(', func(h []byte) error { _, err := BuildBigNumber(h); return err }},
		{'!', func(h []byte) error { _, err := BuildBulkError(h, nil); return err }},
		{'=', func(h []byte) error { _, err := BuildVerbatimString(h, nil); return err }},
		{'%', func(h []byte) error { _, err := BuildMap(h, nil); return err }},
		{'|', func(h []byte) error { _, err := BuildAttribute(h, nil); return err }},
		{'~', func(h []byte) error { _, err := BuildSet(h, nil); return err }},
		{'>', func(h []byte) error { _, err := BuildPush(h, nil); return err }},
	}
	for _, builder := range builders {
		for _, header := range [][]byte{nil, {builder.magic}, []byte(string(builder.magic) + "0xx"), []byte("?0\r\n")} {
			if err := builder.build(header); !errors.Is(err, ErrProtocol) {
				t.Errorf("builder %c with header %q: error = %v, want protocol error", builder.magic, header, err)
			}
		}
	}
}

func TestSimpleValuesCannotInjectFrames(t *testing.T) {
	assertMarshaledValue(t, SimpleString("hello\r\n+injected"), "+hello  +injected\r\n")
	assertMarshaledValue(t, NewSimpleError(errors.New("bad\r\n+injected")), "-ERR bad  +injected\r\n")
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadersPreserveIOErrors(t *testing.T) {
	want := errors.New("read failure")
	for _, prefix := range []string{"", "$1\r\n", "*1\r\n$1\r\n"} {
		for _, command := range []bool{false, true} {
			if command && prefix == "$1\r\n" {
				continue // A non-array header is an inline command.
			}
			rd := io.MultiReader(strings.NewReader(prefix), failingReader{err: want})
			var err error
			if command {
				_, err = NewCommand(rd).Read()
			} else {
				_, err = NewReader(rd).Read()
			}
			if !errors.Is(err, want) || errors.Is(err, io.EOF) || errors.Is(err, ErrProtocol) {
				t.Errorf("prefix %q, command %t: error = %v, want original I/O error", prefix, command, err)
			}
		}
	}
}

func TestCommandAcceptsLargeArgumentsAndCounts(t *testing.T) {
	payload := strings.Repeat("x", (512<<10)+1)
	args, err := NewCommand(strings.NewReader("*1\r\n$" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n")).Read()
	if err != nil {
		t.Fatal(err)
	}
	assertCommandArguments(t, args, []string{payload})
	args, err = NewCommand(strings.NewReader("*1025\r\n" + strings.Repeat("$1\r\nx\r\n", 1025))).Read()
	if err != nil || len(args) != 1025 {
		t.Fatalf("1025 arguments: count = %d, error = %v", len(args), err)
	}
}

func TestCommandInlineQuotesAndWhitespace(t *testing.T) {
	args, err := NewCommand(strings.NewReader("  SET\tkey  \"hello world\\n\\x21\" '' 'it\\'s'\r\n")).Read()
	if err != nil {
		t.Fatal(err)
	}
	assertCommandArguments(t, args, []string{"SET", "key", "hello world\n!", "", "it's"})
	for _, wire := range []string{"SET \"open\r\n", "SET 'open\r\n", "SET \"closed\"suffix\r\n"} {
		if _, err := NewCommand(strings.NewReader(wire)).Read(); !errors.Is(err, ErrProtocol) {
			t.Errorf("wire %q: error = %v, want protocol error", wire, err)
		}
	}
}
