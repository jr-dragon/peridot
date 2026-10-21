package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestCommandSkipsEmptyRequests(t *testing.T) {
	for _, prefix := range []string{"\r\n", "\n", " \t\v\f\r\n", "*0\r\n", "*-1\r\n", "*-10\r\n", "*-9223372036854775808\r\n", strings.Repeat("\n*0\r\n", 10000)} {
		c := NewCommand(strings.NewReader(prefix + "PING\n"))
		args, err := c.Read()
		if err != nil {
			t.Fatalf("empty prefix of length %d: %v", len(prefix), err)
		}
		assertCommandArguments(t, args, []string{"PING"})
		if _, err := c.Read(); err != io.EOF {
			t.Fatalf("after PING: %v", err)
		}
		if _, err := NewCommand(strings.NewReader(prefix)).Read(); err != io.EOF {
			t.Fatalf("after empty requests: %v", err)
		}
	}
}

func TestCommandInlineRedisWhitespace(t *testing.T) {
	for _, tc := range []struct {
		wire string
		want []string
	}{
		{"PING\n", []string{"PING"}},
		{"PING\rhello\r\n", []string{"PING", "hello"}},
		{"PING \"hello\rworld\"\n", []string{"PING", "hello\rworld"}},
		{"\v\fPING \v\fhello\n", []string{"PING", "hello"}},
		{"PING hello\vworld\n", []string{"PING", "hello\vworld"}},
		{"PING hello\fworld\n", []string{"PING", "hello\fworld"}},
		{"PING \"hello\"\v\f\n", []string{"PING", "hello"}},
	} {
		args, err := NewCommand(strings.NewReader(tc.wire)).Read()
		if err != nil {
			t.Fatalf("%q: %v", tc.wire, err)
		}
		assertCommandArguments(t, args, tc.want)
	}
	// Inline permissiveness must not weaken the reply decoder's framing.
	for _, wire := range []string{"+OK\n", "+bad\rtext\r\n", "$1\nx\r\n", "*0\n"} {
		if _, err := NewReader(strings.NewReader(wire)).Read(); !errors.Is(err, ErrProtocol) {
			t.Errorf("reply %q: %v", wire, err)
		}
	}
}

func TestCommandLengthValidationAndReasons(t *testing.T) {
	for _, length := range []string{"01", "00", "+1", "-0", "-01", "-", "oops", "9223372036854775808", "-9223372036854775809", "2147483648"} {
		_, err := NewCommand(strings.NewReader("*" + length + "\r\n")).Read()
		assertCommandProtocolReason(t, err, "invalid multibulk length")
	}
	for _, length := range []string{"04", "00", "+4", "-0", "-1", "-2", "-oops", "536870913", "9223372036854775808"} {
		_, err := NewCommand(strings.NewReader("*1\r\n$" + length + "\r\n")).Read()
		assertCommandProtocolReason(t, err, "invalid bulk length")
	}
	// The upper bound itself remains a valid count and waits for arguments.
	end := errors.New("need more input")
	_, err := NewCommand(io.MultiReader(strings.NewReader("*2147483647\r\n"), failingReader{err: end})).Read()
	if !errors.Is(err, end) {
		t.Fatalf("maximum count: %v", err)
	}
	for _, tc := range []struct{ wire, reason string }{
		{"*1\r\n+PING\r\n", "expected '$', got '+'"},
		{"PING \"open\r\n", "unbalanced quotes in request"},
		{"PING 'open\r\n", "unbalanced quotes in request"},
		{"PING \"closed\"suffix\r\n", "unbalanced quotes in request"},
	} {
		_, err := NewCommand(strings.NewReader(tc.wire)).Read()
		assertCommandProtocolReason(t, err, tc.reason)
	}
	for _, magic := range []string{"$", "*", "!", "=", "%", "|", "~", ">"} {
		if _, err := NewReader(strings.NewReader(magic + "00\r\n")).Read(); !errors.Is(err, ErrProtocol) {
			t.Errorf("reply %s00: %v", magic, err)
		}
	}
}

func assertCommandProtocolReason(t *testing.T, err error, reason string) {
	t.Helper()
	var protocolErr *ProtocolError
	if !errors.Is(err, ErrProtocol) || !errors.As(err, &protocolErr) || protocolErr.Reason != reason {
		t.Fatalf("error = %v, want protocol reason %q", err, reason)
	}
}

func TestCommandReleasesPreviousArgumentsBeforeReading(t *testing.T) {
	payload := strings.Repeat("x", 1<<20)
	first := "*2\r\n$4\r\nPING\r\n$" + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n"
	for _, next := range []string{"PING\n", "*1\r\n$4\r\nPING\r\n", "\n*0\r\nPING\n", "", "*invalid\r\n"} {
		var c *Command
		source := io.MultiReader(strings.NewReader(first), inspectReader{read: func(p []byte) (int, error) {
			for _, arg := range c.argsCache {
				if arg.String() != "" {
					t.Fatal("previous arguments retained while waiting for input")
				}
			}
			return 0, io.EOF
		}}, strings.NewReader(next))
		c = NewCommand(source)
		args, err := c.Read()
		if err != nil {
			t.Fatal(err)
		}
		retained := args[1]
		c.Read()
		if c.argsCache[1].String() != "" {
			t.Fatal("unused slot retains previous payload")
		}
		if retained.String() != payload {
			t.Fatal("retained value changed after cache reuse")
		}
	}
}

type inspectReader struct{ read func([]byte) (int, error) }

func (r inspectReader) Read(p []byte) (int, error) { return r.read(p) }

func TestAggregateBuildersPreserveFollowingFrames(t *testing.T) {
	for _, tc := range []struct {
		name, header, body string
		build              func([]byte, io.Reader) (Value, error)
	}{
		{"array", "*2\r\n", "+ONE\r\n$3\r\ntwo\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildArray(h, r) }},
		{"nested array", "*1\r\n", "*1\r\n$3\r\none\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildArray(h, r) }},
		{"map", "%1\r\n", "+key\r\n*1\r\n:1\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildMap(h, r) }},
		{"attribute", "|1\r\n", "+key\r\n:1\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildAttribute(h, r) }},
		{"set", "~1\r\n", "+ONE\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildSet(h, r) }},
		{"push", ">1\r\n", "+ONE\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildPush(h, r) }},
		{"attributed element", "*1\r\n", "|0\r\n+ONE\r\n", func(h []byte, r io.Reader) (Value, error) { return BuildArray(h, r) }},
	} {
		for _, buffered := range []bool{false, true} {
			t.Run(tc.name+"/buffered="+strconv.FormatBool(buffered), func(t *testing.T) {
				var source io.Reader = strings.NewReader(tc.body + "+NEXT\r\n")
				if buffered {
					source = bufio.NewReader(source)
				}
				value, err := tc.build([]byte(tc.header), source)
				if err != nil {
					t.Fatal(err)
				}
				assertMarshaledValue(t, value, tc.header+tc.body)
				rest, err := io.ReadAll(source)
				if err != nil || string(rest) != "+NEXT\r\n" {
					t.Fatalf("remaining = %q, %v", rest, err)
				}
			})
		}
	}
}
