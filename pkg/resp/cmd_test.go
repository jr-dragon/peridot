package resp

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

var benchmarkCommandResult []BulkString

func TestCommand_Read(t *testing.T) {
	largeArgument := strings.Repeat("a", len(Command{}.byteCache))
	tests := []struct {
		name     string
		wire     string
		want     []string
		wantNull []bool
	}{
		{
			name: "inline command",
			wire: "PING\r\n",
			want: []string{"PING"},
		},
		{
			name: "inline command with arguments",
			wire: "SET key value\r\n",
			want: []string{"SET", "key", "value"},
		},
		{
			name: "array command",
			wire: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
			want: []string{"SET", "key", "value"},
		},
		{
			name:     "empty arguments",
			wire:     "*3\r\n$3\r\nSET\r\n$0\r\n\r\n$0\r\n\r\n",
			want:     []string{"SET", "", ""},
			wantNull: []bool{false, false, false},
		},
		{
			name: "argument count at cache limit",
			wire: "*8\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n",
			want: []string{"0", "1", "2", "3", "4", "5", "6", "7"},
		},
		{
			name: "argument count above cache limit",
			wire: "*9\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n$1\r\n8\r\n",
			want: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"},
		},
		{
			name: "argument larger than byte cache",
			wire: "*1\r\n$" + strconv.Itoa(len(largeArgument)) + "\r\n" + largeArgument + "\r\n",
			want: []string{largeArgument},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCommand(strings.NewReader(tt.wire)).Read()
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}

			assertCommandArguments(t, got, tt.want)
			for i, wantNull := range tt.wantNull {
				if got[i].null != wantNull {
					t.Errorf("Read() argument %d null = %t, want %t", i, got[i].null, wantNull)
				}
			}
		})
	}
}

func TestCommand_ReadError(t *testing.T) {
	tests := []struct {
		name    string
		wire    string
		wantErr error
	}{
		{name: "empty input", wantErr: io.EOF},
		{name: "unterminated inline", wire: "PING", wantErr: errUnexpectedEOF},
		{name: "invalid array length", wire: "*invalid\r\n", wantErr: errInvalidHeader},
		{name: "negative array length", wire: "*-1\r\n", wantErr: io.EOF},
		{name: "overflow array length", wire: "*9223372036854775808\r\n", wantErr: errInvalidHeader},
		{name: "empty array", wire: "*0\r\n", wantErr: io.EOF},
		{name: "missing argument", wire: "*1\r\n", wantErr: errUnexpectedEOF},
		{name: "non-bulk-string argument", wire: "*1\r\n+PING\r\n", wantErr: errInvalidHeader},
		{name: "short bulk string header", wire: "*1\r\n$\r\n", wantErr: errInvalidHeader},
		{name: "invalid bulk string length", wire: "*1\r\n$invalid\r\n", wantErr: errInvalidHeader},
		{name: "overflow bulk string length", wire: "*1\r\n$9223372036854775807\r\n", wantErr: errInvalidHeader},
		{name: "truncated bulk string", wire: "*1\r\n$4\r\nPIN", wantErr: errUnexpectedEOF},
		{name: "invalid bulk string sentinel", wire: "*1\r\n$4\r\nPINGxx", wantErr: errUnexpectedSentinel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCommand(strings.NewReader(tt.wire)).Read()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Read() error = %v, want error matching %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommand_ReadsExactlyOneCommand(t *testing.T) {
	command := NewCommand(strings.NewReader("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"))

	first, err := command.Read()
	if err != nil {
		t.Fatalf("first Read() error = %v", err)
	}
	assertCommandArguments(t, first, []string{"PING"})

	second, err := command.Read()
	if err != nil {
		t.Fatalf("second Read() error = %v", err)
	}
	assertCommandArguments(t, second, []string{"ECHO", "hello"})
}

func BenchmarkCommand_Read(b *testing.B) {
	wire := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	reader := bytes.NewReader(wire)
	command := NewCommand(reader)
	var (
		args []BulkString
		err  error
	)

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		reader.Reset(wire)
		command.rd.rd.Reset(reader)
		args, err = command.Read()
	}

	if err != nil {
		b.Fatalf("Read() error = %v", err)
	}
	assertCommandArguments(b, args, []string{"SET", "key", "value"})
	benchmarkCommandResult = args
}

func assertCommandArguments(t testing.TB, got []BulkString, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("Read() returned %d arguments, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("Read() argument %d = %q, want %q", i, got[i].String(), want[i])
		}
	}
}
