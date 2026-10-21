package resp

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

var benchmarkMarshalResult []byte

func TestSimpleStringMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value SimpleString
		want  string
	}{
		{
			name:  "empty",
			value: SimpleString(""),
			want:  "+\r\n",
		},
		{
			name:  "text",
			value: SimpleString("PONG"),
			want:  "+PONG\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestSimpleStringMarshalToNilBuffer(t *testing.T) {
	SimpleString("PONG").marshalTo(nil)
}

func TestBuildSimpleString(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		expect string
	}{
		{
			name: "empty",
			data: []byte("+\r\n"),
		},
		{
			name: "text",
			data: []byte("+PONG\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildSimpleString(tt.data)
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			assertMarshaledValue(t, got, string(tt.data))
		})
	}
}

func TestBuildSimpleStringCopiesSource(t *testing.T) {
	source := []byte("+PONG\r\n")
	value, err := BuildSimpleString(source)
	if err != nil {
		t.Fatalf("BuildSimpleString() error = %v", err)
	}

	copy(source, "+FAIL\r\n")

	assertMarshaledValue(t, value, "+PONG\r\n")
}

func TestSimpleErrorMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value SimpleError
		want  string
	}{
		{
			name:  "nil error",
			value: NewSimpleError(nil),
			want:  "-ERR\r\n",
		},
		{
			name:  "error message",
			value: NewSimpleError(errors.New("unknown command")),
			want:  "-ERR unknown command\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestSimpleErrorWithCodeMarshal(t *testing.T) {
	for _, tc := range []struct {
		code string
		err  error
		want string
	}{
		{code: "WRONGTYPE", err: errors.New("wrong kind of value"), want: "-WRONGTYPE wrong kind of value\r\n"},
		{code: "NOAUTH", want: "-NOAUTH\r\n"},
		{code: "ERR", err: errors.New("bad\r\n+injected"), want: "-ERR bad  +injected\r\n"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			value := NewSimpleErrorWithCode(tc.code, tc.err)
			assertMarshaledValue(t, value, tc.want)
			assertMarshaledValue(t, NewArray([]Value{value}), "*1\r\n"+tc.want)
		})
	}
}

func TestBuildSimpleError(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "nil err",
			data: []byte("-ERR\r\n"),
		},
		{
			name: "error message",
			data: []byte("-ERR unknown error\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildSimpleError(tt.data)
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			assertMarshaledValue(t, got, string(tt.data))
		})
	}
}

func TestIntegerMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Integer
		want  string
	}{
		{
			name:  "zero",
			value: 0,
			want:  ":0\r\n",
		},
		{
			name:  "negative",
			value: -42,
			want:  ":-42\r\n",
		},
		{
			name:  "maximum",
			value: Integer(math.MaxInt64),
			want:  ":9223372036854775807\r\n",
		},
		{
			name:  "minimum",
			value: Integer(math.MinInt64),
			want:  ":-9223372036854775808\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildInteger(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "zero",
			data: []byte(":0\r\n"),
		},
		{
			name: "negative",
			data: []byte(":-42\r\n"),
		},
		{
			name: "maximum",
			data: []byte(":9223372036854775807\r\n"),
		},
		{
			name: "minimum",
			data: []byte(":-9223372036854775808\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildInteger(tt.data)
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			assertMarshaledValue(t, got, string(tt.data))
		})
	}
}

func TestBuildIntegerError(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "not a number", data: []byte(":invalid\r\n")},
		{name: "overflow", data: []byte(":9223372036854775808\r\n")},
		{name: "underflow", data: []byte(":-9223372036854775809\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildInteger(tt.data); !errors.Is(err, errInvalidHeader) {
				t.Errorf("BuildInteger() error = %v, want invalid header error", err)
			}
		})
	}
}

func TestBulkStringMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value BulkString
		want  string
	}{
		{
			name:  "null",
			value: NullBulkStringValue,
			want:  "$-1\r\n",
		},
		{
			name:  "empty",
			value: NewBulkString(""),
			want:  "$0\r\n\r\n",
		},
		{
			name:  "text",
			value: NewBulkString("hello"),
			want:  "$5\r\nhello\r\n",
		},
		{
			name:  "length is measured in bytes",
			value: NewBulkString("你好"),
			want:  "$6\r\n你好\r\n",
		},
		{
			name:  "embedded sentinel",
			value: NewBulkString("a\r\nb"),
			want:  "$4\r\na\r\nb\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildBulkString(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		expect string
	}{
		{
			name:   "null",
			header: []byte("$-1\r\n"),
			reader: nil,
			expect: "$-1\r\n",
		},
		{
			name:   "empty",
			header: []byte("$0\r\n"),
			reader: bytes.NewBufferString("\r\n"),
			expect: "$0\r\n\r\n",
		},
		{
			name:   "text",
			header: []byte("$5\r\n"),
			reader: bytes.NewBufferString("hello\r\n"),
			expect: "$5\r\nhello\r\n",
		},
		{
			name:   "length is measured in bytes",
			header: []byte("$6\r\n"),
			reader: bytes.NewBufferString("你好\r\n"),
			expect: "$6\r\n你好\r\n",
		},
		{
			name:   "embedded sentinel",
			header: []byte("$4\r\n"),
			reader: bytes.NewBufferString("a\r\nb\r\n"),
			expect: "$4\r\na\r\nb\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildBulkString(tt.header, tt.reader)
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			assertMarshaledValue(t, got, tt.expect)
		})
	}
}

func TestBuildBulkStringError(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
	}{
		{name: "short header", header: []byte("$")},
		{name: "invalid length", header: []byte("$invalid\r\n")},
		{name: "missing payload", header: []byte("$1\r\n"), reader: bytes.NewReader(nil)},
		{name: "truncated payload", header: []byte("$3\r\n"), reader: bytes.NewBufferString("ab")},
		{name: "missing sentinel", header: []byte("$2\r\n"), reader: bytes.NewBufferString("ab")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildBulkString(tt.header, tt.reader); err == nil {
				t.Fatal("BuildBulkString() error = nil, want non-nil")
			}
		})
	}
}

func TestBulkStringString(t *testing.T) {
	const want = "hello"

	if got := NewBulkString(want).String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestArrayMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Array
		want  string
	}{
		{
			name:  "null",
			value: NullArrayValue,
			want:  "*-1\r\n",
		},
		{
			name:  "empty",
			value: NewArray(nil),
			want:  "*0\r\n",
		},
		{
			name: "mixed values",
			value: NewArray([]Value{
				SimpleString("OK"),
				Integer(-2),
				NewBulkString("hey"),
				NewSimpleError(errors.New("failure")),
			}),
			want: "*4\r\n+OK\r\n:-2\r\n$3\r\nhey\r\n-ERR failure\r\n",
		},
		{
			name:  "static OK value",
			value: NewArray([]Value{OKValue}),
			want:  "*1\r\n+OK\r\n",
		},
		{
			name:  "nil element is null",
			value: NewArray([]Value{nil}),
			want:  "*1\r\n_\r\n",
		},
		{
			name:  "null bulk string",
			value: NewArray([]Value{NullBulkStringValue}),
			want:  "*1\r\n$-1\r\n",
		},
		{
			name: "nested array",
			value: NewArray([]Value{
				NewArray([]Value{NewBulkString("PING")}),
			}),
			want: "*1\r\n*1\r\n$4\r\nPING\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildArray(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{
			name:   "null",
			header: []byte("*-1\r\n"),
			want:   "*-1\r\n",
		},
		{
			name:   "empty",
			header: []byte("*0\r\n"),
			want:   "*0\r\n",
		},
		{
			name:   "mixed values",
			header: []byte("*4\r\n"),
			reader: bytes.NewBufferString("+OK\r\n:-2\r\n$3\r\nhey\r\n-ERR failure\r\n"),
			want:   "*4\r\n+OK\r\n:-2\r\n$3\r\nhey\r\n-ERR failure\r\n",
		},
		{
			name:   "nested array",
			header: []byte("*1\r\n"),
			reader: bytes.NewBufferString("*1\r\n$4\r\nPING\r\n"),
			want:   "*1\r\n*1\r\n$4\r\nPING\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildArray(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildArray() error = %v", err)
			}

			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestBuildArrayError(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
	}{
		{
			name:   "invalid length",
			header: []byte("*invalid\r\n"),
		},
		{
			name:   "missing value",
			header: []byte("*1\r\n"),
		},
		{
			name:   "truncated values",
			header: []byte("*2\r\n"),
			reader: bytes.NewBufferString("+OK\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildArray(tt.header, tt.reader); err == nil {
				t.Fatal("BuildArray() error = nil, want non-nil")
			}
		})
	}
}

func TestBuildInlineArray(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{
			name:    "without separator",
			payload: []byte("PING\r\n"),
			want:    "*1\r\n$4\r\nPING\r\n",
		},
		{
			name:    "with separator",
			payload: []byte("SET foo bar\r\n"),
			want:    "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, BuildInlineArray(tt.payload), tt.want)
		})
	}
}

func TestBuildInlineBulkString(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    []string
	}{
		{name: "single argument", payload: []byte("PING\r\n"), want: []string{"PING"}},
		{name: "multiple arguments", payload: []byte("SET foo bar\r\n"), want: []string{"SET", "foo", "bar"}},
		{name: "consecutive spaces", payload: []byte("SET  value\r\n"), want: []string{"SET", "value"}},
		{name: "byte preserving", payload: []byte("SET key 你好\r\n"), want: []string{"SET", "key", "你好"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildInlineBulkString(tt.payload)
			if len(got) != len(tt.want) {
				t.Fatalf("BuildInlineBulkString() length = %d, want %d", len(got), len(tt.want))
			}
			for i, want := range tt.want {
				if got[i].String() != want {
					t.Errorf("BuildInlineBulkString()[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

func TestReaderReadsValueAfterArray(t *testing.T) {
	rd := NewReader(bytes.NewBufferString("*1\r\n$4\r\nPING\r\n+PONG\r\n"))

	array, err := rd.Read()
	if err != nil {
		t.Fatalf("Read() array error = %v", err)
	}
	assertMarshaledValue(t, array, "*1\r\n$4\r\nPING\r\n")

	value, err := rd.Read()
	if err != nil {
		t.Fatalf("Read() following value error = %v", err)
	}
	assertMarshaledValue(t, value, "+PONG\r\n")
}

func assertMarshaledValue(t testing.TB, value Value, want string) {
	t.Helper()

	if got := string(value.Marshal()); got != want {
		t.Fatalf("Marshal() = %q, want %q", got, want)
	}
}

func BenchmarkSimpleStringMarshal(b *testing.B) {
	value := SimpleString("PONG")

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkSimpleErrorMarshal(b *testing.B) {
	value := NewSimpleError(errors.New("unknown command"))

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkIntegerMarshal(b *testing.B) {
	value := Integer(math.MaxInt64)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkBulkStringMarshal(b *testing.B) {
	value := NewBulkString("hello")

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkArrayMarshal(b *testing.B) {
	values := make([]Value, 100)
	for i := range values {
		values[i] = NewBulkString("hello")
	}
	value := NewArray(values)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkBuildSimpleString(b *testing.B) {
	wire := []byte("+PONG\r\n")
	var value SimpleString
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value, err = BuildSimpleString(wire)
	}

	if err != nil {
		b.Fatalf("BuildSimpleString() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
}

func BenchmarkBuildSimpleError(b *testing.B) {
	wire := []byte("-ERR unknown command\r\n")
	var value SimpleError
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value, err = BuildSimpleError(wire)
	}

	if err != nil {
		b.Fatalf("BuildSimpleError() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
}

func BenchmarkBuildInteger(b *testing.B) {
	wire := []byte(":9223372036854775807\r\n")
	var value Integer
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value, err = BuildInteger(wire)
	}

	if err != nil {
		b.Fatalf("BuildInteger() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
}

func BenchmarkBuildBulkString(b *testing.B) {
	header := []byte("$5\r\n")
	payload := []byte("hello\r\n")
	reader := bytes.NewReader(payload)
	var value BulkString
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildBulkString(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildBulkString() error = %v", err)
	}
	assertMarshaledValue(b, value, "$5\r\nhello\r\n")
}

func BenchmarkBuildArray(b *testing.B) {
	header := []byte("*100\r\n")
	payload := []byte(strings.Repeat("$5\r\nhello\r\n", 100))
	reader := bytes.NewReader(payload)
	var value Array
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildArray(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildArray() error = %v", err)
	}
	if len(value.data) != 100 {
		b.Fatalf("BuildArray() length = %d, want 100", len(value.data))
	}
}

func BenchmarkBuildInlineArray(b *testing.B) {
	wire := []byte("SET key value\r\n")
	var value Array

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value = BuildInlineArray(wire)
	}

	if len(value.data) != 3 {
		b.Fatalf("BuildInlineArray() length = %d, want 3", len(value.data))
	}
}

func BenchmarkBuildInlineBulkString(b *testing.B) {
	wire := []byte("SET key value\r\n")
	var value []BulkString

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value = BuildInlineBulkString(wire)
	}

	if len(value) != 3 {
		b.Fatalf("BuildInlineBulkString() length = %d, want 3", len(value))
	}
}
