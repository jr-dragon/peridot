package resp

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
)

func TestNullMarshal(t *testing.T) {
	assertMarshaledValue(t, NullValue, "_\r\n")
}

func TestBooleanMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Boolean
		want  string
	}{
		{name: "true", value: true, want: "#t\r\n"},
		{name: "false", value: false, want: "#f\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildBoolean(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want Boolean
	}{
		{name: "true", data: []byte("#t\r\n"), want: true},
		{name: "false", data: []byte("#f\r\n"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildBoolean(tt.data)
			if err != nil {
				t.Fatalf("BuildBoolean() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("BuildBoolean() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildBooleanError(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "missing framing", data: []byte("#t")},
		{name: "short", data: []byte("#t\n")},
		{name: "too long", data: []byte("#true\r\n")},
		{name: "invalid boolean", data: []byte("#x\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildBoolean(tt.data); !errors.Is(err, errInvalidHeader) {
				t.Errorf("BuildBoolean() error = %v, want invalid header error", err)
			}
		})
	}
}

func TestDoubleMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Double
		want  string
	}{
		{name: "zero", value: 0, want: ",0\r\n"},
		{name: "negative", value: -1.23, want: ",-1.23\r\n"},
		{name: "with exponent 1", value: -1.23e-10, want: ",-1.23e-10\r\n"},
		{name: "with exponent 2", value: -1.23e20, want: ",-1.23e+20\r\n"},
		{name: "positive infinity", value: Double(math.Inf(1)), want: ",inf\r\n"},
		{name: "negative infinity", value: Double(math.Inf(-1)), want: ",-inf\r\n"},
		{name: "not a number", value: Double(math.NaN()), want: ",nan\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildDouble(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "zero", data: []byte(",0\r\n")},
		{name: "negative", data: []byte(",-1.23\r\n")},
		{name: "with exponent 1", data: []byte(",-1.23e-15\r\n")},
		{name: "with exponent 2", data: []byte(",-1.23e+20\r\n")},
		{name: "positive infinity", data: []byte(",inf\r\n")},
		{name: "negative infinity", data: []byte(",-inf\r\n")},
		{name: "not a number", data: []byte(",nan\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildDouble(tt.data)
			if err != nil {
				t.Fatalf("BuildDouble() error = %v", err)
			}
			assertMarshaledValue(t, got, string(tt.data))
		})
	}

	if _, err := BuildDouble([]byte(",invalid\r\n")); err == nil {
		t.Fatal("BuildDouble() error = nil, want non-nil")
	}
}

func TestBigNumberMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value *big.Int
		want  string
	}{
		{
			name:  "zero",
			value: big.NewInt(0),
			want:  "(0\r\n",
		},
		{
			name:  "nil is zero",
			value: nil,
			want:  "(0\r\n",
		},
		{
			name:  "larger than int64",
			value: mustBigInt(t, "3492890328409238509324850943850943825024385"),
			want:  "(3492890328409238509324850943850943825024385\r\n",
		},
		{
			name:  "negative",
			value: mustBigInt(t, "-3492890328409238509324850943850943825024385"),
			want:  "(-3492890328409238509324850943850943825024385\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, NewBigNumber(tt.value), tt.want)
		})
	}

	t.Run("zero value", func(t *testing.T) {
		assertMarshaledValue(t, BigNumber{}, "(0\r\n")
	})
}

func TestNewBigNumberCopiesValue(t *testing.T) {
	source := big.NewInt(42)
	value := NewBigNumber(source)
	source.SetInt64(7)

	assertMarshaledValue(t, value, "(42\r\n")
}

func TestBuildBigNumber(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "zero", data: []byte("(0\r\n")},
		{name: "positive", data: []byte("(3492890328409238509324850943850943825024385\r\n")},
		{name: "negative", data: []byte("(-3492890328409238509324850943850943825024385\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildBigNumber(tt.data)
			if err != nil {
				t.Fatalf("BuildBigNumber() error = %v", err)
			}
			assertMarshaledValue(t, got, string(tt.data))
		})
	}

	if _, err := BuildBigNumber([]byte("(invalid\r\n")); err == nil {
		t.Fatal("BuildBigNumber() error = nil, want non-nil")
	}
}

func TestBulkErrorMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value BulkError
		want  string
	}{
		{name: "nil error", value: NewBulkError(nil), want: "!0\r\n\r\n"},
		{name: "empty", value: BulkError(""), want: "!0\r\n\r\n"},
		{
			name:  "error",
			value: NewBulkError(errors.New("SYNTAX invalid syntax")),
			want:  "!21\r\nSYNTAX invalid syntax\r\n",
		},
		{name: "length is measured in bytes", value: BulkError("ERR 錯誤"), want: "!10\r\nERR 錯誤\r\n"},
		{name: "embedded sentinel", value: BulkError("ERR a\r\nb"), want: "!8\r\nERR a\r\nb\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildBulkError(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{
			name:   "empty",
			header: []byte("!0\r\n"),
			reader: bytes.NewBufferString("\r\n"),
			want:   "!0\r\n\r\n",
		},
		{
			name:   "error",
			header: []byte("!21\r\n"),
			reader: bytes.NewBufferString("SYNTAX invalid syntax\r\n"),
			want:   "!21\r\nSYNTAX invalid syntax\r\n",
		},
		{
			name:   "embedded sentinel",
			header: []byte("!8\r\n"),
			reader: bytes.NewBufferString("ERR a\r\nb\r\n"),
			want:   "!8\r\nERR a\r\nb\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildBulkError(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildBulkError() error = %v", err)
			}
			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestBuildBulkErrorError(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
	}{
		{name: "invalid length", header: []byte("!invalid\r\n")},
		{name: "negative length", header: []byte("!-1\r\n")},
		{name: "missing payload", header: []byte("!1\r\n"), reader: bytes.NewBufferString("")},
		{name: "truncated payload", header: []byte("!3\r\n"), reader: bytes.NewBufferString("ab")},
		{name: "missing sentinel", header: []byte("!2\r\n"), reader: bytes.NewBufferString("ab")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildBulkError(tt.header, tt.reader); err == nil {
				t.Fatal("BuildBulkError() error = nil, want non-nil")
			}
		})
	}
}

func TestBulkErrorError(t *testing.T) {
	const want = "ERR failure"

	if got := BulkError(want).Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestVerbatimStringMarshal(t *testing.T) {
	textEncoding := [3]byte{'t', 'x', 't'}

	tests := []struct {
		name  string
		value VerbatimString
		want  string
	}{
		{
			name:  "text",
			value: NewVerbatimString(textEncoding, "Some string"),
			want:  "=15\r\ntxt:Some string\r\n",
		},
		{
			name:  "length is measured in bytes",
			value: NewVerbatimString(textEncoding, "你好"),
			want:  "=10\r\ntxt:你好\r\n",
		},
		{
			name:  "embedded sentinel",
			value: NewVerbatimString(textEncoding, "a\r\nb"),
			want:  "=8\r\ntxt:a\r\nb\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildVerbatimString(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{
			name:   "text",
			header: []byte("=15\r\n"),
			reader: bytes.NewBufferString("txt:Some string\r\n"),
			want:   "=15\r\ntxt:Some string\r\n",
		},
		{
			name:   "length is measured in bytes",
			header: []byte("=10\r\n"),
			reader: bytes.NewBufferString("txt:你好\r\n"),
			want:   "=10\r\ntxt:你好\r\n",
		},
		{
			name:   "empty data",
			header: []byte("=4\r\n"),
			reader: bytes.NewBufferString("txt:\r\n"),
			want:   "=4\r\ntxt:\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildVerbatimString(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildVerbatimString() error = %v", err)
			}
			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestBuildVerbatimStringError(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
	}{
		{name: "payload shorter than format", header: []byte("=3\r\n"), reader: bytes.NewBufferString("txt\r\n")},
		{name: "missing encoding separator", header: []byte("=4\r\n"), reader: bytes.NewBufferString("txt!\r\n")},
		{name: "long payload missing encoding separator", header: []byte("=11\r\n"), reader: bytes.NewBufferString("txt!invalid\r\n")},
		{name: "truncated payload", header: []byte("=5\r\n"), reader: bytes.NewBufferString("txt:\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildVerbatimString(tt.header, tt.reader); err == nil {
				t.Fatal("BuildVerbatimString() error = nil, want non-nil")
			}
		})
	}
}

func TestMapMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Map
		want  string
	}{
		{name: "empty", value: NewMap(nil), want: "%0\r\n"},
		{
			name: "entries",
			value: NewMap([]MapEntry{
				{Key: SimpleString("first"), Value: Integer(1)},
				{Key: SimpleString("second"), Value: Integer(2)},
			}),
			want: "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n",
		},
		{
			name:  "nil key and value are null",
			value: NewMap([]MapEntry{{}}),
			want:  "%1\r\n_\r\n_\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildMap(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{name: "empty", header: []byte("%0\r\n"), want: "%0\r\n"},
		{
			name:   "entries",
			header: []byte("%2\r\n"),
			reader: bytes.NewBufferString("+first\r\n:1\r\n+second\r\n:2\r\n"),
			want:   "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n",
		},
		{
			name:   "nested value",
			header: []byte("%1\r\n"),
			reader: bytes.NewBufferString("+items\r\n~2\r\n:1\r\n:2\r\n"),
			want:   "%1\r\n+items\r\n~2\r\n:1\r\n:2\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildMap(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildMap() error = %v", err)
			}
			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestAttributeMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Attribute
		want  string
	}{
		{name: "empty", value: NewAttribute(nil), want: "|0\r\n"},
		{
			name: "entry",
			value: NewAttribute([]MapEntry{
				{Key: SimpleString("ttl"), Value: Integer(3600)},
			}),
			want: "|1\r\n+ttl\r\n:3600\r\n",
		},
		{
			name:  "nil key and value are null",
			value: NewAttribute([]MapEntry{{}}),
			want:  "|1\r\n_\r\n_\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildAttribute(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{name: "empty", header: []byte("|0\r\n"), want: "|0\r\n"},
		{
			name:   "entry",
			header: []byte("|1\r\n"),
			reader: bytes.NewBufferString("+ttl\r\n:3600\r\n"),
			want:   "|1\r\n+ttl\r\n:3600\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildAttribute(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildAttribute() error = %v", err)
			}
			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestSetMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Set
		want  string
	}{
		{name: "empty", value: NewSet(nil), want: "~0\r\n"},
		{
			name:  "values",
			value: NewSet([]Value{SimpleString("orange"), Integer(42), nil}),
			want:  "~3\r\n+orange\r\n:42\r\n_\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildSet(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{name: "empty", header: []byte("~0\r\n"), want: "~0\r\n"},
		{
			name:   "values",
			header: []byte("~3\r\n"),
			reader: bytes.NewBufferString("+orange\r\n:42\r\n_\r\n"),
			want:   "~3\r\n+orange\r\n:42\r\n_\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildSet(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildSet() error = %v", err)
			}
			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestPushMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Push
		want  string
	}{
		{name: "empty", value: NewPush(nil), want: ">0\r\n"},
		{
			name: "event",
			value: NewPush([]Value{
				SimpleString("message"),
				NewBulkString("channel"),
				NewBulkString("payload"),
			}),
			want: ">3\r\n+message\r\n$7\r\nchannel\r\n$7\r\npayload\r\n",
		},
		{
			name:  "nil value is null",
			value: NewPush([]Value{nil}),
			want:  ">1\r\n_\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertMarshaledValue(t, tt.value, tt.want)
		})
	}
}

func TestBuildPush(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		reader io.Reader
		want   string
	}{
		{name: "empty", header: []byte(">0\r\n"), want: ">0\r\n"},
		{
			name:   "event",
			header: []byte(">3\r\n"),
			reader: bytes.NewBufferString("+message\r\n$7\r\nchannel\r\n$7\r\npayload\r\n"),
			want:   ">3\r\n+message\r\n$7\r\nchannel\r\n$7\r\npayload\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildPush(tt.header, tt.reader)
			if err != nil {
				t.Fatalf("BuildPush() error = %v", err)
			}
			assertMarshaledValue(t, got, tt.want)
		})
	}
}

func TestBuildAggregateError(t *testing.T) {
	tests := []struct {
		name  string
		build func() error
	}{
		{
			name: "map invalid length",
			build: func() error {
				_, err := BuildMap([]byte("%invalid\r\n"), nil)
				return err
			},
		},
		{
			name: "attribute negative length",
			build: func() error {
				_, err := BuildAttribute([]byte("|-1\r\n"), nil)
				return err
			},
		},
		{
			name: "set missing value",
			build: func() error {
				_, err := BuildSet([]byte("~1\r\n"), nil)
				return err
			},
		},
		{
			name: "push truncated values",
			build: func() error {
				_, err := BuildPush([]byte(">2\r\n"), bytes.NewBufferString(":1\r\n"))
				return err
			},
		},
		{
			name: "map missing value",
			build: func() error {
				_, err := BuildMap([]byte("%1\r\n"), bytes.NewBufferString("+key\r\n"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.build(); err == nil {
				t.Fatal("Build aggregate error = nil, want non-nil")
			}
		})
	}
}

func TestReaderReadsValueAfterRESP3Aggregate(t *testing.T) {
	rd := NewReader(bytes.NewBufferString("~1\r\n,1.5\r\n+PONG\r\n"))

	set, err := rd.Read()
	if err != nil {
		t.Fatalf("Read() set error = %v", err)
	}
	assertMarshaledValue(t, set, "~1\r\n,1.5\r\n")

	value, err := rd.Read()
	if err != nil {
		t.Fatalf("Read() following value error = %v", err)
	}
	assertMarshaledValue(t, value, "+PONG\r\n")
}

func TestAggregateMarshalMatchesValueMarshal(t *testing.T) {
	values := []Value{
		SimpleString("simple"),
		NewSimpleError(errors.New("failure")),
		Integer(-42),
		NewBulkString("bulk"),
		NewArray([]Value{Integer(1)}),
		NullValue,
		Boolean(true),
		Boolean(false),
		Double(1.23),
		NewBigNumber(big.NewInt(1234567890)),
		BulkError("ERR failure"),
		NewVerbatimString([3]byte{'t', 'x', 't'}, "verbatim"),
		NewMap([]MapEntry{{Key: Integer(1), Value: Boolean(true)}}),
		NewAttribute([]MapEntry{{Key: Integer(2), Value: Double(2.5)}}),
		NewSet([]Value{Integer(3)}),
		NewPush([]Value{Integer(4)}),
	}

	want := marshalAggregateHeaderForTest(MAGIC_PUSH, len(values))
	for _, value := range values {
		want = append(want, value.Marshal()...)
	}
	assertMarshaledValue(t, NewPush(values), string(want))
}

func marshalAggregateHeaderForTest(magic byte, length int) []byte {
	return []byte(string(magic) + strconv.Itoa(length) + SENTINEL)
}

func mustBigInt(t testing.TB, value string) *big.Int {
	t.Helper()

	n, ok := new(big.Int).SetString(value, 10)
	if !ok {
		t.Fatalf("SetString(%q) failed", value)
	}
	return n
}

func BenchmarkNullMarshal(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = NullValue.Marshal()
	}
}

func BenchmarkBooleanMarshal(b *testing.B) {
	value := Boolean(true)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkDoubleMarshal(b *testing.B) {
	value := Double(1.23)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkBigNumberMarshal(b *testing.B) {
	value := NewBigNumber(mustBigInt(b, "3492890328409238509324850943850943825024385"))

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkBulkErrorMarshal(b *testing.B) {
	value := BulkError("SYNTAX invalid syntax")

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkVerbatimStringMarshal(b *testing.B) {
	value := NewVerbatimString([3]byte{'t', 'x', 't'}, "Some string")

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkMapMarshal(b *testing.B) {
	entries := make([]MapEntry, 100)
	for i := range entries {
		entries[i] = MapEntry{Key: Integer(i), Value: NewBulkString("hello")}
	}
	value := NewMap(entries)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkAttributeMarshal(b *testing.B) {
	entries := make([]MapEntry, 100)
	for i := range entries {
		entries[i] = MapEntry{Key: Integer(i), Value: NewBulkString("hello")}
	}
	value := NewAttribute(entries)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkSetMarshal(b *testing.B) {
	values := make([]Value, 100)
	for i := range values {
		values[i] = Integer(i)
	}
	value := NewSet(values)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkPushMarshal(b *testing.B) {
	values := make([]Value, 100)
	for i := range values {
		values[i] = NewBulkString("hello")
	}
	value := NewPush(values)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkMarshalResult = value.Marshal()
	}
}

func BenchmarkBuildBoolean(b *testing.B) {
	wire := []byte("#t\r\n")
	var value Boolean
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value, err = BuildBoolean(wire)
	}

	if err != nil {
		b.Fatalf("BuildBoolean() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
}

func BenchmarkBuildDouble(b *testing.B) {
	wire := []byte(",-1.23e+20\r\n")
	var value Double
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value, err = BuildDouble(wire)
	}

	if err != nil {
		b.Fatalf("BuildDouble() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
}

func BenchmarkBuildBigNumber(b *testing.B) {
	wire := []byte("(3492890328409238509324850943850943825024385\r\n")
	var value BigNumber
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		value, err = BuildBigNumber(wire)
	}

	if err != nil {
		b.Fatalf("BuildBigNumber() error = %v", err)
	}
	assertMarshaledValue(b, value, string(wire))
}

func BenchmarkBuildBulkError(b *testing.B) {
	header := []byte("!21\r\n")
	payload := []byte("SYNTAX invalid syntax\r\n")
	reader := bytes.NewReader(payload)
	var value BulkError
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildBulkError(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildBulkError() error = %v", err)
	}
	assertMarshaledValue(b, value, "!21\r\nSYNTAX invalid syntax\r\n")
}

func BenchmarkBuildVerbatimString(b *testing.B) {
	header := []byte("=15\r\n")
	payload := []byte("txt:Some string\r\n")
	reader := bytes.NewReader(payload)
	var value VerbatimString
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildVerbatimString(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildVerbatimString() error = %v", err)
	}
	assertMarshaledValue(b, value, "=15\r\ntxt:Some string\r\n")
}

func BenchmarkBuildMap(b *testing.B) {
	header := []byte("%100\r\n")
	payload := []byte(strings.Repeat(":1\r\n$5\r\nhello\r\n", 100))
	reader := bytes.NewReader(payload)
	var value Map
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildMap(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildMap() error = %v", err)
	}
	if len(value.data) != 100 {
		b.Fatalf("BuildMap() length = %d, want 100", len(value.data))
	}
}

func BenchmarkBuildAttribute(b *testing.B) {
	header := []byte("|100\r\n")
	payload := []byte(strings.Repeat(":1\r\n$5\r\nhello\r\n", 100))
	reader := bytes.NewReader(payload)
	var value Attribute
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildAttribute(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildAttribute() error = %v", err)
	}
	if len(value.data) != 100 {
		b.Fatalf("BuildAttribute() length = %d, want 100", len(value.data))
	}
}

func BenchmarkBuildSet(b *testing.B) {
	header := []byte("~100\r\n")
	payload := []byte(strings.Repeat(":1\r\n", 100))
	reader := bytes.NewReader(payload)
	var value Set
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildSet(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildSet() error = %v", err)
	}
	if len(value.data) != 100 {
		b.Fatalf("BuildSet() length = %d, want 100", len(value.data))
	}
}

func BenchmarkBuildPush(b *testing.B) {
	header := []byte(">100\r\n")
	payload := []byte(strings.Repeat("$5\r\nhello\r\n", 100))
	reader := bytes.NewReader(payload)
	var value Push
	var err error

	b.ReportAllocs()
	b.SetBytes(int64(len(header) + len(payload)))
	for b.Loop() {
		reader.Reset(payload)
		value, err = BuildPush(header, reader)
	}

	if err != nil {
		b.Fatalf("BuildPush() error = %v", err)
	}
	if len(value.data) != 100 {
		b.Fatalf("BuildPush() length = %d, want 100", len(value.data))
	}
}
