package aof

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func logBytes(commands ...[]string) []byte {
	var b []byte
	for _, args := range commands {
		b = append(b, Encode(args...)...)
	}
	return b
}

func TestEncodeBinaryAndExpiration(t *testing.T) {
	got := Replacement("k\x00", "\xff\r\n", 1234)
	want := "*5\r\n$3\r\nSET\r\n$2\r\nk\x00\r\n$3\r\n\xff\r\n\r\n$4\r\nPXAT\r\n$4\r\n1234\r\n"
	if string(got) != want {
		t.Fatalf("wire = %q, want %q", got, want)
	}
	state, err := Load(bytes.NewReader(got))
	if err != nil {
		t.Fatal(err)
	}
	if want := (String{Value: "\xff\r\n", ExpiresAt: 1234, HasExpiration: true}); state["k\x00"] != want {
		t.Fatalf("state = %#v", state)
	}
}

func TestLoadResolvedStringHistory(t *testing.T) {
	input := append([]byte("#TS:123\r\n"), logBytes(
		[]string{"SELECT", "0"},
		[]string{"SET", "keep", "before", "PXAT", "1"},
		[]string{"SET", "keep", "after", "KEEPTTL"},
		[]string{"PERSIST", "keep"},
		[]string{"SET", "gone", "x"},
		[]string{"DEL", "gone", "absent"},
		[]string{"MULTI"},
		[]string{"SET", "txn", "old", "NX"},
		[]string{"SET", "txn", "new", "IFEQ", "old"},
		[]string{"PEXPIREAT", "txn", "1000"},
		[]string{"EXEC"},
		[]string{"SETNX", "new", "yes"},
		[]string{"SET", "empty", ""},
		[]string{"SET", "unlinked", "x"},
		[]string{"UNLINK", "unlinked"},
		[]string{"SET", "epoch", "x"},
		[]string{"PEXPIREAT", "epoch", "0"},
	)...)
	state, err := Load(bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]String{
		"keep": {Value: "after"}, "txn": {Value: "new", ExpiresAt: 1000, HasExpiration: true},
		"new": {Value: "yes"}, "empty": {}, "epoch": {Value: "x", HasExpiration: true},
	}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("state = %#v, want %#v", state, want)
	}
}

func TestLoadRetainedSetOptions(t *testing.T) {
	cases := []struct {
		name     string
		commands [][]string
		want     String
	}{
		{"absent short digest", [][]string{{"SET", "key", "value", "IFDNE", "bad"}}, String{Value: "value"}},
		{"absent empty digest", [][]string{{"SET", "key", "value", "IFDNE", ""}}, String{Value: "value"}},
		{"overridden digest", [][]string{{"SET", "key", "old"}, {"SET", "key", "value", "IFDNE", "bad", "IFDNE", "0000000000000000"}}, String{Value: "value"}},
		{"overridden milliseconds", [][]string{{"SET", "key", "value", "PXAT", "bad", "PXAT", "1234"}}, String{Value: "value", ExpiresAt: 1234, HasExpiration: true}},
		{"overridden seconds overflow", [][]string{{"SET", "key", "value", "EXAT", "9223372036854776", "EXAT", "2"}}, String{Value: "value", ExpiresAt: 2000, HasExpiration: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := Load(bytes.NewReader(logBytes(tc.commands...)))
			if err != nil {
				t.Fatal(err)
			}
			if got, exists := state["key"]; !exists || got != tc.want {
				t.Fatalf("key = %#v,%v, want %#v", got, exists, tc.want)
			}
		})
	}
}

func TestLoadRetainedExpirationOptions(t *testing.T) {
	for _, flags := range [][]string{{"NX"}, {"XX"}, {"GT"}, {"LT"}, {"XX", "GT"}, {"XX", "LT"}, {"NX", "NX"}, {"nx\x00suffix"}} {
		t.Run(strings.Join(flags, "-"), func(t *testing.T) {
			initial := []string{"SET", "key", "value"}
			if flags[0] == "XX" || flags[0] == "GT" || flags[0] == "LT" {
				initial = append(initial, "PXAT", "1000")
			}
			deadline := "2000"
			if flags[len(flags)-1] == "LT" {
				deadline = "500"
			}
			command := append([]string{"PEXPIREAT", "key", deadline}, flags...)
			state, err := Load(bytes.NewReader(logBytes(initial, command)))
			if err != nil {
				t.Fatal(err)
			}
			expiresAt, _ := integer(deadline)
			if want := (String{Value: "value", ExpiresAt: expiresAt, HasExpiration: true}); state["key"] != want {
				t.Fatalf("key = %#v, want %#v", state["key"], want)
			}
			// Historical expiration must not use today's clock: PERSIST can
			// still restore a value whose recorded deadline is in the past.
			state, err = Load(bytes.NewReader(logBytes(initial, command, []string{"PERSIST", "key"})))
			if err != nil || state["key"] != (String{Value: "value"}) {
				t.Fatalf("persisted history = %#v, error = %v", state, err)
			}
		})
	}
}

func TestLoadRejectsInvalidRetainedOptions(t *testing.T) {
	cases := [][]string{
		{"SET", "key", "value", "PXAT", "1234", "PXAT", "bad"},
		{"SET", "key", "value", "EXAT", "1", "EXAT", "9223372036854776"},
		{"SET", "key", "value", "IFDNE", "0000000000000000", "IFDNE", "bad"},
		{"SET", "key", "value", "IFDEQ", "bad"},
		{"SET", "key", "value", "IFDNE"},
		{"SET", "key", "value", "PXAT", "bad", "EXAT", "1234"},
		{"PEXPIREAT", "key"},
		{"PEXPIREAT", "key", "bad", "NX"},
		{"PEXPIREAT", "key", "1234", "NX", "XX"},
		{"PEXPIREAT", "key", "1234", "NX", "GT"},
		{"PEXPIREAT", "key", "1234", "NX", "LT"},
		{"PEXPIREAT", "key", "1234", "GT", "LT"},
		{"PEXPIREAT", "key", "1234", "unknown"},
		{"PEXPIREAT", "key", "1234", "ＮＸ"},
	}
	for _, command := range cases {
		t.Run(strings.Join(command, "-"), func(t *testing.T) {
			input := logBytes([]string{"SET", "key", "old", "PXAT", "1"}, command)
			if state, err := Load(bytes.NewReader(input)); !errors.Is(err, ErrFormat) || state != nil {
				t.Fatalf("state = %#v, error = %v", state, err)
			}
		})
	}
}

func TestLoadRejectsInvalidOrUnsupportedInput(t *testing.T) {
	cases := map[string][]byte{
		"inline": []byte("SET x y\r\n"), "rdb": []byte("REDIS0015"),
		"manifest":    []byte("file appendonly.aof.1.base.aof seq 1 type b\n"),
		"empty array": []byte("*0\r\n"), "null array": []byte("*-1\r\n"),
		"null bulk":       []byte("*1\r\n$-1\r\n"),
		"bad sentinel":    []byte("*1\r\n$3\r\nSETxx"),
		"bad header":      []byte("*1\n$3\r\nSET\r\n"),
		"large bulk":      []byte("*1\r\n$536870913\r\n"),
		"large count":     []byte("*2147483648\r\n"),
		"annotation":      []byte("#TS:123"),
		"long annotation": []byte("#" + strings.Repeat("x", 64<<10) + "\r\n"),
		"unknown":         Encode("INCR", "key"), "wrong type": Encode("SADD", "key", "x"),
		"database": Encode("SELECT", "1"), "bad set": Encode("SET", "key"),
		"relative ttl":           Encode("SET", "key", "value", "EX", "10"),
		"bad ttl":                Encode("SET", "key", "value", "PXAT", "0"),
		"bad expiry":             Encode("PEXPIREAT", "key", "no"),
		"missing match":          Encode("SET", "key", "value", "IFEQ"),
		"unicode command":        Encode("ſET", "key", "value"),
		"unicode option":         Encode("SET", "key", "value", "KEEPTTL"),
		"conflicting conditions": Encode("SET", "key", "value", "NX", "XX"),
		"conflicting expiration": Encode("SET", "key", "value", "PXAT", "123", "KEEPTTL"),
		"expiration overflow":    Encode("SET", "key", "value", "EXAT", "9223372036854776"),
		"signed length":          []byte("*+1\r\n$3\r\nSET\r\n"),
		"open transaction":       logBytes([]string{"MULTI"}, []string{"SET", "key", "value"}),
		"nested transaction":     logBytes([]string{"MULTI"}, []string{"MULTI"}),
		"stray exec":             Encode("EXEC"), "arity": Encode("DEL"),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			state, err := Load(bytes.NewReader(input))
			if !errors.Is(err, ErrFormat) || state != nil {
				t.Fatalf("state=%v, error=%v", state, err)
			}
		})
	}
}

func TestLoadRejectsEveryTruncatedCommand(t *testing.T) {
	first := Encode("SET", "stable", "original")
	last := Encode("SET", "binary\x00", "\xff\r\n", "PXAT", "9999999999999")
	for cut := 1; cut < len(last); cut++ {
		input := append(bytes.Clone(first), last[:cut]...)
		if state, err := Load(bytes.NewReader(input)); !errors.Is(err, ErrFormat) || state != nil {
			t.Fatalf("cut %d: state=%v, error=%v", cut, state, err)
		}
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestLoadPreservesIOFailure(t *testing.T) {
	cause := errors.New("read failed")
	if _, err := Load(io.MultiReader(bytes.NewReader(Encode("SET", "key", "value")), errorReader{cause})); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}
