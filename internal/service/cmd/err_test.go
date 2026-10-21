package cmd

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/pkg/resp"
)

type commandErrorCause struct{}

func (*commandErrorCause) Error() string { return "storage failure" }

func TestNewErrorPreservesCodeAndCause(t *testing.T) {
	cause := &commandErrorCause{}
	for _, tc := range []struct {
		name    string
		code    string
		message string
		cause   error
		want    string
	}{
		{name: "default", code: "ERR", message: "failure", want: "-ERR failure\r\n"},
		{name: "wrong type", code: "WRONGTYPE", message: "wrong kind of value", cause: cause, want: "-WRONGTYPE wrong kind of value\r\n"},
		{name: "authentication", code: "NOAUTH", message: "Authentication required.", cause: cause, want: "-NOAUTH Authentication required.\r\n"},
		{name: "wrong arity", code: "ERR", message: "wrong arity", cause: ErrWrongArity, want: "-ERR wrong arity\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := NewError(tc.cause, tc.code, tc.message)
			wrapped := fmt.Errorf("execution: %w", err)
			if !errors.Is(wrapped, ErrCommand) || (tc.cause != nil && !errors.Is(wrapped, tc.cause)) {
				t.Fatalf("error lost command classification or cause: %v", wrapped)
			}
			if tc.cause == cause {
				var gotCause *commandErrorCause
				if !errors.As(wrapped, &gotCause) || gotCause != cause {
					t.Fatalf("error lost typed cause: %v", wrapped)
				}
			}
			var value resp.Value
			if !errors.As(wrapped, &value) || string(value.Marshal()) != tc.want {
				t.Fatalf("reply = %v, want %q", value, tc.want)
			}
			if err.Error() != tc.message {
				t.Fatalf("Error() = %q, want %q", err.Error(), tc.message)
			}
		})
	}
}

func TestUnknownCommandErrorRedis8102(t *testing.T) {
	// Expected replies follow Redis 8.10.2's commandCheckExistence in src/server.c.
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "no command", want: "-ERR unknown command ''\r\n"},
		{name: "no arguments", args: []string{"MISSING"}, want: "-ERR unknown command 'MISSING'\r\n"},
		{name: "preserve case", args: []string{"MiSsInG", "one", "two"}, want: "-ERR unknown command 'MiSsInG', with args beginning with: 'one' 'two' \r\n"},
		{name: "empty command", args: []string{"", "one"}, want: "-ERR unknown command '', with args beginning with: 'one' \r\n"},
		{name: "empty argument", args: []string{"MISSING", ""}, want: "-ERR unknown command 'MISSING', with args beginning with: '' \r\n"},
		{name: "quotes and backslashes", args: []string{"MISSING", "a'b", `c\d`}, want: "-ERR unknown command 'MISSING', with args beginning with: 'a'b' 'c\\d' \r\n"},
		{name: "command at byte limit", args: []string{strings.Repeat("x", 128)}, want: "-ERR unknown command '" + strings.Repeat("x", 128) + "'\r\n"},
		{name: "command past byte limit", args: []string{strings.Repeat("x", 129)}, want: "-ERR unknown command '" + strings.Repeat("x", 128) + "'\r\n"},
		{name: "command splits UTF-8", args: []string{strings.Repeat("x", 127) + "é"}, want: "-ERR unknown command '" + strings.Repeat("x", 127) + "\xc3'\r\n"},
		{name: "command NUL", args: []string{"MIS\x00SING"}, want: "-ERR unknown command 'MIS'\r\n"},
		{name: "argument NUL", args: []string{"MISSING", "one\x00hidden", "two"}, want: "-ERR unknown command 'MISSING', with args beginning with: 'one' 'two' \r\n"},
		{name: "argument starts with NUL", args: []string{"MISSING", "\x00hidden", "two"}, want: "-ERR unknown command 'MISSING', with args beginning with: '' 'two' \r\n"},
		{name: "replace CR and LF", args: []string{"MIS\r\nSING", "one\rtwo\n"}, want: "-ERR unknown command 'MIS  SING', with args beginning with: 'one two ' \r\n"},
		{name: "preserve binary bytes", args: []string{"MIS\xffSING", "\xfe\x01"}, want: "-ERR unknown command 'MIS\xffSING', with args beginning with: '\xfe\x01' \r\n"},
		{name: "argument below byte limit", args: []string{"MISSING", strings.Repeat("a", 127), "hidden"}, want: "-ERR unknown command 'MISSING', with args beginning with: '" + strings.Repeat("a", 127) + "' \r\n"},
		{name: "argument at byte limit", args: []string{"MISSING", strings.Repeat("a", 128), "hidden"}, want: "-ERR unknown command 'MISSING', with args beginning with: '" + strings.Repeat("a", 128) + "' \r\n"},
		{name: "argument past byte limit", args: []string{"MISSING", strings.Repeat("a", 129), "hidden"}, want: "-ERR unknown command 'MISSING', with args beginning with: '" + strings.Repeat("a", 128) + "' \r\n"},
		{name: "argument splits UTF-8", args: []string{"MISSING", strings.Repeat("a", 127) + "é"}, want: "-ERR unknown command 'MISSING', with args beginning with: '" + strings.Repeat("a", 127) + "\xc3' \r\n"},
		{name: "remaining budget includes quotes and spaces", args: []string{"MISSING", strings.Repeat("a", 124), "xyz", "hidden"}, want: "-ERR unknown command 'MISSING', with args beginning with: '" + strings.Repeat("a", 124) + "' 'x' \r\n"},
		{name: "summary exactly fills budget", args: []string{"MISSING", strings.Repeat("a", 125), "hidden"}, want: "-ERR unknown command 'MISSING', with args beginning with: '" + strings.Repeat("a", 125) + "' \r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := make([]resp.BulkString, len(tc.args))
			for i, arg := range tc.args {
				args[i] = resp.NewBulkString(arg)
			}
			err := NewUnknownCommandError(args)
			if got := string(err.Marshal()); got != tc.want {
				t.Fatalf("Marshal() = %q, want %q", got, tc.want)
			}
			if !errors.Is(err, ErrCommand) {
				t.Fatalf("error lost ErrCommand cause: %v", err)
			}
			if want := strings.TrimSuffix(strings.TrimPrefix(tc.want, "-ERR "), "\r\n"); err.Error() != want {
				t.Fatalf("Error() = %q, want %q", err.Error(), want)
			}
		})
	}
}
