package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func TestHandlerServeRESP(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wire      string
		want      string
		err       error
		class     error
		closeConn bool
	}{
		{name: "ping", wire: "*1\r\n$4\r\nPING\r\n", want: "+PONG\r\n"},
		{name: "case insensitive inline", wire: "ping\r\n", want: "+PONG\r\n"},
		{name: "mixed case", wire: "pInG\r\n", want: "+PONG\r\n"},
		{name: "Unicode is not ASCII case", wire: "PıNG\r\n", want: "-ERR unknown command 'PıNG'\r\n", err: cmd.ErrCommand, class: ErrClient},
		{name: "invalid UTF-8 command", wire: "*1\r\n$4\r\nP\xffNG\r\n", want: "-ERR unknown command 'P\xffNG'\r\n", err: cmd.ErrCommand, class: ErrClient},
		{name: "ping argument", wire: "PING \"hello world\"\r\n", want: "$11\r\nhello world\r\n"},
		{name: "ping empty argument", wire: "PING \"\"\r\n", want: "$0\r\n\r\n"},
		{name: "ping binary argument", wire: "*2\r\n$4\r\nPING\r\n$4\r\n\x00\r\n\xff\r\n", want: "$4\r\n\x00\r\n\xff\r\n"},
		{name: "unknown command", wire: "MISSING\r\n", want: "-ERR unknown command 'MISSING'\r\n", err: cmd.ErrCommand, class: ErrClient},
		{name: "unknown command arguments", wire: "MISSING one \"two words\"\r\n", want: "-ERR unknown command 'MISSING', with args beginning with: 'one' 'two words' \r\n", err: cmd.ErrCommand, class: ErrClient},
		{name: "unknown command binary arguments", wire: "*2\r\n$8\r\nMIS\x00SING\r\n$7\r\na\r\nb\x00cd\r\n", want: "-ERR unknown command 'MIS', with args beginning with: 'a  b' \r\n", err: cmd.ErrCommand, class: ErrClient},
		{name: "wrong arity", wire: "PING one two\r\n", want: "-ERR wrong number of arguments for 'ping' command\r\n", err: cmd.ErrWrongArity, class: ErrClient},
		{name: "protocol error", wire: "*1\r\n+PING\r\n", err: resp.ErrProtocol, class: ErrClientFatal, closeConn: true},
		{name: "truncated request", wire: "*1\r\n", err: resp.ErrProtocol, class: ErrClientFatal, closeConn: true},
		{name: "end of connection", err: io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(cmd.NewCommands())
			value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader(tc.wire)))
			if !errors.Is(err, tc.err) {
				t.Fatalf("ServeRESP() error = %v, want %v", err, tc.err)
			}
			if tc.want != "" && (value == nil || string(value.Marshal()) != tc.want) {
				t.Fatalf("ServeRESP() value = %v, want %q", value, tc.want)
			}
			if tc.err == io.EOF {
				if err != io.EOF || value != nil {
					t.Fatalf("connection EOF = %v, %v, want nil, bare io.EOF", value, err)
				}
			} else if tc.err != nil {
				if !errors.Is(err, tc.class) {
					t.Fatalf("ServeRESP() error = %v, want class %v", err, tc.class)
				}
			}
			if tc.closeConn && (value == nil || !strings.HasPrefix(string(value.Marshal()), "-ERR ")) {
				t.Fatalf("protocol error did not produce an error reply: %v", value)
			}
		})
	}
}

func TestHandlerMiddlewareOrderAndContext(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request")
	var order []string
	middleware := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(got context.Context, args []resp.BulkString) (resp.Value, error) {
				if got.Value(contextKey{}) != "request" || len(args) != 1 || args[0].String() != "PING" {
					t.Error("middleware lost request context or arguments")
				}
				order = append(order, name+" before")
				value, err := next(got, args)
				order = append(order, name+" after")
				return value, err
			}
		}
	}
	h := NewHandler(cmd.NewCommands(), middleware("outer"), middleware("inner"))
	value, err := h.ServeRESP(ctx, resp.NewCommand(strings.NewReader("PING\r\n")))
	if err != nil || string(value.Marshal()) != "+PONG\r\n" {
		t.Fatalf("ServeRESP() = %v, %v", value, err)
	}
	if got := strings.Join(order, ", "); got != "outer before, inner before, inner after, outer after" {
		t.Fatalf("middleware order = %s", got)
	}
}

func TestHandlerClassifiesExecutionResults(t *testing.T) {
	serverErr := errors.New("execution failed")
	for _, tc := range []struct {
		name      string
		value     resp.Value
		err       error
		want      string
		class     error
		closeConn bool
	}{
		{name: "nil result", want: "$-1\r\n"},
		{name: "command error fallback", err: cmd.ErrCommand, want: "-ERR unknown error\r\n", class: ErrClient},
		{name: "explicit command reply", value: resp.SimpleString("CUSTOM"), err: cmd.ErrCommand, want: "+CUSTOM\r\n", class: ErrClient},
		{name: "internal error", err: serverErr, want: "-ERR internal server error\r\n", class: ErrServer, closeConn: true},
		{name: "internal error masks explicit result", value: resp.SimpleString("SECRET"), err: serverErr, want: "-ERR internal server error\r\n", class: ErrServer, closeConn: true},
		{name: "execution EOF", err: io.EOF, want: "-ERR internal server error\r\n", class: ErrServer, closeConn: true},
		{name: "wrapped execution EOF", err: fmt.Errorf("storage: %w", io.EOF), want: "-ERR internal server error\r\n", class: ErrServer, closeConn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(nil, func(HandlerFunc) HandlerFunc {
				return func(context.Context, []resp.BulkString) (resp.Value, error) { return tc.value, tc.err }
			})
			value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader("TEST\r\n")))
			if !errors.Is(err, tc.err) {
				t.Fatalf("ServeRESP() error = %v, want cause %v", err, tc.err)
			}
			if tc.err != nil {
				if !errors.Is(err, tc.class) {
					t.Fatalf("ServeRESP() error = %v, want class %v", err, tc.class)
				}
			}
			if value == nil || string(value.Marshal()) != tc.want {
				t.Fatalf("ServeRESP() = %v, want %q", value, tc.want)
			}
		})
	}
}

func TestHandlerPreservesWrappedCommandErrorCause(t *testing.T) {
	cause := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("middleware failure")}
	commandErr := cmd.NewError(cause, "NOAUTH", "Authentication required.")
	h := NewHandler(nil, func(HandlerFunc) HandlerFunc {
		return func(context.Context, []resp.BulkString) (resp.Value, error) {
			return nil, fmt.Errorf("middleware: %w", commandErr)
		}
	})
	value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader("PING\r\n")))
	var gotCause *net.OpError
	if !errors.Is(err, ErrClient) || !errors.Is(err, cause) || !errors.As(err, &gotCause) || gotCause != cause {
		t.Fatalf("middleware classification or cause changed: %v", err)
	}
	if value == nil || string(value.Marshal()) != "-NOAUTH Authentication required.\r\n" {
		t.Fatalf("reply = %v, want NOAUTH reply", value)
	}
}

type failingCommandReader struct{ err error }

func (r failingCommandReader) Read([]byte) (int, error) { return 0, r.err }

func TestHandlerClassifiesReadFailuresAsServerErrors(t *testing.T) {
	for _, prefix := range []string{"", "*1\r\n", "*1\r\n$4\r\nPI"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			cause := errors.New("read failed")
			rd := io.MultiReader(strings.NewReader(prefix), failingCommandReader{err: cause})
			value, err := NewHandler(nil).ServeRESP(context.Background(), resp.NewCommand(rd))
			if value == nil || string(value.Marshal()) != "-ERR internal server error\r\n" ||
				!errors.Is(err, cause) || !errors.Is(err, ErrServer) {
				t.Fatalf("read failure = %v, %v, want a server error with a safe reply", value, err)
			}
		})
	}
}
