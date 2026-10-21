package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type namedPing struct {
	cmd.Ping
	name string
}

func (p *namedPing) Name() string { return p.name }

func TestHandlerNormalizesRegisteredNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request string
	}{
		{name: "ping", request: "PING"},
		{name: "pInG", request: "ping"},
		{name: "PıNG", request: "pıng"},
		{name: "P\xffNG", request: "p\xffng"},
	} {
		t.Run(fmt.Sprintf("%q", tc.name), func(t *testing.T) {
			h := NewHandler([]cmd.Command{&namedPing{name: tc.name}})
			wire := resp.NewArray([]resp.Value{resp.NewBulkString(tc.request)}).Marshal()
			value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader(string(wire))))
			if err != nil || value == nil || string(value.Marshal()) != "+PONG\r\n" {
				t.Fatalf("ServeRESP() = %v, %v", value, err)
			}
		})
	}
}

func TestHandlerRejectsDuplicateCommandNames(t *testing.T) {
	for _, name := range []string{"PING", "ping", "pInG"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if got := recover(); got != `duplicate command "PING"` {
					t.Fatalf("NewHandler() panic = %v, want duplicate command error", got)
				}
			}()
			NewHandler([]cmd.Command{&cmd.Ping{}, &namedPing{name: name}})
		})
	}
}

func TestHandlerPreservesCommandErrorCodeAndCause(t *testing.T) {
	cause := errors.New("access denied")
	commandErr := cmd.NewError(cause, "NOAUTH", "Authentication required.")
	h := NewHandler(nil, func(HandlerFunc) HandlerFunc {
		return func(context.Context, []resp.BulkString) (resp.Value, error) {
			return nil, fmt.Errorf("authentication: %w", commandErr)
		}
	})
	value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader("PING\r\n")))
	var reply *cmd.Error
	if !errors.Is(err, cmd.ErrCommand) || !errors.Is(err, cause) || !errors.As(err, &reply) || reply != commandErr {
		t.Fatalf("ServeRESP() lost command error or cause: %v", err)
	}
	if value == nil || string(value.Marshal()) != "-NOAUTH Authentication required.\r\n" {
		t.Fatalf("ServeRESP() reply = %v, want NOAUTH reply", value)
	}
}

func TestHandlerPipelineContinuesAfterCommandErrors(t *testing.T) {
	h := NewHandler(cmd.NewCommands(storage.NewXsync(), nil))
	reader := resp.NewCommand(strings.NewReader("PıNG\r\nPING one two\r\nPING\r\n"))
	for _, tc := range []struct {
		want string
		err  error
	}{
		{want: "-ERR unknown command 'PıNG'\r\n", err: cmd.ErrCommand},
		{want: "-ERR wrong number of arguments for 'ping' command\r\n", err: cmd.ErrWrongArity},
		{want: "+PONG\r\n"},
	} {
		value, err := h.ServeRESP(context.Background(), reader)
		if !errors.Is(err, tc.err) || value == nil || string(value.Marshal()) != tc.want {
			t.Fatalf("ServeRESP() = %v, %v, want %q, %v", value, err, tc.want, tc.err)
		}
	}
}
