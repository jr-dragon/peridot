package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/jr-dragon/peridot/internal/client"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/pkg/resp"
)

var (
	ErrServer      = errors.New("handler: server error")
	ErrClient      = errors.New("handler: client error")
	ErrClientFatal = errors.New("handler: client fatal error")
)

// HandlerFunc returns command errors for recoverable client failures. Other
// execution errors are internal failures and their details are not exposed.
type HandlerFunc func(context.Context, []resp.BulkString) (resp.Value, error)
type Middleware func(HandlerFunc) HandlerFunc

//go:generate go tool moq -out handler_mock.go . Handler

type Handler interface {
	// ServeRESP returns ErrClient for recoverable command errors, ErrClientFatal
	// for invalid protocol frames, and ErrServer for internal failures. Fatal
	// client and server failures close the connection after sending the reply.
	// Bare io.EOF signals that the connection ended between commands.
	ServeRESP(ctx context.Context, rd *resp.Command) (resp.Value, error)
}

type handler struct {
	executor map[string]HandlerFunc
	serve    HandlerFunc
	l        *slog.Logger
}

// NewHandler registers commands and panics if names collide after ASCII case
// normalization. Duplicate names indicate an invalid server configuration.
func NewHandler(cmds []cmd.Command, middlewares ...Middleware) Handler {
	h := handler{
		executor: make(map[string]HandlerFunc, len(cmds)),
		l:        slog.Default().With(slog.String("component", "handler")),
	}

	for _, command := range cmds {
		name := normalizeCommandName(command.Name())
		if _, exists := h.executor[name]; exists {
			panic(fmt.Sprintf("duplicate command %q", name))
		}
		h.executor[name] = command.Exec
	}

	h.serve = h.Exec
	for _, m := range slices.Backward(middlewares) {
		h.serve = m(h.serve)
	}

	return &h
}

func (h *handler) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) == 0 {
		h.l.DebugContext(ctx, "command rejected", slog.String("reason", "empty command"))
		return nil, cmd.NewUnknownCommandError(args)
	}
	name := normalizeCommandName(args[0].String())
	f, ok := h.executor[name]
	if !ok {
		// Unknown names and command errors may contain raw request data.
		h.l.DebugContext(ctx, "command rejected", slog.String("reason", "unknown command"), slog.Int("argument_count", len(args)-1))
		return nil, cmd.NewUnknownCommandError(args)
	}

	if !h.l.Enabled(ctx, slog.LevelDebug) {
		return f(ctx, args)
	}
	started := time.Now()
	value, err := f(ctx, args)
	outcome := "success"
	if errors.Is(err, cmd.ErrCommand) {
		outcome = "command_error"
	} else if err != nil {
		outcome = "internal_error"
	}
	h.l.DebugContext(ctx, "command completed",
		slog.String("command", name),
		slog.Int("argument_count", len(args)-1),
		slog.String("outcome", outcome),
		slog.Duration("duration", time.Since(started)),
	)
	return value, err
}

// normalizeCommandName folds ASCII letters without interpreting command bytes
// as Unicode. Redis command names are binary strings.
func normalizeCommandName(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] >= 'a' && name[i] <= 'z' {
			upper := []byte(name)
			for j := i; j < len(upper); j++ {
				if upper[j] >= 'a' && upper[j] <= 'z' {
					upper[j] -= 'a' - 'A'
				}
			}
			return string(upper)
		}
	}
	return name
}

func (h *handler) ServeRESP(ctx context.Context, rd *resp.Command) (resp.Value, error) {
	args, err := rd.Read()
	if err != nil {
		if errors.Is(err, resp.ErrProtocol) {
			var protocolErr *resp.ProtocolError
			message := "Protocol error: invalid request"
			if errors.As(err, &protocolErr) {
				message = protocolErr.Error()
			}
			return resp.NewSimpleError(errors.New(message)), fmt.Errorf("%w: %w", ErrClientFatal, err)
		} else if errors.Is(err, io.EOF) {
			return nil, io.EOF
		} else {
			return resp.InternalError, fmt.Errorf("%w: %w", ErrServer, err)
		}
	}

	res, err := h.serve(ctx, args)
	if err != nil {
		if errors.Is(err, cmd.ErrCommand) {
			if res == nil {
				var respErr resp.Value
				if !errors.As(err, &respErr) {
					respErr = resp.UnknownError
				}
				res = respErr
			}
			return res, fmt.Errorf("%w: %w", ErrClient, err)
		} else {
			return resp.InternalError, fmt.Errorf("%w: %w", ErrServer, err)
		}
	}
	if res == nil {
		res = resp.NullBulkStringValue
		if session := client.FromContext(ctx); session != nil && session.Protocol == 3 {
			res = resp.NullValue
		}
	}

	return res, nil
}
