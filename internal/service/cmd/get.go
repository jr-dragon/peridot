package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jr-dragon/peridot/internal/liblog"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type Get struct {
	store storage.Store
	l     *slog.Logger
}

func NewGet(store storage.Store) *Get {
	return &Get{store: store, l: slog.Default().With(slog.String("component", "get"))}
}

func (c *Get) Name() string { return "GET" }

func (c *Get) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	entry, exists, err := c.store.Get(ctx, args[1].String())
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	if !exists {
		liblog.D(ctx, c.l, "value lookup completed", slog.Bool("found", false))
		return nil, nil
	}
	value, ok := entry.Value.(string)
	if !ok {
		liblog.D(ctx, c.l, "value lookup rejected", slog.String("reason", "wrong type"))
		return nil, newWrongTypeError()
	}
	liblog.D(ctx, c.l, "value lookup completed", slog.Bool("found", true))
	return resp.NewBulkString(value), nil
}

func newWrongTypeError() *Error {
	return NewError(ErrCommand, resp.ERRTYPE_WRONGTYPE, "Operation against a key holding the wrong kind of value")
}
