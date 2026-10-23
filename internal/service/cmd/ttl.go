package cmd

import (
	"context"
	"fmt"

	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type TTL struct{ store storage.KeyReader }

func NewTTL(store storage.KeyReader) *TTL { return &TTL{store: store} }
func (*TTL) Name() string                 { return "TTL" }
func (c *TTL) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	ttl := int64(-2)
	err := c.store.Read(ctx, args[1].String(), func(e storage.Entry, exists bool, now int64) {
		if !exists {
			return
		}
		ttl = -1
		if e.ExpiresAt != 0 {
			// Redis rounds milliseconds to the nearest second, with half rounded up.
			ttl = (max(int64(0), e.ExpiresAt-now) + 500) / 1000
		}
	})
	if err != nil {
		return nil, fmt.Errorf("ttl: %w", err)
	}
	return resp.Integer(ttl), nil
}
