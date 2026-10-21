package cmd

import (
	"context"
	"fmt"

	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
	"github.com/zeebo/xxh3"
)

type Digest struct{ store storage.Store }

func NewDigest(store storage.Store) *Digest { return &Digest{store: store} }
func (*Digest) Name() string                { return "DIGEST" }
func (c *Digest) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	entry, exists, err := c.store.Get(ctx, args[1].String())
	if err != nil {
		return nil, fmt.Errorf("digest: %w", err)
	}
	if !exists {
		return nil, nil
	}
	value, ok := entry.Value.(string)
	if !ok {
		return nil, newWrongTypeError()
	}
	return resp.NewBulkString(fmt.Sprintf("%016x", xxh3.HashString(value))), nil
}
