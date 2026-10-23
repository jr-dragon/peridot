package cmd

import (
	"context"
	"fmt"

	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type Del struct{ store storage.KeyDeleter }

func NewDel(store storage.KeyDeleter) *Del { return &Del{store: store} }
func (*Del) Name() string                  { return "DEL" }
func (c *Del) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	keys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		keys[i] = arg.String()
	}
	count, err := c.store.DeleteKeys(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("del: %w", persistenceCommandError(err))
	}
	return resp.Integer(count), nil
}
