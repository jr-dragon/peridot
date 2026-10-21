package cmd

import (
	"context"

	"github.com/jr-dragon/peridot/pkg/resp"
)

// Select deliberately rejects every database index, including zero.
type Select struct{}

func (*Select) Name() string { return "SELECT" }
func (c *Select) Exec(_ context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	return nil, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "SELECT is not allowed: Peridot does not support multiple databases")
}
