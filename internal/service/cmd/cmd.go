package cmd

import (
	"context"

	"github.com/jr-dragon/peridot/pkg/resp"
)

// Command implements a Redis command. Instances are shared across connections
// and must support concurrent execution.
type Command interface {
	// Name returns the command name. Registration and lookup ignore ASCII case.
	Name() string
	// Exec receives the command name at args[0]. The argument slice is borrowed
	// until Exec returns; implementations must copy it before retaining it.
	// A nil result with no error becomes a null bulk string. Command errors must
	// match ErrCommand; other errors are internal failures and are not exposed.
	Exec(context.Context, []resp.BulkString) (resp.Value, error)
}

func NewCommands() []Command {
	return []Command{
		&Ping{},
	}
}
