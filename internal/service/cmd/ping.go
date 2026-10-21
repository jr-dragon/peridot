package cmd

import (
	"context"

	"github.com/jr-dragon/peridot/pkg/resp"
)

type Ping struct {
	writeStatus interface{ WriteError() error }
}

func (c *Ping) Name() string {
	return "PING"
}

func (c *Ping) Exec(_ context.Context, cmd []resp.BulkString) (resp.Value, error) {
	if len(cmd) == 0 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	// Redis checks persistence before executing PING, including its optional
	// payload and command-specific upper arity check.
	if c.writeStatus != nil {
		if err := c.writeStatus.WriteError(); err != nil {
			return nil, persistenceCommandError(err)
		}
	}
	switch len(cmd) {
	case 1:
		return resp.SimpleString("PONG"), nil
	case 2:
		return cmd[1], nil
	default:
		return nil, newWrongNumberOfArgumentsError(c)
	}
}
