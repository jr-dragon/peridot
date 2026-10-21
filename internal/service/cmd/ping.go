package cmd

import (
	"context"

	"github.com/jr-dragon/peridot/pkg/resp"
)

type Ping struct{}

func (c *Ping) Name() string {
	return "PING"
}

func (c *Ping) Exec(_ context.Context, cmd []resp.BulkString) (resp.Value, error) {
	switch len(cmd) {
	case 1:
		return resp.SimpleString("PONG"), nil
	case 2:
		return cmd[1], nil
	default:
		return nil, newWrongNumberOfArgumentsError(c)
	}
}
