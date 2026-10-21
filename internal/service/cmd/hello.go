package cmd

import (
	"context"
	"strconv"

	"github.com/jr-dragon/peridot/internal/client"
	"github.com/jr-dragon/peridot/pkg/resp"
)

// Hello negotiates the RESP version needed for protocol-specific null replies.
// Authentication and client naming are not implemented.
type Hello struct{}

func (c *Hello) Name() string { return "HELLO" }

func (c *Hello) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	session := client.FromContext(ctx)
	protocol := 2
	if session != nil {
		protocol = session.Protocol
	}
	if len(args) > 1 {
		version := args[1].String()
		parsed, err := strconv.ParseInt(version, 10, 64)
		if err != nil || strconv.FormatInt(parsed, 10) != version {
			return nil, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "Protocol version is not an integer or out of range")
		}
		if parsed != 2 && parsed != 3 {
			return nil, NewError(ErrCommand, "NOPROTO", "unsupported protocol version")
		}
		protocol = int(parsed)
	}
	if len(args) > 2 {
		return nil, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "HELLO options are not supported")
	}
	if session != nil {
		session.Protocol = protocol
	}
	id := uint64(0)
	if session != nil {
		id = session.ID
	}
	values := []resp.Value{
		resp.NewBulkString("server"), resp.NewBulkString("peridot"),
		resp.NewBulkString("version"), resp.NewBulkString("0.0.0"),
		resp.NewBulkString("proto"), resp.Integer(protocol),
		resp.NewBulkString("id"), resp.Integer(id),
		resp.NewBulkString("mode"), resp.NewBulkString("standalone"),
		resp.NewBulkString("role"), resp.NewBulkString("master"),
		resp.NewBulkString("modules"), resp.NewArray(nil),
	}
	if protocol == 2 {
		return resp.NewArray(values), nil
	}
	entries := make([]resp.MapEntry, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		entries = append(entries, resp.MapEntry{Key: values[i], Value: values[i+1]})
	}
	return resp.NewMap(entries), nil
}
