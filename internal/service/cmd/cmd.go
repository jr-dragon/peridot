package cmd

import (
	"context"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/storage"
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

// NewCommandsFromConfig constructs the application's configured command set.
func NewCommandsFromConfig(_ data.Config, db *database.DB) []Command {
	return append(NewCommands(db, db), &Save{db}, &BGSave{db}, &LastSave{db})
}

func NewCommands(store storage.Store, rewriter AOFRewriter) []Command {
	status, _ := store.(interface{ WriteError() error })
	return []Command{
		&Ping{writeStatus: status},
		&Hello{},
		NewGet(store),
		NewSet(store),
		&Select{},
		NewDel(store),
		NewTTL(store),
		NewExpire(store),
		NewDigest(store),
		&BGRewriteAOF{rewriter: rewriter},
	}
}
