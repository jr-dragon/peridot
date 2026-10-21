package cmd

import (
	"context"
	"errors"
	"github.com/jr-dragon/peridot/internal/liberr"
	"log/slog"
	"strings"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/pkg/resp"
)

// Save and BGSave share the database checkpoint scheduler with BGREWRITEAOF.
type Save struct{ db *database.DB }

func (*Save) Name() string { return "SAVE" }
func (c *Save) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	if err := c.db.Save(ctx); err != nil {
		return nil, snapshotError(err)
	}
	return resp.SimpleString("OK"), nil
}

type BGSave struct{ db *database.DB }

func (*BGSave) Name() string { return "BGSAVE" }
func (c *BGSave) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	schedule := false
	if len(args) > 1 {
		option, _, _ := strings.Cut(args[1].String(), "\x00")
		if len(args) != 2 || !strings.EqualFold(option, "schedule") {
			return nil, syntaxError()
		}
		schedule = true
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.db.StartSave(schedule); err != nil {
		if errors.Is(err, database.ErrSaveScheduled) {
			return resp.SimpleString("Background saving scheduled"), nil
		}
		return nil, snapshotError(err)
	}
	return resp.SimpleString("Background saving started"), nil
}

type LastSave struct{ db *database.DB }

func (*LastSave) Name() string { return "LASTSAVE" }
func (c *LastSave) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return resp.Integer(c.db.LastSave()), nil
}
func snapshotError(err error) error {
	switch {
	case errors.Is(err, database.ErrSaveInProgress):
		return NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "Background save already in progress")
	case errors.Is(err, database.ErrRewriteInProgress):
		return NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "Another child process is active (AOF?): can't BGSAVE right now. Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible.")
	case errors.Is(err, database.ErrPersistenceDisabled):
		return NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "RDB persistence is disabled")
	default:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, database.ErrClosed) {
			return err
		}
		slog.Error("RDB snapshot failed", slog.String("component", "persistence"), liberr.Attr(err))
		return &Error{SimpleError: resp.NewSimpleErrorWithCode(resp.ERRTYPE_DEFAULT, nil), cause: errors.Join(ErrCommand, err), message: "snapshot failed"}
	}
}
