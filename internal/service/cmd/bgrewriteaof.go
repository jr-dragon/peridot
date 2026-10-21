package cmd

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/liberr"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type AOFRewriter interface{ StartRewrite() error }

// BGRewriteAOF exports a snapshot even when ongoing AOF logging is disabled.
type BGRewriteAOF struct{ rewriter AOFRewriter }

func (c *BGRewriteAOF) Name() string { return "BGREWRITEAOF" }

func (c *BGRewriteAOF) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.rewriter == nil {
		return nil, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "AOF persistence is disabled")
	}
	if err := c.rewriter.StartRewrite(); err != nil {
		if errors.Is(err, database.ErrPersistenceDisabled) {
			return nil, NewError(err, resp.ERRTYPE_DEFAULT, "AOF persistence is disabled")
		}
		if errors.Is(err, database.ErrRewriteScheduled) {
			return resp.SimpleString("Background append only file rewriting scheduled"), nil
		}
		if errors.Is(err, database.ErrRewriteInProgress) {
			return nil, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "Background append only file rewriting already in progress")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, database.ErrClosed) {
			return nil, err
		}
		slog.Error("AOF rewrite failed", slog.String("component", "persistence"), liberr.Attr(err))
		return nil, NewError(err, resp.ERRTYPE_DEFAULT, "Can't execute an AOF background rewriting. Please check the server logs for more information.")
	}
	return resp.SimpleString("Background append only file rewriting started"), nil
}
