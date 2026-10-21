package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type rewriteFunc func() error

func (f rewriteFunc) StartRewrite() error { return f() }

func TestBGRewriteAOFReplies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rewriter AOFRewriter
		args     []string
		want     string
	}{
		{"disabled", nil, []string{"BGREWRITEAOF"}, "-ERR AOF persistence is disabled\r\n"},
		{"arity", nil, []string{"BGREWRITEAOF", "extra"}, "-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"},
		{"started", rewriteFunc(func() error { return nil }), []string{"BGREWRITEAOF"}, "+Background append only file rewriting started\r\n"},
		{"running", rewriteFunc(func() error { return database.ErrRewriteInProgress }), []string{"BGREWRITEAOF"}, "-ERR Background append only file rewriting already in progress\r\n"},
		{"preparation failed", rewriteFunc(func() error { return errors.New("private filesystem details") }), []string{"BGREWRITEAOF"}, "-ERR Can't execute an AOF background rewriting. Please check the server logs for more information.\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := commandReply(t, &BGRewriteAOF{rewriter: tc.rewriter}, tc.args...); got != tc.want {
				t.Fatalf("reply=%q, want=%q", got, tc.want)
			}
		})
	}
	cause := errors.New("internal failure")
	c := &BGRewriteAOF{rewriter: rewriteFunc(func() error { return cause })}
	if _, err := c.Exec(context.Background(), []resp.BulkString{resp.NewBulkString("BGREWRITEAOF")}); !errors.Is(err, cause) || !errors.Is(err, ErrCommand) {
		t.Fatalf("lost recoverable failure or cause: %v", err)
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, database.ErrClosed} {
		c := &BGRewriteAOF{rewriter: rewriteFunc(func() error { return cause })}
		if _, err := c.Exec(context.Background(), []resp.BulkString{resp.NewBulkString("BGREWRITEAOF")}); !errors.Is(err, cause) || errors.Is(err, ErrCommand) {
			t.Fatalf("lifecycle error became recoverable: %v", err)
		}
	}
}
