package cmd

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type Expire struct{ store storage.Store }

func NewExpire(store storage.Store) *Expire { return &Expire{store: store} }
func (*Expire) Name() string                { return "EXPIRE" }

type expireOptions struct{ nx, xx, gt, lt bool }

func parseExpireOptions(args []resp.BulkString) (expireOptions, error) {
	var opts expireOptions
	for _, arg := range args {
		option := strings.SplitN(arg.String(), "\x00", 2)[0]
		switch foldOption(option) {
		case "NX":
			opts.nx = true
		case "XX":
			opts.xx = true
		case "GT":
			opts.gt = true
		case "LT":
			opts.lt = true
		default:
			option = strings.NewReplacer("\r", " ", "\n", " ").Replace(option)
			return opts, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "Unsupported option "+option)
		}
	}
	if opts.nx && (opts.xx || opts.gt || opts.lt) {
		return opts, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "NX and XX, GT or LT options at the same time are not compatible")
	}
	if opts.gt && opts.lt {
		return opts, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "GT and LT options at the same time are not compatible")
	}
	return opts, nil
}

func (c *Expire) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	opts, err := parseExpireOptions(args[3:])
	if err != nil {
		return nil, writeValidationError(c.store, err)
	}
	seconds, err := strconv.ParseInt(args[2].String(), 10, 64)
	if err != nil || strconv.FormatInt(seconds, 10) != args[2].String() {
		return nil, writeValidationError(c.store, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "value is not an integer or out of range"))
	}
	invalid := func() error {
		return NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "invalid expire time in 'expire' command")
	}
	if seconds > math.MaxInt64/1000 || seconds < math.MinInt64/1000 {
		return nil, writeValidationError(c.store, invalid())
	}
	milliseconds := seconds * 1000
	var applied int64
	err = c.store.Update(ctx, args[1].String(), func(e storage.Entry, exists bool, now int64) (storage.Entry, bool, error) {
		if milliseconds > math.MaxInt64-now {
			return e, exists, invalid()
		}
		deadline := now + milliseconds
		if !exists || (opts.nx && e.ExpiresAt != 0) || (opts.xx && e.ExpiresAt == 0) ||
			(opts.gt && (e.ExpiresAt == 0 || deadline <= e.ExpiresAt)) ||
			(opts.lt && e.ExpiresAt != 0 && deadline >= e.ExpiresAt) {
			return e, exists, storage.ErrUnchanged
		}
		applied = 1
		if deadline <= now {
			return storage.Entry{}, false, nil
		}
		e.ExpiresAt = deadline
		return e, true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("expire: %w", persistenceCommandError(err))
	}
	return resp.Integer(applied), nil
}
