package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
	"github.com/zeebo/xxh3"
)

type Set struct {
	store storage.Store
	l     *slog.Logger
}

func NewSet(store storage.Store) *Set {
	return &Set{store: store, l: slog.Default().With(slog.String("component", "set"))}
}

func (c *Set) Name() string { return "SET" }

type setOptions struct {
	condition, match, expiration, ttl string
	get                               bool
}

func syntaxError() *Error { return NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "syntax error") }

// foldOption folds ASCII without interpreting arbitrary command bytes as Unicode.
func foldOption(option string) string {
	buf := []byte(option)
	for i, b := range buf {
		if b >= 'a' && b <= 'z' {
			buf[i] = b - ('a' - 'A')
		}
	}
	return string(buf)
}

func parseSetOptions(args []resp.BulkString) (setOptions, error) {
	var opts setOptions
	for i := 3; i < len(args); i++ {
		option := foldOption(args[i].String())
		if nul := strings.IndexByte(option, 0); nul >= 0 {
			// Redis's option comparisons terminate at the first C NUL.
			option = option[:nul]
		}
		switch option {
		case "NX", "XX", "IFEQ", "IFNE", "IFDEQ", "IFDNE":
			if opts.condition != "" && opts.condition != option {
				return opts, syntaxError()
			}
			opts.condition = option
			if option != "NX" && option != "XX" {
				if i+1 == len(args) {
					return opts, syntaxError()
				}
				i++
				opts.match = args[i].String()
			}
		case "GET":
			opts.get = true
		case "EX", "PX", "EXAT", "PXAT", "KEEPTTL":
			if opts.expiration != "" && opts.expiration != option {
				return opts, syntaxError()
			}
			opts.expiration = option
			if option != "KEEPTTL" {
				if i+1 == len(args) {
					return opts, syntaxError()
				}
				i++
				opts.ttl = args[i].String()
			}
		default:
			return opts, syntaxError()
		}
	}
	return opts, nil
}

func (o setOptions) expiresAt(now int64) (int64, error) {
	if o.expiration == "" || o.expiration == "KEEPTTL" {
		return 0, nil
	}
	// Redis accepts canonical signed decimal integers only (no +, leading zero,
	// negative zero, whitespace, or numeric suffixes).
	ttl, err := strconv.ParseInt(o.ttl, 10, 64)
	if err != nil || strconv.FormatInt(ttl, 10) != o.ttl {
		return 0, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "value is not an integer or out of range")
	}
	invalid := func() (int64, error) {
		return 0, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "invalid expire time in 'set' command")
	}
	if ttl <= 0 {
		return invalid()
	}
	if o.expiration == "EX" || o.expiration == "EXAT" {
		if ttl > math.MaxInt64/1000 {
			return invalid()
		}
		ttl *= 1000
	}
	if o.expiration == "EX" || o.expiration == "PX" {
		// The Redis 8.10.2 reference executable wraps relative TTL addition.
		// A wrapped deadline is in the past and SET removes the key with OK.
		// Keep signed millisecond arithmetic rather than time.Duration, which
		// would also truncate otherwise valid deadlines above 292 years.
		ttl += now
	}
	return ttl, nil
}

func (c *Set) Exec(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(c)
	}
	// Redis validates command arity before disk errors, but disk errors before
	// SET options. Update still rechecks under the database mutation gate.
	if status, ok := c.store.(interface{ WriteError() error }); ok {
		if err := status.WriteError(); err != nil {
			return nil, persistenceCommandError(err)
		}
	}
	opts, err := parseSetOptions(args)
	if err != nil {
		return nil, err
	}
	var reply resp.Value
	var applied, deleted bool
	err = c.store.Update(ctx, args[1].String(), func(current storage.Entry, exists bool, now int64) (storage.Entry, bool, error) {
		expiresAt, err := opts.expiresAt(now)
		if err != nil {
			return current, exists, err
		}
		value, isString := current.Value.(string)
		if opts.get && exists {
			if !isString {
				return current, exists, newWrongTypeError()
			}
			reply = resp.NewBulkString(value)
		}
		if (opts.condition == "NX" && exists) || (!exists && (opts.condition == "XX" || opts.condition == "IFEQ" || opts.condition == "IFDEQ")) {
			return current, exists, storage.ErrUnchanged
		}
		if exists && opts.condition != "" && opts.condition != "NX" && opts.condition != "XX" {
			if !isString {
				return current, exists, newWrongTypeError()
			}
			var match bool
			switch opts.condition {
			case "IFEQ", "IFNE":
				match = value == opts.match
			case "IFDEQ", "IFDNE":
				// Redis validates length only, and only for an existing string.
				if len(opts.match) != 16 {
					return current, exists, NewError(ErrCommand, resp.ERRTYPE_DEFAULT, "must be exactly 16 hexadecimal characters")
				}
				digest := fmt.Sprintf("%016x", xxh3.HashString(value))
				// strcasecmp in Redis terminates at NUL.
				match = foldOption(digest) == foldOption(strings.SplitN(opts.match, "\x00", 2)[0])
			}
			if opts.condition == "IFNE" || opts.condition == "IFDNE" {
				match = !match
			}
			if !match {
				return current, exists, storage.ErrUnchanged
			}
		}
		if !opts.get {
			reply = resp.SimpleString("OK")
		}
		applied = true
		if expiresAt != 0 && expiresAt <= now {
			deleted = true
			return storage.Entry{}, false, nil
		}
		if opts.expiration == "KEEPTTL" && exists {
			expiresAt = current.ExpiresAt
		}
		return storage.Entry{Value: args[2].String(), ExpiresAt: expiresAt}, true, nil
	})
	if err != nil {
		if errors.Is(err, database.ErrSnapshotFailed) || errors.Is(err, database.ErrAOFFailed) {
			return nil, persistenceCommandError(err)
		}
		var commandErr *Error
		if reply != nil && errors.As(err, &commandErr) {
			return resp.ReplySequence{reply, commandErr}, fmt.Errorf("set: %w", err)
		}
		return nil, fmt.Errorf("set: %w", err)
	}
	c.l.DebugContext(ctx, "set completed",
		slog.Bool("applied", applied),
		slog.Bool("expired", deleted),
		slog.String("condition", opts.condition),
		slog.String("expiration", opts.expiration),
		slog.Bool("return_previous", opts.get),
	)
	return reply, nil
}
