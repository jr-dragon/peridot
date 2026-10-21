package persistence

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

// Engine combines independent RDB snapshots with optional multipart AOF.
// When AOF is enabled it is the sole recovery authority, even when RDB exists.
type Engine struct {
	aof          *MultipartAOF
	aofDirectory string
	aofFilename  string
	aofPreamble  bool
	recovered    map[string]storage.Entry
	snapshotPath string
	lock         *os.File
	rules        []data.SaveRule
	stopWrites   bool
	mu           sync.Mutex
	saveFailure  error
	closed       bool
}

// New opens persistence before the server starts accepting requests. A zero
// Config retains the explicitly unconfigured memory-only embedding behavior.
func New(config data.Config) (_ database.Persistence, err error) {
	if !config.AppendOnly && config.DBFilename == "" {
		return nil, nil
	}
	defaults := data.DefaultConfig()
	if config.AppendDirname == "" {
		config.AppendDirname = defaults.AppendDirname
	}
	if config.AppendFilename == "" {
		config.AppendFilename = defaults.AppendFilename
	}
	if !validFilename(config.AppendDirname) || !validFilename(config.AppendFilename) {
		return nil, errors.New("aof: invalid directory or filename")
	}
	e := &Engine{
		rules: slices.Clone(config.Save), stopWrites: config.StopWritesOnBGSaveError,
		aofDirectory: filepath.Join(config.Dir, config.AppendDirname),
		aofFilename:  config.AppendFilename, aofPreamble: config.AOFUseRDBPreamble,
	}
	defer func() {
		if err != nil {
			e.Close()
		}
	}()
	if config.DBFilename != "" {
		if !validFilename(config.DBFilename) {
			return nil, errors.New("rdb: invalid filename")
		}
		e.snapshotPath, err = canonicalAOFPath(filepath.Join(config.Dir, config.DBFilename))
		if err != nil {
			return nil, err
		}
		e.lock, err = lockAOF(e.snapshotPath + ".lock")
		if err != nil {
			return nil, err
		}
	}
	if config.AppendOnly {
		if config.AppendFsync == "" {
			config.AppendFsync = defaults.AppendFsync
		}
		e.aof, err = OpenMultipartAOF(filepath.Join(config.Dir, config.AppendDirname), config.AppendFilename, config.AOFPath(), config.AppendFsync, config.AOFUseRDBPreamble)
		if err != nil {
			return nil, err
		}
		e.recovered = e.aof.Restore()
	} else {
		state := make(map[string]aof.String)
		err = loadSegment(e.snapshotPath, state, true, true)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if err != nil {
			return nil, fmt.Errorf("load RDB: %w", err)
		}
		e.recovered = recoveredEntries(state)
	}
	return e, nil
}
func (e *Engine) Restore() map[string]storage.Entry {
	state := e.recovered
	e.recovered = nil
	return state
}
func (e *Engine) SnapshotRules() []data.SaveRule { return slices.Clone(e.rules) }
func (e *Engine) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return database.ErrClosed
	}
	if e.aof != nil {
		if err := e.aof.Err(); err != nil {
			return err
		}
	}
	if e.stopWrites && len(e.rules) > 0 && e.saveFailure != nil {
		return errors.Join(database.ErrSnapshotFailed, e.saveFailure)
	}
	return nil
}
func (e *Engine) Append(key string, next storage.Entry, keep bool) error {
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return database.ErrClosed
	}
	// The database checks Err before user mutations. Expiration remains allowed
	// after a snapshot failure, and still appends DEL when AOF is active.
	if e.aof != nil {
		return e.aof.Append(key, next, keep)
	}
	return nil
}
func (e *Engine) Rewrite(ctx context.Context, state map[string]storage.Entry) error {
	finish, err := e.BeginRewrite(ctx, state)
	if err != nil {
		return err
	}
	return finish()
}

func (e *Engine) AppendDelete(keys []string) error {
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return database.ErrClosed
	}
	if e.aof != nil {
		return e.aof.AppendDelete(keys)
	}
	return nil
}
func (e *Engine) BeginRewrite(ctx context.Context, state map[string]storage.Entry) (func() error, error) {
	if e.aof == nil {
		return prepareSnapshotAOF(ctx, e.aofDirectory, e.aofFilename, e.aofPreamble, state)
	}
	return e.aof.BeginRewrite(ctx, state)
}
func (e *Engine) Save(ctx context.Context, state map[string]storage.Entry, background bool) error {
	if e.snapshotPath == "" {
		return database.ErrPersistenceDisabled
	}
	err := writeRDB(ctx, e.snapshotPath, state)
	e.mu.Lock()
	// A failed foreground SAVE leaves the last BGSAVE status unchanged.
	// Any successful snapshot clears write protection, matching Redis 8.10.2.
	if background || err == nil {
		e.saveFailure = err
	}
	e.mu.Unlock()
	return err
}
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	var err error
	if e.aof != nil {
		err = e.aof.Close()
	}
	if e.lock != nil {
		err = errors.Join(err, e.lock.Close())
	}
	return err
}
