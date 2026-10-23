package database

import (
	"context"
	"errors"

	"github.com/jr-dragon/peridot/internal/storage"
)

// updateComputed replaces database key stripes with the backend's atomic raw
// computation. The shared database gate spans publication and bookkeeping; the
// bucket lock keeps same-key appends and index changes in mutation order. The
// append completes before returning a value for publication. Persistence that
// requires global order uses Update's separate append-before-publication path.
func (s *DB) updateComputed(ctx context.Context, key string, update storage.UpdateFunc) error {
	computation := computedUpdate{db: s, ctx: ctx, key: key, update: update}
	if backend, ok := s.store.(*storage.Xsync); ok {
		// Concrete dispatch keeps the computation on the stack. Passing its
		// callback through an interface would add allocations to every update.
		backend.Compute(key, computation.apply)
		return computation.err
	}
	return s.updateComputedBackend(computation)
}

func (s *DB) updateComputedBackend(computation computedUpdate) error {
	s.store.Compute(computation.key, computation.apply)
	return computation.err
}

type computedUpdate struct {
	db     *DB
	ctx    context.Context
	key    string
	update storage.UpdateFunc
	err    error
}

func (c *computedUpdate) apply(current storage.Entry, exists bool) (storage.Entry, storage.ComputeOp) {
	if c.db.persistence != nil {
		if c.err = c.db.persistence.Err(); c.err != nil {
			return current, storage.ComputeCancel
		}
	}
	if c.err = c.ctx.Err(); c.err != nil {
		return current, storage.ComputeCancel
	}
	now := c.db.now().UnixMilli()
	op := storage.ComputeCancel
	if exists && expired(current, now) {
		if c.err = c.db.expireComputed(c.key); c.err != nil {
			return current, storage.ComputeCancel
		}
		current, exists, op = storage.Entry{}, false, storage.ComputeDelete
		if c.db.persistence != nil {
			if c.err = c.db.persistence.Err(); c.err != nil {
				return current, op
			}
		}
	}
	next, keep, err := c.update(current, exists, now)
	if errors.Is(err, storage.ErrUnchanged) {
		return current, op
	}
	if err != nil {
		c.err = err
		return current, op
	}
	keep = keep && !expired(next, now)
	if !keep && !exists {
		return current, op
	}
	if c.err = c.db.append(c.key, next, keep); c.err != nil {
		return current, op
	}
	c.db.dirty.Add(1)
	if keep {
		if current.ExpiresAt != 0 || next.ExpiresAt != 0 {
			c.db.trackExpiration(c.key, next)
		}
		return next, storage.ComputeUpdate
	}
	c.db.trackExpiration(c.key, storage.Entry{})
	return storage.Entry{}, storage.ComputeDelete
}

// readComputed rechecks an expired observation under the same bucket lock as
// non-AOF updates. Callbacks must not reenter the raw backend to delete the key.
func (s *DB) readComputed(ctx context.Context, key string, read storage.ReadFunc) error {
	computation := computedRead{db: s, ctx: ctx, key: key, read: read}
	if backend, ok := s.store.(*storage.Xsync); ok {
		backend.Compute(key, computation.apply)
		return computation.err
	}
	return s.readComputedBackend(computation)
}

func (s *DB) readComputedBackend(computation computedRead) error {
	s.store.Compute(computation.key, computation.apply)
	return computation.err
}

type computedRead struct {
	db   *DB
	ctx  context.Context
	key  string
	read storage.ReadFunc
	err  error
}

func (c *computedRead) apply(entry storage.Entry, exists bool) (storage.Entry, storage.ComputeOp) {
	if c.err = c.ctx.Err(); c.err != nil {
		return entry, storage.ComputeCancel
	}
	now := c.db.now().UnixMilli()
	op := storage.ComputeCancel
	if exists && expired(entry, now) {
		if c.err = c.db.expireComputed(c.key); c.err != nil {
			return entry, storage.ComputeCancel
		}
		entry, exists, op = storage.Entry{}, false, storage.ComputeDelete
	}
	c.read(entry, exists, now)
	return entry, op
}
