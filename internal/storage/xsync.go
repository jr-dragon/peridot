package storage

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// Xsync stores entries in an xsync map without background expiration workers.
type Xsync struct {
	gate    sync.RWMutex // Excludes multi-key deletion from ordinary access.
	entries *xsync.Map[string, Entry]
	now     func() time.Time
	l       *slog.Logger
}

var _ Store = (*Xsync)(nil)

func NewXsync() *Xsync {
	return &Xsync{
		entries: xsync.NewMap[string, Entry](),
		now:     time.Now,
		l:       slog.Default().With(slog.String("component", "storage"), slog.String("backend", "xsync")),
	}
}

func (m *Xsync) Get(ctx context.Context, key string) (Entry, bool, error) {
	m.gate.RLock()
	defer m.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	entry, ok := m.entries.Load(key)
	if !ok || !expired(entry, m.now().UnixMilli()) {
		return entry, ok, nil
	}
	// Recheck under the bucket lock: a concurrent writer may have replaced the
	// expired entry since Load. Never delete that writer's live replacement.
	var deleted bool
	entry, ok = m.entries.Compute(key, func(current Entry, exists bool) (Entry, xsync.ComputeOp) {
		if exists && expired(current, m.now().UnixMilli()) {
			deleted = true
			return Entry{}, xsync.DeleteOp
		}
		return current, xsync.CancelOp
	})
	if deleted {
		m.l.DebugContext(ctx, "expired entry deleted", slog.String("operation", "get"))
	}
	return entry, ok, nil
}

func (m *Xsync) Update(ctx context.Context, key string, update UpdateFunc) error {
	m.gate.RLock()
	defer m.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var updateErr error
	var wasExpired bool
	m.entries.Compute(key, func(current Entry, exists bool) (Entry, xsync.ComputeOp) {
		if updateErr = ctx.Err(); updateErr != nil {
			return current, xsync.CancelOp
		}
		now := m.now().UnixMilli()
		wasExpired = exists && expired(current, now)
		if wasExpired {
			current, exists = Entry{}, false
		}
		next, keep, err := update(current, exists, now)
		updateErr = err
		if err != nil {
			if errors.Is(err, ErrUnchanged) {
				updateErr = nil
			}
			if wasExpired {
				return Entry{}, xsync.DeleteOp
			}
			return current, xsync.CancelOp
		}
		if !keep || expired(next, now) {
			return Entry{}, xsync.DeleteOp
		}
		return next, xsync.UpdateOp
	})
	if wasExpired {
		m.l.DebugContext(ctx, "expired entry removed", slog.String("operation", "update"))
	}
	return updateErr
}

func (m *Xsync) Load(key string) (Entry, bool) { return m.entries.Load(key) }
func (m *Xsync) Put(key string, entry Entry)   { m.entries.Store(key, entry) }
func (m *Xsync) Delete(key string)             { m.entries.Delete(key) }
func (m *Xsync) Snapshot() map[string]Entry {
	entries := make(map[string]Entry)
	m.entries.Range(func(key string, entry Entry) bool { entries[key] = entry; return true })
	return entries
}

var _ Backend = (*Xsync)(nil)

// Read observes expiration and value under the same per-key lock.
func (m *Xsync) Read(ctx context.Context, key string, read ReadFunc) error {
	return m.Update(ctx, key, func(e Entry, exists bool, now int64) (Entry, bool, error) {
		read(e, exists, now)
		return e, exists, ErrUnchanged
	})
}

func (m *Xsync) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := m.now().UnixMilli()
	var count int64
	for _, key := range keys {
		if e, exists := m.Load(key); exists {
			m.Delete(key)
			if !expired(e, now) {
				count++
			}
		}
	}
	return count, nil
}
