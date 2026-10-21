package storage

import (
	"context"
	"errors"
	"hash/maphash"
	"log/slog"
	"sync"
	"time"

	"github.com/jeremiah-masters/dlht"
)

// DLHT stores entries in independently locked dlht maps without background
// expiration workers. Each shard serializes writes and read/modify/write
// callbacks, while different shards can run concurrently. The pinned dlht
// version reads bucket keys non-atomically, so reads of each map must also be
// synchronized with its writes. This adapter is not lock-free.
type DLHT struct {
	gate   sync.RWMutex // Excludes multi-key deletion from ordinary access.
	shards [64]dlhtShard
	seed   maphash.Seed
	now    func() time.Time
	l      *slog.Logger
}

type dlhtShard struct {
	mu      sync.RWMutex
	entries *dlht.Map[string, Entry]
}

var _ Store = (*DLHT)(nil)

func NewDLHT() *DLHT {
	d := &DLHT{
		seed: maphash.MakeSeed(),
		now:  time.Now,
		l:    slog.Default().With(slog.String("component", "storage"), slog.String("backend", "dlht")),
	}
	for i := range d.shards {
		d.shards[i].entries = dlht.New[string, Entry](dlht.Options{InitialSize: 1})
	}
	return d
}

func (d *DLHT) shardFor(key string) *dlhtShard {
	return &d.shards[maphash.String(d.seed, key)%uint64(len(d.shards))]
}

func (s *dlhtShard) get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries.Get(key)
}

func (d *DLHT) Get(ctx context.Context, key string) (Entry, bool, error) {
	d.gate.RLock()
	defer d.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	shard := d.shardFor(key)
	entry, exists := shard.get(key)
	if !exists || !expired(entry, d.now().UnixMilli()) {
		return entry, exists, nil
	}
	var deleted bool
	func() {
		shard.mu.Lock()
		defer shard.mu.Unlock()
		// Recheck after acquiring the writer's lock to preserve replacements.
		entry, exists = shard.entries.Get(key)
		if exists && expired(entry, d.now().UnixMilli()) {
			shard.entries.Delete(key)
			entry, exists, deleted = Entry{}, false, true
		}
	}()
	if deleted {
		d.l.DebugContext(ctx, "expired entry deleted", slog.String("operation", "get"))
	}
	return entry, exists, nil
}

func (d *DLHT) Update(ctx context.Context, key string, update UpdateFunc) error {
	d.gate.RLock()
	defer d.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var wasExpired bool
	err := func() error {
		shard := d.shardFor(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		current, exists := shard.entries.Get(key)
		stored := exists
		now := d.now().UnixMilli()
		wasExpired = exists && expired(current, now)
		if wasExpired {
			current, exists = Entry{}, false
		}
		next, keep, err := update(current, exists, now)
		if err != nil {
			if wasExpired {
				shard.entries.Delete(key)
			}
			if errors.Is(err, ErrUnchanged) {
				return nil
			}
			return err
		}
		if !keep || expired(next, now) {
			shard.entries.Delete(key)
		} else if stored {
			shard.entries.Put(key, next)
		} else {
			shard.entries.Insert(key, next)
		}
		return nil
	}()
	if wasExpired {
		d.l.DebugContext(ctx, "expired entry removed", slog.String("operation", "update"))
	}
	return err
}

func (d *DLHT) Load(key string) (Entry, bool) { return d.shardFor(key).get(key) }
func (d *DLHT) Put(key string, entry Entry) {
	shard := d.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if _, updated := shard.entries.Put(key, entry); !updated {
		shard.entries.Insert(key, entry)
	}
}
func (d *DLHT) Delete(key string) {
	shard := d.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.entries.Delete(key)
}
func (d *DLHT) Snapshot() map[string]Entry {
	entries := make(map[string]Entry)
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.RLock()
		shard.entries.Range(func(key string, entry Entry) bool { entries[key] = entry; return true })
		shard.mu.RUnlock()
	}
	return entries
}

var _ Backend = (*DLHT)(nil)

// Read observes expiration and value under the same per-key lock.
func (d *DLHT) Read(ctx context.Context, key string, read ReadFunc) error {
	return d.Update(ctx, key, func(e Entry, exists bool, now int64) (Entry, bool, error) {
		read(e, exists, now)
		return e, exists, ErrUnchanged
	})
}

func (d *DLHT) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	d.gate.Lock()
	defer d.gate.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := d.now().UnixMilli()
	var count int64
	for _, key := range keys {
		if e, exists := d.Load(key); exists {
			d.Delete(key)
			if !expired(e, now) {
				count++
			}
		}
	}
	return count, nil
}
