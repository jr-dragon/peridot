// Package database coordinates memory operations and optional persistence.
package database

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/liberr"
	"github.com/jr-dragon/peridot/internal/storage"
)

var (
	ErrAOFFailed           = errors.New("database: AOF persistence failed")
	ErrSnapshotFailed      = errors.New("database: snapshot persistence failed")
	ErrSaveScheduled       = errors.New("database: snapshot scheduled after rewrite")
	ErrSaveInProgress      = errors.New("database: snapshot already in progress")
	ErrRewriteScheduled    = errors.New("database: rewrite scheduled after snapshot")
	ErrClosed              = errors.New("database: closed")
	ErrRewriteInProgress   = errors.New("database: rewrite already in progress")
	ErrPersistenceDisabled = errors.New("database: persistence is disabled")
)

// Persistence owns durable state, never the working memory backend. Restore
// transfers its validated startup state once. Append writes before returning and
// syncs according to policy; Err reports failures that make later appends unsafe.
type Persistence interface {
	Restore() map[string]storage.Entry
	Append(string, storage.Entry, bool) error
	// AppendDelete persists one indivisible multi-key deletion record.
	AppendDelete([]string) error
	Err() error
	Rewrite(context.Context, map[string]storage.Entry) error
	Close() error
}

// snapshotWriter is optional so existing single-file persistence remains usable.
type snapshotWriter interface {
	// background distinguishes BGSAVE failures from foreground SAVE failures.
	Save(ctx context.Context, state map[string]storage.Entry, background bool) error
}

type concurrentRewriter interface {
	BeginRewrite(context.Context, map[string]storage.Entry) (func() error, error)
}

// DB exclusively owns its selected memory backend. When persistence is enabled,
// its gate orders expiration, append, publication, rewrite, and close.
type DB struct {
	mu             sync.RWMutex
	store          storage.Backend
	persistence    Persistence
	now            func() time.Time
	closed         bool
	jobsMu         sync.Mutex
	closing        bool
	rewriting      bool
	jobs           sync.WaitGroup
	saving         bool
	rewritePending bool
	savePending    bool
	dirty          uint64
	lastSave       time.Time
	rules          []data.SaveRule
	stop           chan struct{}
	stopped        chan struct{}
	expiration     *activeExpiration
}

// Option configures a database before recovery or concurrent access.
type Option func(*DB)

// WithClock supplies the clock used for durable expiration and snapshots.
func WithClock(now func() time.Time) Option { return func(d *DB) { d.now = now } }

func New(store storage.Backend, persistence Persistence, options ...Option) *DB {
	d := &DB{store: store, persistence: persistence, now: time.Now}
	for _, option := range options {
		option(d)
	}
	if persistence != nil {
		now := d.now().UnixMilli()
		for key, entry := range persistence.Restore() {
			if !expired(entry, now) {
				store.Put(key, entry)
			}
		}
	}
	d.lastSave = d.now()
	if snapshots, ok := persistence.(interface{ SnapshotRules() []data.SaveRule }); ok {
		d.rules = snapshots.SnapshotRules()
		if len(d.rules) > 0 {
			d.stop, d.stopped = make(chan struct{}), make(chan struct{})
			go d.snapshotLoop()
		}
	}
	if d.expiration != nil {
		for key, entry := range store.Snapshot() {
			d.trackExpiration(key, entry)
		}
		go d.expirationLoop()
	}
	return d
}

func (s *DB) lockAccess() func() {
	if s.persistence == nil && s.expiration == nil {
		s.mu.RLock()
		return s.mu.RUnlock
	}
	s.mu.Lock()
	return s.mu.Unlock
}

func expired(entry storage.Entry, now int64) bool {
	return entry.ExpiresAt != 0 && now > entry.ExpiresAt
}

// WriteError reports persistence failures to commands such as PING without
// mutating data. Updates check the same status under their mutation gate.
func (s *DB) WriteError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	if s.persistence != nil {
		return s.persistence.Err()
	}
	return nil
}

func (s *DB) Get(ctx context.Context, key string) (storage.Entry, bool, error) {
	unlock := s.lockAccess()
	defer unlock()
	if s.closed {
		return storage.Entry{}, false, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return storage.Entry{}, false, err
	}
	if s.persistence == nil && s.expiration == nil {
		return s.store.Get(ctx, key)
	}
	entry, exists := s.store.Load(key)
	if exists && expired(entry, s.now().UnixMilli()) {
		if err := s.expireKey(key); err != nil {
			return storage.Entry{}, false, err
		}
		return storage.Entry{}, false, nil
	}
	return entry, exists, nil
}

func (s *DB) Update(ctx context.Context, key string, update storage.UpdateFunc) error {
	unlock := s.lockAccess()
	defer unlock()
	if s.closed {
		return ErrClosed
	}
	if s.persistence == nil && s.expiration == nil {
		return s.store.Update(ctx, key, update)
	}
	if s.persistence != nil {
		if err := s.persistence.Err(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, exists := s.store.Load(key)
	now := s.now().UnixMilli()
	if exists && expired(current, now) {
		if err := s.expireKey(key); err != nil {
			return err
		}
		// Expiration may discover a new AOF failure. Reject the user mutation
		// even though the expired key is now absent.
		if s.persistence != nil {
			if err := s.persistence.Err(); err != nil {
				return err
			}
		}
		current, exists = storage.Entry{}, false
	}
	next, keep, err := update(current, exists, now)
	if errors.Is(err, storage.ErrUnchanged) {
		return nil
	}
	if err != nil {
		return err
	}
	keep = keep && !expired(next, now)
	if !keep && !exists {
		return nil
	}
	if err := s.append(key, next, keep); err != nil {
		return err
	}

	s.dirty++
	if keep {
		s.store.Put(key, next)
		s.trackExpiration(key, next)
	} else {
		s.delete(key)
	}
	return nil
}

// snapshotLocked captures a point-in-time image under the mutation gate.
func (s *DB) snapshotLocked() map[string]storage.Entry {
	entries := s.store.Snapshot()
	now := s.now().UnixMilli()
	for key, entry := range entries {
		if expired(entry, now) {
			delete(entries, key)
		}
	}
	return entries
}

// Rewrite waits for one checkpoint, preserving the legacy blocking fallback.
func (s *DB) Rewrite(ctx context.Context) error {
	if err := s.reserve(false); err != nil {
		return err
	}
	defer s.finish(false)
	return s.rewrite(ctx)
}
func (s *DB) rewrite(ctx context.Context) error {
	finish, err := s.prepareRewrite(ctx)
	if err != nil {
		return err
	}
	return finish()
}

// prepareRewrite captures the boundary before acknowledging BGREWRITEAOF. This
// also matters with AOF disabled, when later writes have no incremental log.
func (s *DB) prepareRewrite(ctx context.Context) (func() error, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	// RDB write protection does not block an independent AOF checkpoint.
	// AOF failures still prevent capture/rotation of an unsafe incremental log.
	if err := s.persistence.Err(); err != nil && !errors.Is(err, ErrSnapshotFailed) {
		s.mu.Unlock()
		return nil, err
	}
	entries := s.snapshotLocked()
	if checkpoint, ok := s.persistence.(concurrentRewriter); ok {
		finish, err := checkpoint.BeginRewrite(ctx, entries)
		s.mu.Unlock()
		return finish, err
	}
	return func() error {
		defer s.mu.Unlock()
		return s.persistence.Rewrite(ctx, entries)
	}, nil
}

func (s *DB) reserve(save bool, foreground ...bool) error {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	if s.closing {
		return ErrClosed
	}
	if s.persistence == nil {
		return ErrPersistenceDisabled
	}
	if save {
		if _, ok := s.persistence.(snapshotWriter); !ok {
			return ErrPersistenceDisabled
		}
		if s.saving {
			return ErrSaveInProgress
		}
		if s.rewriting && !(len(foreground) > 0 && foreground[0]) {
			return ErrRewriteInProgress
		}
		s.saving = true
	} else {
		if s.rewriting {
			return ErrRewriteInProgress
		}
		if s.saving {
			return ErrSaveInProgress
		}
		s.rewriting = true
	}
	s.jobs.Add(1)
	return nil
}
func (s *DB) finish(save bool) {
	s.jobsMu.Lock()
	if save {
		s.saving = false
	} else {
		s.rewriting = false
	}
	if s.rewritePending && !s.closing && !s.saving && !s.rewriting {
		s.rewritePending = false
		s.rewriting = true
		s.jobs.Add(1)
		go s.backgroundRewrite()
	} else if s.savePending && !s.closing && !s.saving && !s.rewriting {
		s.savePending = false
		s.saving = true
		s.jobs.Add(1)
		go func() {
			defer s.finish(true)
			if err := s.save(context.Background(), false); err != nil {
				slog.Error("RDB snapshot failed", slog.String("component", "persistence"), liberr.Attr(err))
			}
		}()
	}
	s.jobsMu.Unlock()
	s.jobs.Done()
}
func (s *DB) backgroundRewrite() {
	defer s.finish(false)
	if err := s.rewrite(context.Background()); err != nil {
		slog.Error("AOF rewrite failed", slog.String("component", "persistence"), liberr.Attr(err))
	}
}

// StartRewrite schedules a rewrite after an active snapshot, as Redis does.
func (s *DB) StartRewrite() error {
	err := s.reserve(false)
	if errors.Is(err, ErrSaveInProgress) {
		s.jobsMu.Lock()
		if s.saving && !s.closing {
			s.rewritePending = true
			s.jobsMu.Unlock()
			return ErrRewriteScheduled
		}
		s.jobsMu.Unlock()
		return s.StartRewrite()
	}
	if err != nil {
		return err
	}
	// Legacy persistence has no concurrent checkpoint boundary; preserve its
	// asynchronous, blocking rewrite path.
	if _, ok := s.persistence.(concurrentRewriter); !ok {
		go s.backgroundRewrite()
		return nil
	}
	finish, err := s.prepareRewrite(context.Background())
	if err != nil {
		s.finish(false)
		return err
	}
	go func() {
		defer s.finish(false)
		if err := finish(); err != nil {
			slog.Error("AOF rewrite failed", slog.String("component", "persistence"), liberr.Attr(err))
		}
	}()
	return nil
}

// Save completes a foreground snapshot. Snapshot writes use immutable entries.
func (s *DB) Save(ctx context.Context) error {
	if err := s.reserve(true, true); err != nil {
		return err
	}
	defer s.finish(true)
	return s.save(ctx, true)
}
func (s *DB) save(ctx context.Context, foreground bool) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	entries, dirty := s.snapshotLocked(), s.dirty
	if !foreground {
		s.mu.Unlock()
	}
	err := s.persistence.(snapshotWriter).Save(ctx, entries, !foreground)
	if !foreground {
		s.mu.Lock()
	}
	if err == nil {
		s.dirty -= dirty
		s.lastSave = s.now()
	}
	s.mu.Unlock()
	return err
}
func (s *DB) StartSave(schedule ...bool) error {
	err := s.reserve(true)
	if errors.Is(err, ErrRewriteInProgress) && len(schedule) > 0 && schedule[0] {
		s.jobsMu.Lock()
		if s.rewriting && !s.closing {
			s.savePending = true
			s.jobsMu.Unlock()
			return ErrSaveScheduled
		}
		s.jobsMu.Unlock()
		return s.StartSave(schedule...)
	}
	if err != nil {
		return err
	}
	// Capture before returning the BGSAVE reply; later writes belong to the next snapshot.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.finish(true)
		return ErrClosed
	}
	entries, dirty := s.snapshotLocked(), s.dirty
	s.mu.Unlock()
	go func() {
		defer s.finish(true)
		err := s.persistence.(snapshotWriter).Save(context.Background(), entries, true)
		s.mu.Lock()
		if err == nil {
			s.dirty -= dirty
			s.lastSave = s.now()
		}
		s.mu.Unlock()
		if err != nil {
			slog.Error("RDB snapshot failed", slog.String("component", "persistence"), liberr.Attr(err))
		}
	}()
	return nil
}
func (s *DB) LastSave() int64 { s.mu.RLock(); defer s.mu.RUnlock(); return s.lastSave.Unix() }
func (s *DB) snapshotLoop() {
	defer close(s.stopped)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.RLock()
			due := false
			for _, rule := range s.rules {
				if s.dirty >= rule.Changes && s.now().Unix()-s.lastSave.Unix() >= rule.Seconds {
					due = true
					break
				}
			}
			s.mu.RUnlock()
			if due {
				_ = s.StartSave()
			}
		}
	}
}

// Close waits for checkpoints and saves a final RDB when save rules are enabled.
func (s *DB) Close() error {
	s.jobsMu.Lock()
	if !s.closing {
		s.closing = true
		if s.stop != nil {
			close(s.stop)
		}
		if s.expiration != nil {
			close(s.expiration.stop)
		}
	}
	s.jobsMu.Unlock()
	if s.expiration != nil {
		<-s.expiration.done
	}
	if s.stopped != nil {
		<-s.stopped
	}
	s.jobs.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.persistence != nil {
		if len(s.rules) > 0 && s.dirty > 0 {
			err = s.persistence.(snapshotWriter).Save(context.Background(), s.snapshotLocked(), false)
		}
		err = errors.Join(err, s.persistence.Close())
	}
	return err
}

// Read shares Get's read-only persistence policy and supplies an expiration
// snapshot, without treating a TTL lookup as a write or increasing dirty state.
func (s *DB) Read(ctx context.Context, key string, read storage.ReadFunc) error {
	unlock := s.lockAccess()
	defer unlock()
	if s.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.persistence == nil && s.expiration == nil {
		return s.store.Read(ctx, key, read)
	}
	now := s.now().UnixMilli()
	entry, exists := s.store.Load(key)
	if exists && expired(entry, now) {
		if err := s.expireKey(key); err != nil {
			return err
		}
		entry, exists = storage.Entry{}, false
	}
	read(entry, exists, now)
	return nil
}

// DeleteKeys holds the database gate across the entire command, including AOF
// appends, so no other command or snapshot observes an intermediate deletion.
func (s *DB) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.persistence == nil && s.expiration == nil {
		return s.store.DeleteKeys(ctx, keys)
	}
	if s.persistence != nil {
		if err := s.persistence.Err(); err != nil {
			return 0, err
		}
	}
	now := s.now().UnixMilli()
	var count int64
	deleted := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		entry, exists := s.store.Load(key)
		if !exists {
			continue
		}
		deleted = append(deleted, key)
		if !expired(entry, now) {
			count++
		}
	}
	if len(deleted) > 0 && s.persistence != nil {
		if err := s.persistence.AppendDelete(deleted); err != nil {
			return 0, err
		}
	}
	for _, key := range deleted {
		s.delete(key)
		s.dirty++
	}
	return count, nil
}
