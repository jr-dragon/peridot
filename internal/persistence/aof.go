package persistence

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/liberr"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

var (
	ErrAOFClosed = errors.New("aof: closed")
)

type appendFile interface {
	io.Writer
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
}

// AOF manages durable command history independently of the memory backend.
// It does not implement storage.Store. The database coordinates publication and
// supplies a consistent snapshot for rewrite.
type AOF struct {
	// syncMu keeps the descriptor alive across background sync, rewrite, and
	// close. Acquire it before mu; appends only need mu and can proceed during sync.
	syncMu    sync.Mutex
	mu        sync.Mutex
	file      appendFile
	lock      *os.File
	path      string
	failure   error
	closed    bool
	recovered map[string]storage.Entry
	policy    string
	dirty     bool
	stop      chan struct{}
	done      chan struct{}
}

// OpenAOF validates a single-file AOF and prepares its recovered state.
// The parent directory must exist. With no policy, synchronization remains per
// change for existing callers. Server configuration defaults to everysec.
// Symlinks must resolve to an existing target; hardlinked files are rejected.
func OpenAOF(path string, policies ...string) (_ *AOF, err error) {
	policy := "always"
	if len(policies) > 1 {
		return nil, errors.New("aof: expected one sync policy")
	}
	if len(policies) == 1 {
		policy = policies[0]
	}
	if policy != "always" && policy != "everysec" && policy != "no" {
		return nil, fmt.Errorf("aof: invalid sync policy %q", policy)
	}
	path, err = canonicalAOFPath(path)
	if err != nil {
		return nil, err
	}
	lock, err := lockAOF(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("lock AOF: %w", err)
	}
	defer func() {
		if err != nil {
			lock.Close()
		}
	}()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("open AOF: %w", err)
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	if err = validateAOFFile(f); err != nil {
		return nil, err
	}
	state, err := aof.Load(f)
	if err != nil {
		return nil, fmt.Errorf("load AOF: %w", err)
	}
	// Persist the initial directory entry before acknowledging future writes.
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = syncDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	s := &AOF{file: f, lock: lock, path: path, policy: policy, recovered: make(map[string]storage.Entry, len(state))}
	now := time.Now().UnixMilli()
	for key, value := range state {
		if value.HasExpiration && now > value.ExpiresAt {
			continue
		}
		s.recovered[key] = storage.Entry{Value: value.Value, ExpiresAt: value.ExpiresAt}
	}
	if policy == "everysec" {
		s.stop, s.done = make(chan struct{}), make(chan struct{})
		ticker := time.NewTicker(time.Second)
		go s.syncEverySecond(ticker)
	}
	return s, nil
}

func (s *AOF) syncEverySecond(ticker *time.Ticker) {
	defer close(s.done)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			if err := s.syncPending(); err != nil {
				slog.Error("AOF background sync failed", slog.String("component", "persistence"), liberr.Attr(err))
				return
			}
		}
	}
}

func (s *AOF) syncPending() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	if s.closed || s.failure != nil || !s.dirty {
		s.mu.Unlock()
		return nil
	}
	// Writes arriving during Sync leave dirty set for the following tick.
	s.dirty = false
	s.mu.Unlock()
	err := s.file.Sync()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.failure = errors.Join(s.failure, fmt.Errorf("sync AOF: %w", err))
	}
	return err
}

// canonicalAOFPath gives the log, sidecar lock, and rewrite one stable path.
// A missing final file is allowed, but a dangling symlink is not: falling back
// to its unresolved name would give another writer a different sidecar lock.
func canonicalAOFPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(path); err == nil {
		return filepath.EvalSymlinks(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	dir, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, filepath.Base(path)), nil
}

// Restore transfers the validated recovery state to the database once, before use.
func (s *AOF) Restore() map[string]storage.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.recovered
	s.recovered = nil
	return state
}

// Err reports an append or rewrite publication failure that prevents new writes.
func (s *AOF) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrAOFClosed
	}
	return classifyAOFFailure(s.failure)
}

func classifyAOFFailure(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(database.ErrAOFFailed, err)
}

// Append writes a resolved mutation before publication, syncing according to policy.
func (s *AOF) Append(key string, next storage.Entry, keep bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrAOFClosed
	}
	var record []byte
	if keep {
		value, ok := next.Value.(string)
		if !ok {
			return errors.New("aof: unsupported value type")
		}
		record = aof.Replacement(key, value, next.ExpiresAt)
	} else {
		record = aof.Encode("DEL", key)
	}
	return classifyAOFFailure(s.append(record))
}

// AppendDelete uses one RESP command so recovery can never replay a prefix of
// a multi-key DEL as a complete deletion of only some keys.
func (s *AOF) AppendDelete(keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrAOFClosed
	}
	if len(keys) == 0 {
		return nil
	}
	args := append([]string{"DEL"}, keys...)
	return classifyAOFFailure(s.append(aof.Encode(args...)))
}

// append is called only under mu. A partial write or failed sync makes later
// appends unsafe; reads of live entries may continue until persistence closes.
func (s *AOF) append(record []byte) error {
	if s.failure != nil {
		return s.failure
	}
	if n, err := s.file.Write(record); err != nil || n != len(record) {
		if err == nil {
			err = io.ErrShortWrite
		}
		s.failure = fmt.Errorf("append AOF: %w", err)
		return s.failure
	}
	if s.policy == "always" {
		if err := s.file.Sync(); err != nil {
			s.failure = fmt.Errorf("sync AOF: %w", err)
			return s.failure
		}
	} else {
		s.dirty = true
	}
	return nil
}

// Rewrite replaces the AOF using a live snapshot supplied by the database.
// The caller must exclude mutations until replacement completes.
func (s *AOF) Rewrite(ctx context.Context, snapshot map[string]storage.Entry) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrAOFClosed
	}
	if s.failure != nil {
		return s.failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A hardlink may have been added after OpenAOF. Replacing only one name
	// would leave the other name pointing to stale state after the next append.
	if err := validateAOFFile(s.file); err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".peridot-aof-*")
	if err != nil {
		return err
	}
	temporary := f.Name()
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(temporary)
	}()
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := snapshot[key]
		value, ok := entry.Value.(string)
		if !ok {
			return errors.New("aof: unsupported value type")
		}
		if _, err := f.Write(aof.Replacement(key, value, entry.ExpiresAt)); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), s.path); err != nil {
		return err
	}
	old := s.file
	s.file = f
	s.dirty = false
	f = nil
	if err := errors.Join(old.Close(), syncDirectory(dir)); err != nil {
		s.failure = fmt.Errorf("publish AOF rewrite: %w", err)
		return s.failure
	}
	return nil
}

// Close synchronizes and closes the log, then releases its file lock.
func (s *AOF) Close() error {
	// Join the worker only after releasing locks it may still need.
	defer func() {
		if s.done != nil {
			<-s.done
		}
	}()
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.stop != nil {
		close(s.stop)
	}
	return errors.Join(s.failure, s.file.Sync(), s.file.Close(), s.lock.Close())
}
