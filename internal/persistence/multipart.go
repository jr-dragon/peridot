package persistence

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/persistence/rdb"
	"github.com/jr-dragon/peridot/internal/storage"
)

// MultipartAOF owns a manifest and an append descriptor. Rewrite rotates the
// incremental log at the database snapshot boundary, then writes the base while
// ordinary appends continue. Its embedded AOF retains the existing sync policies.
type MultipartAOF struct {
	*AOF
	manifestPath string
	name         string
	manifest     manifest
	preamble     bool
	rewriting    bool
}

// OpenMultipartAOF loads base + incremental files, or imports a legacy AOF when
// no manifest exists. A missing referenced segment is always a startup failure.
func OpenMultipartAOF(dir, name, legacy, policy string, preamble bool) (_ *MultipartAOF, err error) {
	if !validFilename(name) {
		return nil, errors.New("aof: invalid filename")
	}
	if policy != "always" && policy != "everysec" && policy != "no" {
		return nil, errors.New("aof: invalid sync policy")
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = syncDirectory(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name+".manifest")
	lock, err := lockAOF(path + ".lock")
	if err != nil {
		return nil, err
	}
	s := &MultipartAOF{AOF: &AOF{lock: lock, policy: policy}, manifestPath: path, name: name, preamble: preamble}
	defer func() {
		if err != nil {
			if s.file != nil {
				s.file.Close()
			}
			lock.Close()
		}
	}()
	state := make(map[string]aof.String)
	f, openErr := os.Open(path)
	if openErr == nil {
		s.manifest, err = readManifest(f)
		f.Close()
		if err != nil {
			return nil, err
		}
		if s.manifest.Base != nil {
			if err = loadSegment(filepath.Join(dir, s.manifest.Base.Name), state, true, false); err != nil {
				return nil, fmt.Errorf("load AOF base: %w", err)
			}
		}
		for _, part := range s.manifest.Incr {
			if err = loadSegment(filepath.Join(dir, part.Name), state, false, false); err != nil {
				return nil, fmt.Errorf("load AOF increment %s: %w", part.Name, err)
			}
		}
	} else if !errors.Is(openErr, os.ErrNotExist) {
		return nil, openErr
	} else {
		if legacy != "" {
			if _, statErr := os.Stat(legacy); statErr == nil {
				legacyLock, lockErr := lockAOF(legacy + ".lock")
				if lockErr != nil {
					return nil, lockErr
				}
				defer legacyLock.Close()
				if err = loadSegment(legacy, state, true, false); err != nil {
					return nil, fmt.Errorf("import legacy AOF: %w", err)
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return nil, statErr
			}
		}
		base, err := s.nextSegment("b")
		if err != nil {
			return nil, err
		}
		if err = s.writeBase(context.Background(), base, recoveredEntries(state)); err != nil {
			return nil, err
		}
		s.manifest.Base = &base
	}
	// A new increment is created only after every existing segment validates.
	// Never silently create a missing referenced file with O_CREATE.
	if len(s.manifest.Incr) > 0 {
		s.path = filepath.Join(dir, s.manifest.Incr[len(s.manifest.Incr)-1].Name)
		s.file, err = os.OpenFile(s.path, os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		if err = validateAOFFile(s.file); err != nil {
			return nil, err
		}
	} else {
		if err = s.rotate(); err != nil {
			return nil, err
		}
	}
	s.recovered = recoveredEntries(state)
	if policy == "everysec" {
		s.stop, s.done = make(chan struct{}), make(chan struct{})
		go s.syncEverySecond(time.NewTicker(time.Second))
	}
	return s, nil
}

func (s *MultipartAOF) nextSegment(kind string) (segment, error) {
	var seq int64
	for _, part := range s.manifest.files() {
		seq = max(seq, part.Seq)
	}
	for {
		if seq == 1<<63-1 {
			return segment{}, errors.New("aof: sequence exhausted")
		}
		seq++
		suffix := "incr.aof"
		if kind == "b" {
			suffix = "base.aof"
			if s.preamble {
				suffix = "base.rdb"
			}
		}
		part := segment{fmt.Sprintf("%s.%d.%s", s.name, seq, suffix), seq, kind}
		if _, err := os.Lstat(filepath.Join(filepath.Dir(s.manifestPath), part.Name)); errors.Is(err, os.ErrNotExist) {
			return part, nil
		} else if err != nil {
			return segment{}, err
		}
	}
}

// publish only changes the in-memory authority after rename. If directory sync
// fails after rename, poison writes and retain every possibly referenced file.
func (s *MultipartAOF) publish(next manifest) error {
	published, err := atomicFile(s.manifestPath, next.write)
	if published {
		s.manifest = next
		if err != nil {
			s.failure = fmt.Errorf("publish AOF manifest: %w", err)
		}
	}
	return err
}

// rotate is called while writes and background sync are excluded, or at startup.
func (s *MultipartAOF) rotate() error {
	if s.file != nil {
		if err := s.file.Sync(); err != nil {
			s.failure = err
			return err
		}
	}
	part, err := s.nextSegment("i")
	if err != nil {
		return err
	}
	path := filepath.Join(filepath.Dir(s.manifestPath), part.Name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if err = errors.Join(f.Sync(), syncDirectory(filepath.Dir(path))); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	next := s.manifest
	next.Incr = append(slices.Clone(next.Incr), part)
	if err = s.publish(next); err != nil {
		f.Close()
		return err
	}
	old := s.file
	s.file, s.path, s.dirty = f, path, false
	if old != nil {
		if err := old.Close(); err != nil {
			s.failure = err
			return err
		}
	}
	return nil
}

func (s *MultipartAOF) writeBase(ctx context.Context, part segment, entries map[string]storage.Entry) error {
	_, err := atomicFile(filepath.Join(filepath.Dir(s.manifestPath), part.Name), func(w io.Writer) error {
		if s.preamble {
			return rdb.Write(ctx, w, entries)
		}
		keys := make([]string, 0, len(entries))
		for k := range entries {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry := entries[k]
			v, ok := entry.Value.(string)
			if !ok {
				return errors.New("aof: unsupported value type")
			}
			if _, err := w.Write(aof.Replacement(k, v, entry.ExpiresAt)); err != nil {
				return err
			}
		}
		return ctx.Err()
	})
	return err
}

// BeginRewrite must run under the database mutation gate with a consistent
// snapshot. Its returned function must run exactly once outside that gate.
func (s *MultipartAOF) BeginRewrite(ctx context.Context, entries map[string]storage.Entry) (func() error, error) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waitAppendLocked()
	if s.closed {
		return nil, ErrAOFClosed
	}
	if s.failure != nil {
		return nil, s.failure
	}
	if s.rewriting {
		return nil, database.ErrRewriteInProgress
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	old := s.manifest
	if err := s.rotate(); err != nil {
		return nil, err
	}
	base, err := s.nextSegment("b")
	if err != nil {
		return nil, err
	}
	increment := s.manifest.Incr[len(s.manifest.Incr)-1]
	s.rewriting = true
	// Captured strings are immutable; the snapshot map belongs to this job.
	var once sync.Once
	var result error
	finish := func() error {
		once.Do(func() {
			result = s.writeBase(ctx, base, entries)
			s.syncMu.Lock()
			defer s.syncMu.Unlock()
			s.mu.Lock()
			defer s.mu.Unlock()
			defer func() { s.rewriting = false }()
			if result != nil {
				return
			}
			if s.closed {
				result = ErrAOFClosed
				return
			}
			if s.failure != nil {
				result = s.failure
				return
			}
			if result = ctx.Err(); result != nil {
				return
			}
			if result = s.file.Sync(); result != nil {
				s.failure = result
				return
			}
			next := manifest{Base: &base, Incr: []segment{increment}}
			if result = s.publish(next); result != nil {
				return
			}
			// Cleanup follows durable publication. Orphans after a crash are ignored.
			for _, part := range old.files() {
				_ = os.Remove(filepath.Join(filepath.Dir(s.manifestPath), part.Name))
			}
		})
		return result
	}
	return finish, nil
}
func (s *MultipartAOF) Rewrite(ctx context.Context, entries map[string]storage.Entry) error {
	finish, err := s.BeginRewrite(ctx, entries)
	if err != nil {
		return err
	}
	return finish()
}
