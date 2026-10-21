package database

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/storage"
)

type checkpointPersistence struct {
	mu          sync.Mutex
	snapshots   []map[string]storage.Entry
	rules       []data.SaveRule
	entered     chan struct{}
	release     chan struct{}
	fail        error
	writeErr    error
	backgrounds []bool
	rewrites    int
}

func (*checkpointPersistence) Restore() map[string]storage.Entry        { return nil }
func (*checkpointPersistence) Append(string, storage.Entry, bool) error { return nil }
func (*checkpointPersistence) AppendDelete([]string) error              { return nil }
func (p *checkpointPersistence) Err() error                             { return p.writeErr }
func (*checkpointPersistence) Close() error                             { return nil }
func (p *checkpointPersistence) SnapshotRules() []data.SaveRule         { return p.rules }
func (p *checkpointPersistence) Rewrite(context.Context, map[string]storage.Entry) error {
	p.mu.Lock()
	p.rewrites++
	p.mu.Unlock()
	return nil
}
func (p *checkpointPersistence) Save(_ context.Context, snapshot map[string]storage.Entry, background bool) error {
	if p.entered != nil {
		p.entered <- struct{}{}
		<-p.release
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots = append(p.snapshots, snapshot)
	p.backgrounds = append(p.backgrounds, background)
	return p.fail
}
func put(t *testing.T, db *DB, key, value string) {
	t.Helper()
	if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
		return storage.Entry{Value: value}, true, nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotDoesNotBlockWritesAndCloseWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &checkpointPersistence{entered: make(chan struct{}, 1), release: make(chan struct{})}
		db := New(storage.New(), p)
		put(t, db, "key", "before")
		if err := db.StartSave(); err != nil {
			t.Fatal(err)
		}
		<-p.entered
		if err := db.StartSave(); !errors.Is(err, ErrSaveInProgress) {
			t.Fatal(err)
		}
		put(t, db, "key", "after")
		put(t, db, "tail", "value")
		closed := make(chan error, 1)
		go func() { closed <- db.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatal("closed before snapshot completed", err)
		default:
		}
		close(p.release)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if len(p.snapshots) != 1 || len(p.snapshots[0]) != 1 || p.snapshots[0]["key"].Value != "before" {
			t.Fatal(p.snapshots)
		}
	})
}
func TestRewriteScheduledAfterSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &checkpointPersistence{entered: make(chan struct{}, 1), release: make(chan struct{})}
		db := New(storage.New(), p)
		if err := db.StartSave(); err != nil {
			t.Fatal(err)
		}
		<-p.entered
		if err := db.StartRewrite(); !errors.Is(err, ErrRewriteScheduled) {
			t.Fatal(err)
		}
		close(p.release)
		synctest.Wait()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if p.rewrites != 1 {
			t.Fatalf("rewrites=%d", p.rewrites)
		}
	})
}
func TestAutomaticSnapshotRulesAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &checkpointPersistence{rules: []data.SaveRule{{Seconds: 2, Changes: 2}}}
		db := New(storage.New(), p)
		initial := db.LastSave()
		put(t, db, "one", "1")
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if len(p.snapshots) != 0 {
			t.Fatal("saved below change threshold")
		}
		put(t, db, "two", "2")
		time.Sleep(time.Second)
		synctest.Wait()
		if len(p.snapshots) != 1 || db.LastSave() <= initial {
			t.Fatal("automatic snapshot did not complete")
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if len(p.snapshots) != 1 {
			t.Fatal("saved unchanged database")
		}
		put(t, db, "last", "shutdown")
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if len(p.snapshots) != 2 || p.snapshots[1]["last"].Value != "shutdown" {
			t.Fatal("final snapshot missing")
		}
		if !p.backgrounds[0] || p.backgrounds[1] {
			t.Fatalf("automatic/shutdown snapshot modes: %v", p.backgrounds)
		}
	})
}
func TestSaveFailureRetainsDirtyStateAndLastSave(t *testing.T) {
	p := &checkpointPersistence{fail: errors.New("disk failed")}
	db := New(storage.New(), p)
	defer db.Close()
	put(t, db, "key", "value")
	before := db.LastSave()
	if err := db.Save(context.Background()); !errors.Is(err, p.fail) {
		t.Fatal(err)
	}
	if db.dirty.Load() != 1 || db.LastSave() != before {
		t.Fatal("failure advanced checkpoint")
	}
	p.fail = nil
	if err := db.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.dirty.Load() != 0 {
		t.Fatal("successful save did not clear captured changes")
	}
}

type pausingRewrite struct {
	checkpointPersistence
	started, continueRewrite chan struct{}
}

func (p *pausingRewrite) BeginRewrite(context.Context, map[string]storage.Entry) (func() error, error) {
	close(p.started)
	return func() error { <-p.continueRewrite; return nil }, nil
}
func TestSaveSchedulingAndForegroundSaveDuringRewrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &pausingRewrite{started: make(chan struct{}), continueRewrite: make(chan struct{})}
		db := New(storage.New(), p)
		put(t, db, "key", "value")
		if err := db.StartRewrite(); err != nil {
			t.Fatal(err)
		}
		<-p.started
		if err := db.StartSave(); !errors.Is(err, ErrRewriteInProgress) {
			t.Fatal(err)
		}
		if err := db.StartSave(true); !errors.Is(err, ErrSaveScheduled) {
			t.Fatal(err)
		}
		if err := db.Save(context.Background()); err != nil {
			t.Fatal("foreground SAVE during rewrite", err)
		}
		close(p.continueRewrite)
		synctest.Wait()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if len(p.snapshots) != 2 {
			t.Fatalf("snapshots=%d", len(p.snapshots))
		}
		if p.backgrounds[0] || !p.backgrounds[1] {
			t.Fatalf("foreground/scheduled snapshot modes: %v", p.backgrounds)
		}
	})
}

func TestRewriteIgnoresOnlySnapshotWriteProtection(t *testing.T) {
	aofFailure := errors.New("AOF write failed")
	for _, tc := range []struct {
		name      string
		writeErr  error
		wantError bool
	}{
		{"healthy", nil, false},
		{"RDB failure", errors.Join(ErrSnapshotFailed, errors.New("RDB write failed")), false},
		{"AOF failure", aofFailure, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &checkpointPersistence{writeErr: tc.writeErr}
			db := New(storage.New(), p)
			defer db.Close()
			err := db.Rewrite(context.Background())
			if tc.wantError {
				if !errors.Is(err, tc.writeErr) || p.rewrites != 0 {
					t.Fatalf("unsafe rewrite: %v, rewrites=%d", err, p.rewrites)
				}
			} else if err != nil || p.rewrites != 1 {
				t.Fatalf("rewrite blocked: %v, rewrites=%d", err, p.rewrites)
			}
		})
	}
}
