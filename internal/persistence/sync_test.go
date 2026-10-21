package persistence

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

type observedSyncFile struct {
	appendFile
	syncs   atomic.Int64
	syncErr error
	entered chan struct{}
	release chan struct{}
}

func (f *observedSyncFile) Sync() error {
	if f.syncs.Add(1) == 1 && f.entered != nil {
		close(f.entered)
		<-f.release
	}
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.appendFile.Sync()
}

func observeSync(s *AOF) *observedSyncFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := &observedSyncFile{appendFile: s.file}
	s.file = f
	return f
}

func TestAOFConfiguredSyncPolicies(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := data.DefaultConfig()
				config.AppendOnly, config.AppendFsync = true, policy
				config.Dir = t.TempDir()
				p, err := New(config)
				if err != nil {
					t.Fatal(err)
				}
				s := p.(*Engine).aof.AOF
				defer p.Close()
				f := observeSync(s)
				key, value := "key\x00\xff", "value\r\n\x00\xff"
				if err := s.Append(key, storage.Entry{Value: value}, true); err != nil {
					t.Fatal(err)
				}
				if got := readAOF(t, s.path); !bytes.Equal(got, aof.Replacement(key, value, 0)) {
					t.Fatalf("append bytes = %q", got)
				}
				want := int64(0)
				if policy == "always" {
					want = 1
				}
				if got := f.syncs.Load(); got != want {
					t.Fatalf("syncs before tick = %d, want %d", got, want)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if policy == "everysec" {
					want = 1
				}
				if got := f.syncs.Load(); got != want {
					t.Fatalf("syncs after tick = %d, want %d", got, want)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if got := f.syncs.Load(); got != want {
					t.Fatalf("idle syncs = %d, want %d", got, want)
				}
				if err := s.Append("pending", storage.Entry{Value: "shutdown"}, true); err != nil {
					t.Fatal(err)
				}
				if policy == "always" {
					want++
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				want++
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if got := f.syncs.Load(); got != want {
					t.Fatalf("syncs after close = %d, want %d", got, want)
				}
				if s.done != nil {
					select {
					case <-s.done:
					default:
						t.Fatal("close did not join worker")
					}
				}
				restored, err := OpenAOF(s.path)
				if err != nil {
					t.Fatal(err)
				}
				defer restored.Close()
				state := restored.Restore()
				if state[key].Value != value || state["pending"].Value != "shutdown" {
					t.Fatalf("restored = %#v", state)
				}
			})
		})
	}
}

func TestAOFEverysecSyncFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "everysec")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		f := observeSync(s)
		cause := errors.New("background disk failure")
		f.syncErr = cause
		db := database.New(storage.New(), s)
		if err := db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
			return storage.Entry{Value: "acknowledged"}, true, nil
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if !errors.Is(s.Err(), cause) {
			t.Fatalf("lost background error: %v", s.Err())
		}
		if err := db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
			t.Fatal("executed update after sync failure")
			return storage.Entry{}, false, nil
		}); !errors.Is(err, cause) {
			t.Fatalf("update = %v", err)
		}
		if entry, ok, err := db.Get(context.Background(), "key"); err != nil || !ok || entry.Value != "acknowledged" {
			t.Fatalf("read after failed sync = %v, %v, %v", entry, ok, err)
		}
		if err := s.Rewrite(context.Background(), nil); !errors.Is(err, cause) {
			t.Fatalf("rewrite = %v", err)
		}
		if err := db.Close(); !errors.Is(err, cause) {
			t.Fatalf("close = %v", err)
		}
	})
}

func TestAOFEverysecAppendDuringSync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "everysec")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		f := observeSync(s)
		f.entered, f.release = make(chan struct{}), make(chan struct{})
		if err := s.Append("first", storage.Entry{Value: "one"}, true); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		<-f.entered
		// This append must finish even though the background sync is blocked.
		if err := s.Append("second", storage.Entry{Value: "two"}, true); err != nil {
			t.Fatal(err)
		}
		close(f.release)
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if got := f.syncs.Load(); got != 2 {
			t.Fatalf("write during sync was not synced on next tick: %d", got)
		}
	})
}

func TestAOFEverysecWaitsForSyncBeforeRewriteOrClose(t *testing.T) {
	for _, operation := range []string{"rewrite", "close"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "everysec")
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				f := observeSync(s)
				f.entered, f.release = make(chan struct{}), make(chan struct{})
				entry := storage.Entry{Value: "value"}
				if err := s.Append("key", entry, true); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				<-f.entered
				finished := make(chan error, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					if operation == "rewrite" {
						finished <- s.Rewrite(context.Background(), map[string]storage.Entry{"key": entry})
					} else {
						finished <- s.Close()
					}
				}()
				<-started
				select {
				case err := <-finished:
					t.Fatalf("%s finished during sync: %v", operation, err)
				default:
				}
				close(f.release)
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
				if operation == "rewrite" {
					replacement := observeSync(s)
					if err := s.Append("after", entry, true); err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Second)
					synctest.Wait()
					if replacement.syncs.Load() != 1 {
						t.Fatal("replacement file was not synced")
					}
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
				}
				restored, err := OpenAOF(s.path)
				if err != nil {
					t.Fatal(err)
				}
				defer restored.Close()
				state := restored.Restore()
				if state["key"] != entry || (operation == "rewrite" && state["after"] != entry) {
					t.Fatal(state)
				}
			})
		})
	}
}

func TestAOFDisabledDoesNotTouchFiles(t *testing.T) {
	config := data.DefaultConfig()
	config.Dir = t.TempDir()
	path := filepath.Join(config.Dir, config.AppendFilename)
	original := []byte("invalid AOF must not be read or changed")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if !bytes.Equal(readAOF(t, path), original) {
		t.Fatal("disabled AOF changed file")
	}
	if _, err := os.Stat(path + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled AOF created lock: %v", err)
	}
}

func TestAOFRejectsInvalidSyncPolicyBeforeOpening(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	if s, err := OpenAOF(path, "invalid"); err == nil {
		s.Close()
		t.Fatal("accepted invalid policy")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid policy created file: %v", err)
	}
}
