package persistence

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

type batchFile struct {
	appendFile
	entered chan int
	release chan struct{}
	writes  atomic.Int64
	syncs   atomic.Int64
	fail    bool
}

func TestAOFAlwaysSyncPrecedesMemoryPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "always")
		if err != nil {
			t.Fatal(err)
		}
		f := observeSync(s)
		f.entered, f.release = make(chan struct{}), make(chan struct{})
		backend := storage.New()
		backend.Put("key", storage.Entry{Value: "before"})
		db := database.New(backend, s)
		defer db.Close()
		finished := make(chan error, 1)
		go func() {
			finished <- db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
				return storage.Entry{Value: "after"}, true, nil
			})
		}()
		<-f.entered
		if entry, _ := backend.Load("key"); entry.Value != "before" {
			t.Fatal("published before fsync completed", entry)
		}
		// Live reads can proceed while another goroutine is syncing its append.
		if entry, _, err := db.Get(context.Background(), "key"); err != nil || entry.Value != "before" {
			t.Fatal(entry, err)
		}
		close(f.release)
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if entry, _ := backend.Load("key"); entry.Value != "after" {
			t.Fatal("missing synced publication", entry)
		}
	})
}

func TestAOFDescriptorWaitsForActiveAppend(t *testing.T) {
	for _, operation := range []string{"rewrite", "close"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "no")
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				f := &batchFile{appendFile: s.file, entered: make(chan int, 1), release: make(chan struct{})}
				s.file = f
				appended := make(chan error, 1)
				go func() { appended <- s.Append("key", storage.Entry{Value: "value"}, true) }()
				<-f.entered
				finished := make(chan error, 1)
				go func() {
					if operation == "close" {
						finished <- s.Close()
					} else {
						finished <- s.Rewrite(context.Background(), map[string]storage.Entry{"key": {Value: "value"}})
					}
				}()
				synctest.Wait()
				select {
				case err := <-finished:
					t.Fatalf("%s crossed active write: %v", operation, err)
				default:
				}
				close(f.release)
				if err := <-appended; err != nil {
					t.Fatal(err)
				}
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
				state, err := aof.Load(bytes.NewReader(readAOF(t, s.path)))
				if err != nil || state["key"].Value != "value" {
					t.Fatal(state, err)
				}
			})
		})
	}
}

func TestAOFConcurrentComputedUpdatesSurviveRewriteAndRestart(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.AppendOnly, config.AppendFsync, config.Save = true, policy, nil
			p, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			backend := storage.New()
			db := database.New(backend, p, database.WithActiveExpiration(10, false))
			defer db.Close()
			if dbErr := db.Update(context.Background(), "deleted", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
				return storage.Entry{Value: "remove"}, true, nil
			}); dbErr != nil {
				t.Fatal(dbErr)
			}
			const workers, updates = 8, 50
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Go(func() {
					for i := range updates {
						for _, key := range []string{"shared", fmt.Sprint(worker)} {
							if err := db.Update(context.Background(), key, func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
								n := 0
								if exists {
									n, _ = strconv.Atoi(e.Value.(string))
								}
								deadline := int64(0)
								if i%2 != 0 {
									deadline = 4102444800000
								}
								return storage.Entry{Value: strconv.Itoa(n + 1), ExpiresAt: deadline}, true, nil
							}); err != nil {
								t.Error(err)
								return
							}
						}
					}
				})
			}
			if err := db.StartRewrite(); err != nil {
				t.Fatal(err)
			}
			if n, err := db.DeleteKeys(context.Background(), []string{"deleted", "deleted"}); err != nil || n != 1 {
				t.Fatal(n, err)
			}
			wg.Wait()
			want := backend.Snapshot()
			if want["shared"].Value != strconv.Itoa(workers*updates) || len(want) != workers+1 {
				t.Fatal("lost concurrent update", want)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := reopened.Restore(); !reflect.DeepEqual(got, want) {
				t.Fatalf("restart diverged: got %#v, want %#v", got, want)
			}
		})
	}
}

func (f *batchFile) Write(p []byte) (int, error) {
	n := int(f.writes.Add(1))
	f.entered <- n
	<-f.release
	if f.fail {
		return len(p) / 2, nil // A short write without an OS error is still fatal.
	}
	return f.appendFile.Write(p)
}

func (f *batchFile) Sync() error {
	f.syncs.Add(1)
	return f.appendFile.Sync()
}

func TestAOFWaitingAppendsShareWriteAndSync(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), policy)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				f := &batchFile{appendFile: s.file, entered: make(chan int, 2), release: make(chan struct{})}
				s.file = f
				first := make(chan error, 1)
				go func() { first <- s.Append("first", storage.Entry{Value: "one"}, true) }()
				if n := <-f.entered; n != 1 {
					t.Fatal(n)
				}
				const writers = 16
				finished := make(chan error, writers)
				for i := range writers {
					go func() {
						finished <- s.Append(fmt.Sprint(i), storage.Entry{Value: "\x00\xff\r\n", ExpiresAt: 4102444800000}, true)
					}()
				}
				synctest.Wait()
				s.mu.Lock()
				sequence, committed := s.sequence, s.committed
				s.mu.Unlock()
				if sequence != writers+1 || committed != 0 {
					t.Fatalf("pending appends: sequence=%d committed=%d", sequence, committed)
				}
				f.release <- struct{}{}
				if err := <-first; err != nil {
					t.Fatal(err)
				}
				if n := <-f.entered; n != 2 {
					t.Fatal(n)
				}
				select {
				case err := <-finished:
					t.Fatalf("append acknowledged before batch write: %v", err)
				default:
				}
				f.release <- struct{}{}
				for range writers {
					if err := <-finished; err != nil {
						t.Fatal(err)
					}
				}
				wantSyncs := int64(0)
				if policy == "always" {
					wantSyncs = 2
				}
				if f.writes.Load() != 2 || f.syncs.Load() != wantSyncs {
					t.Fatalf("writes=%d syncs=%d", f.writes.Load(), f.syncs.Load())
				}
				state, err := aof.Load(bytes.NewReader(readAOF(t, s.path)))
				if err != nil || len(state) != writers+1 || state["first"].Value != "one" {
					t.Fatalf("replay: %v, %v", state, err)
				}
				for i := range writers {
					if e := state[fmt.Sprint(i)]; e.Value != "\x00\xff\r\n" || e.ExpiresAt != 4102444800000 {
						t.Fatalf("replayed entry %d: %#v", i, e)
					}
				}
			})
		})
	}
}

func TestAOFBatchShortWriteFailsAllWaitingAppends(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "always")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		f := &batchFile{appendFile: s.file, entered: make(chan int, 1), release: make(chan struct{}), fail: true}
		s.file = f
		finished := make(chan error, 17)
		go func() { finished <- s.Append("first", storage.Entry{Value: "one"}, true) }()
		<-f.entered
		for i := range 16 {
			go func() { finished <- s.Append(fmt.Sprint(i), storage.Entry{Value: "two"}, true) }()
		}
		synctest.Wait()
		close(f.release)
		for range 17 {
			if err := <-finished; !errors.Is(err, database.ErrAOFFailed) || !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("lost batch failure: %v", err)
			}
		}
		if err := s.Append("later", storage.Entry{Value: "three"}, true); !errors.Is(err, io.ErrShortWrite) {
			t.Fatal(err)
		}
		if f.writes.Load() != 1 || f.syncs.Load() != 0 {
			t.Fatalf("continued after short write: writes=%d syncs=%d", f.writes.Load(), f.syncs.Load())
		}
	})
}

func TestAOFBatchSyncFailureWakesAllWaitingAppends(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "always")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		f := observeSync(s)
		cause := errors.New("batch sync failure")
		f.entered, f.release, f.syncErr = make(chan struct{}), make(chan struct{}), cause
		finished := make(chan error, 17)
		go func() { finished <- s.Append("first", storage.Entry{Value: "one"}, true) }()
		<-f.entered
		for i := range 16 {
			go func() { finished <- s.Append(fmt.Sprint(i), storage.Entry{Value: "two"}, true) }()
		}
		synctest.Wait()
		close(f.release)
		for range 17 {
			if err := <-finished; !errors.Is(err, database.ErrAOFFailed) || !errors.Is(err, cause) {
				t.Fatalf("lost sync failure: %v", err)
			}
		}
		if s.committed != 0 || f.syncs.Load() != 1 {
			t.Fatalf("continued after sync failure: committed=%d syncs=%d", s.committed, f.syncs.Load())
		}
	})
}

func TestAOFCompletedBatchSurvivesLaterFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "no")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		f := &batchFile{appendFile: s.file, entered: make(chan int, 3), release: make(chan struct{})}
		s.file = f
		first := make(chan error, 1)
		go func() { first <- s.Append("first", storage.Entry{Value: "one"}, true) }()
		<-f.entered
		const writers = 16
		completed := make(chan error, writers)
		for i := range writers {
			go func() { completed <- s.Append(fmt.Sprint(i), storage.Entry{Value: "two"}, true) }()
		}
		synctest.Wait()
		f.release <- struct{}{}
		if err := <-first; err != nil {
			t.Fatal(err)
		}
		if n := <-f.entered; n != 2 {
			t.Fatal(n)
		}
		failed := make(chan error, 2)
		go func() { failed <- s.Append("third", storage.Entry{Value: "three"}, true) }()
		synctest.Wait()
		f.release <- struct{}{}
		if n := <-f.entered; n != 3 {
			t.Fatal(n)
		}
		go func() { failed <- s.Append("fourth", storage.Entry{Value: "four"}, true) }()
		synctest.Wait()
		f.fail = true
		close(f.release)
		for range 2 {
			if err := <-failed; !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("missing later batch failure: %v", err)
			}
		}
		for range writers {
			if err := <-completed; err != nil {
				t.Fatalf("later failure changed a completed batch: %v", err)
			}
		}
		if f.writes.Load() != 3 {
			t.Fatalf("wrote a queued batch after failure: %d", f.writes.Load())
		}
	})
}

func TestAOFCloseDrainsQueuedAppends(t *testing.T) {
	for _, writers := range []int{16, 272} {
		t.Run(strconv.Itoa(writers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := OpenAOF(filepath.Join(t.TempDir(), "data.aof"), "always")
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				f := &batchFile{appendFile: s.file, entered: make(chan int, 4), release: make(chan struct{})}
				s.file = f
				finished := make(chan error, writers+1)
				go func() { finished <- s.Append("first", storage.Entry{Value: "one"}, true) }()
				<-f.entered
				for i := range writers {
					go func() { finished <- s.Append(fmt.Sprint(i), storage.Entry{Value: "two"}, true) }()
				}
				synctest.Wait()
				closed := make(chan error, 1)
				go func() { closed <- s.Close() }()
				synctest.Wait()
				if err := s.Append("late", storage.Entry{Value: "rejected"}, true); !errors.Is(err, ErrAOFClosed) {
					t.Fatalf("accepted append after close started: %v", err)
				}
				close(f.release)
				for range writers + 1 {
					if err := <-finished; err != nil {
						t.Fatalf("discarded an accepted append: %v", err)
					}
				}
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenAOF(s.path)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if got := reopened.Restore(); len(got) != writers+1 || got["first"].Value != "one" {
					t.Fatalf("lost accepted appends on close: %#v", got)
				}
			})
		})
	}
}
