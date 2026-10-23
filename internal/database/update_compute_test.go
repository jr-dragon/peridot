package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/storage"
)

type completedComputeBackend struct {
	storage.Backend
	pause             atomic.Bool
	completed, resume chan struct{}
}

func (b *completedComputeBackend) Compute(key string, compute storage.ComputeFunc) {
	b.Backend.Compute(key, compute)
	if b.pause.CompareAndSwap(true, false) {
		close(b.completed)
		<-b.resume
	}
}

func awaitUpdate(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("update did not complete")
	}
}

func TestComputedUpdateKeepsIndexInPublicationOrder(t *testing.T) {
	backend := &completedComputeBackend{Backend: storage.New(), completed: make(chan struct{}), resume: make(chan struct{})}
	backend.Put("key", storage.Entry{Value: "initial", ExpiresAt: 2000})
	db := manualExpirationDB(t, backend, &snapshotOnlyPersistence{})
	backend.pause.Store(true)
	var once sync.Once
	unblock := func() { once.Do(func() { close(backend.resume) }) }
	t.Cleanup(unblock)
	first := make(chan error, 1)
	go func() {
		first <- db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
			return storage.Entry{Value: "persistent"}, true, nil
		})
	}()
	<-backend.completed
	// The physical update is complete, but the outer DB operation still owns
	// its shared gate. Neither a checkpoint nor Close may cross that boundary.
	if db.mu.TryLock() {
		db.mu.Unlock()
		t.Fatal("unfinished update released the database gate")
	}
	assertExpirationIndex(t, db)
	second := make(chan error, 1)
	go func() {
		second <- db.Update(context.Background(), "key", func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
			if !exists || e.Value != "persistent" {
				return e, exists, fmt.Errorf("second update observed %v, %v", e, exists)
			}
			return storage.Entry{Value: "expiring", ExpiresAt: 3000}, true, nil
		})
	}()
	// A DB stripe would unnecessarily block this already-published same key.
	awaitUpdate(t, second)
	assertExpirationIndex(t, db)
	unblock()
	awaitUpdate(t, first)
	assertExpirationIndex(t, db)
	if db.dirty.Load() != 2 || len(db.expiration.keys) != 1 {
		t.Fatal("lost mutation accounting or later TTL index entry")
	}
}

type completedDirectBackend struct {
	storage.Backend
	storage.KeyStore
	pause             atomic.Bool
	completed, resume chan struct{}
}

func (b *completedDirectBackend) Update(ctx context.Context, key string, update storage.UpdateFunc) error {
	err := b.KeyStore.Update(ctx, key, update)
	if b.pause.CompareAndSwap(true, false) {
		close(b.completed)
		<-b.resume
	}
	return err
}

func TestMemoryUpdateHasNoDatabaseKeyStripe(t *testing.T) {
	store := storage.NewXsync()
	backend := &completedDirectBackend{Backend: store, KeyStore: store, completed: make(chan struct{}), resume: make(chan struct{})}
	db := New(backend, nil)
	var once sync.Once
	unblock := func() { once.Do(func() { close(backend.resume) }) }
	t.Cleanup(func() { unblock(); _ = db.Close() })
	backend.pause.Store(true)
	update := func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
		n := 0
		if exists {
			n = e.Value.(int)
		}
		return storage.Entry{Value: n + 1}, true, nil
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- db.Update(context.Background(), "counter", update) }()
	<-backend.completed
	go func() { second <- db.Update(context.Background(), "counter", update) }()
	awaitUpdate(t, second)
	if db.mu.TryLock() {
		db.mu.Unlock()
		t.Fatal("direct update released the database gate")
	}
	unblock()
	awaitUpdate(t, first)
	if e, exists, err := db.Get(context.Background(), "counter"); err != nil || !exists || e.Value != 2 {
		t.Fatal(e, exists, err)
	}
}

type requestedComputeBackend struct {
	storage.Backend
	requested chan struct{}
}

func (b *requestedComputeBackend) Compute(key string, compute storage.ComputeFunc) {
	close(b.requested)
	b.Backend.Compute(key, compute)
}

func TestComputedOperationsRecheckCancellationAfterBucketWait(t *testing.T) {
	for _, operation := range []string{"Update", "Get", "Read"} {
		t.Run(operation, func(t *testing.T) {
			store := storage.New()
			store.Put("key", storage.Entry{Value: "expired", ExpiresAt: 999})
			backend := &requestedComputeBackend{Backend: store, requested: make(chan struct{})}
			db := manualExpirationDB(t, backend, nil)
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			go func() {
				store.Compute("key", func(e storage.Entry, _ bool) (storage.Entry, storage.ComputeOp) {
					close(entered)
					<-release
					return e, storage.ComputeCancel
				})
				close(finished)
			}()
			<-entered
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if operation == "Update" {
					result <- db.Update(ctx, "key", func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
						return e, exists, errors.New("canceled callback ran")
					})
				} else {
					_, _, err := readOperation(ctx, db, operation, "key")
					result <- err
				}
			}()
			<-backend.requested
			cancel()
			unblock()
			<-finished
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, exists := store.Load("key"); !exists || db.dirty.Load() != 0 {
				t.Fatal("canceled operation changed the expired entry")
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestComputedUpdateExpirationAndErrors(t *testing.T) {
	for _, initial := range []string{"missing", "live", "expired"} {
		for _, outcome := range []string{"unchanged", "error", "delete", "past", "replace", "same"} {
			t.Run(initial+"/"+outcome, func(t *testing.T) {
				store := storage.New()
				original := storage.Entry{Value: "old", ExpiresAt: 2000}
				if initial == "expired" {
					original.ExpiresAt = 999
				}
				if initial != "missing" {
					store.Put("key", original)
				}
				db := manualExpirationDB(t, store, &snapshotOnlyPersistence{})
				cause := errors.New("callback failed")
				calls := 0
				err := db.Update(context.Background(), "key", func(e storage.Entry, exists bool, now int64) (storage.Entry, bool, error) {
					calls++
					if now != 1000 || exists != (initial == "live") {
						t.Errorf("view = %v, %v, %d", e, exists, now)
					}
					switch outcome {
					case "unchanged":
						return storage.Entry{Value: "ignored"}, true, fmt.Errorf("wrapped: %w", storage.ErrUnchanged)
					case "error":
						return storage.Entry{Value: "ignored"}, true, cause
					case "delete":
						return storage.Entry{}, false, nil
					case "past":
						return storage.Entry{Value: "gone", ExpiresAt: 999}, true, nil
					case "same":
						return e, exists, nil
					default:
						return storage.Entry{Value: "new", ExpiresAt: 1000}, true, nil
					}
				})
				wantErr := error(nil)
				if outcome == "error" {
					wantErr = cause
				}
				if calls != 1 || !errors.Is(err, wantErr) {
					t.Fatal(calls, err)
				}
				entry, exists := store.Load("key")
				wantExists := outcome == "replace" || (initial == "live" && (outcome == "same" || outcome == "unchanged" || outcome == "error"))
				if exists != wantExists {
					t.Fatal(entry, exists)
				}
				if exists && outcome == "replace" && entry != (storage.Entry{Value: "new", ExpiresAt: 1000}) {
					t.Fatal(entry)
				}
				if exists && outcome != "replace" && entry != original {
					t.Fatal(entry)
				}
				wantDirty := uint64(0)
				if initial == "expired" {
					wantDirty++
				}
				if outcome == "replace" || (initial == "live" && outcome != "unchanged" && outcome != "error") {
					wantDirty++
				}
				if db.dirty.Load() != wantDirty {
					t.Fatalf("dirty = %d, want %d", db.dirty.Load(), wantDirty)
				}
				assertExpirationIndex(t, db)
			})
		}
	}
}

type failingComputedPersistence struct {
	snapshotOnlyPersistence
	failure         error
	failAt, appends int
	poison          bool
}

func (p *failingComputedPersistence) Err() error { return p.failure }
func (p *failingComputedPersistence) Append(string, storage.Entry, bool) error {
	p.appends++
	if p.appends == p.failAt {
		if p.poison {
			p.failure = ErrAOFFailed
			return p.failure
		}
		return errors.New("append failed")
	}
	return nil
}

func TestComputedUpdateAppendFailureDoesNotPublishReplacement(t *testing.T) {
	for _, tc := range []struct {
		name              string
		expired, poison   bool
		failAt, wantCalls int
		wantExists        bool
		wantDirty         uint64
	}{
		{"live-append", false, false, 1, 1, true, 0},
		{"expiration-append", true, false, 1, 0, true, 0},
		{"replacement-append", true, false, 2, 1, false, 1},
		{"expiration-poisons", true, true, 1, 0, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := storage.New()
			deadline := int64(2000)
			if tc.expired {
				deadline = 999
			}
			original := storage.Entry{Value: "old", ExpiresAt: deadline}
			store.Put("key", original)
			p := &failingComputedPersistence{failAt: tc.failAt, poison: tc.poison}
			db := manualExpirationDB(t, store, p)
			calls := 0
			err := db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
				calls++
				return storage.Entry{Value: "must not publish"}, true, nil
			})
			if err == nil || calls != tc.wantCalls {
				t.Fatal(err, calls)
			}
			entry, exists := store.Load("key")
			if exists != tc.wantExists || (exists && entry != original) || db.dirty.Load() != tc.wantDirty {
				t.Fatal(entry, exists, db.dirty.Load())
			}
			assertExpirationIndex(t, db)
		})
	}
}
