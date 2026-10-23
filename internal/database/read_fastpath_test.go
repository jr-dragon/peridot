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

func readOperation(ctx context.Context, db *DB, operation, key string) (storage.Entry, bool, error) {
	if operation == "Get" {
		return db.Get(ctx, key)
	}
	var entry storage.Entry
	var exists bool
	err := db.Read(ctx, key, func(e storage.Entry, ok bool, _ int64) { entry, exists = e, ok })
	return entry, exists, err
}

func TestLiveReadsDoNotWaitForUnpublishedUpdate(t *testing.T) {
	for _, mode := range []string{"memory", "active-expiration", "snapshot"} {
		for _, operation := range []string{"Get", "Read"} {
			t.Run(mode+"/"+operation, func(t *testing.T) {
				store := storage.New()
				store.Put("key", storage.Entry{Value: "before"})
				var p Persistence
				if mode == "snapshot" {
					p = &snapshotOnlyPersistence{}
				}
				db := New(store, p)
				if mode == "active-expiration" {
					WithActiveExpiration(10, false)(db)
					close(db.expiration.done)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				updated := make(chan error, 1)
				go func() {
					updated <- db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
						close(entered)
						<-release
						return storage.Entry{Value: "after"}, true, nil
					})
				}()
				<-entered
				read := make(chan error, 1)
				go func() {
					entry, exists, err := readOperation(context.Background(), db, operation, "key")
					if err == nil && (!exists || entry.Value != "before") {
						err = fmt.Errorf("unpublished read: %v, %v", entry, exists)
					}
					read <- err
				}()
				select {
				case err := <-read:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("live read waited for the updater")
				}
				unblock()
				if err := <-updated; err != nil {
					t.Fatal(err)
				}
				entry, exists, err := readOperation(context.Background(), db, operation, "key")
				if err != nil || !exists || entry.Value != "after" {
					t.Fatal(entry, exists, err)
				}
			})
		}
	}
}

type pausedReadBackend struct {
	storage.Backend
	pause          atomic.Bool
	loaded, resume chan struct{}
}

func (b *pausedReadBackend) Load(key string) (storage.Entry, bool) {
	entry, exists := b.Backend.Load(key)
	if b.pause.CompareAndSwap(true, false) {
		close(b.loaded)
		<-b.resume
	}
	return entry, exists
}

func TestExpiredReadsRecheckConcurrentReplacement(t *testing.T) {
	for _, operation := range []string{"Get", "Read"} {
		t.Run(operation, func(t *testing.T) {
			store := &pausedReadBackend{Backend: storage.New(), loaded: make(chan struct{}), resume: make(chan struct{})}
			store.Put("key", storage.Entry{Value: "expired", ExpiresAt: 999})
			db := manualExpirationDB(t, store, nil)
			store.pause.Store(true)
			var once sync.Once
			unblock := func() { once.Do(func() { close(store.resume) }) }
			t.Cleanup(unblock)
			read := make(chan error, 1)
			go func() {
				entry, exists, err := readOperation(context.Background(), db, operation, "key")
				if err == nil && (!exists || entry.Value != "replacement") {
					err = fmt.Errorf("replacement lost: %v, %v", entry, exists)
				}
				read <- err
			}()
			select {
			case <-store.loaded:
			case <-time.After(3 * time.Second):
				t.Fatal("read did not load the expired entry")
			}
			updated := make(chan error, 1)
			go func() {
				updated <- db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
					return storage.Entry{Value: "replacement"}, true, nil
				})
			}()
			select {
			case err := <-updated:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("read prevented replacement")
			}
			unblock()
			if err := <-read; err != nil {
				t.Fatal(err)
			}
			if entry, exists := store.Load("key"); !exists || entry.Value != "replacement" {
				t.Fatal(entry, exists)
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestExpiredReadCancellationDuringLockUpgrade(t *testing.T) {
	for _, operation := range []string{"Get", "Read"} {
		t.Run(operation, func(t *testing.T) {
			store := &pausedReadBackend{Backend: storage.New(), loaded: make(chan struct{}), resume: make(chan struct{})}
			store.Put("key", storage.Entry{Value: "expired", ExpiresAt: 999})
			db := manualExpirationDB(t, store, nil)
			store.pause.Store(true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			read := make(chan error, 1)
			go func() { _, _, err := readOperation(ctx, db, operation, "key"); read <- err }()
			<-store.loaded
			cancel()
			close(store.resume)
			if err := <-read; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, exists := store.Load("key"); !exists || db.dirty.Load() != 0 {
				t.Fatal("canceled read expired a key")
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestLiveReadCallbackKeepsCloseAndDeleteOutsideItsGate(t *testing.T) {
	for _, action := range []string{"close", "delete"} {
		t.Run(action, func(t *testing.T) {
			db := New(storage.New(), nil)
			put(t, db, "key", "value")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() {
				unblock()
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			read := make(chan error, 1)
			go func() {
				read <- db.Read(context.Background(), "key", func(storage.Entry, bool, int64) { close(entered); <-release })
			}()
			<-entered
			if db.mu.TryLock() {
				db.mu.Unlock()
				t.Fatal("read callback released the database gate")
			}
			finished := make(chan error, 1)
			go func() {
				if action == "close" {
					finished <- db.Close()
				} else {
					_, err := db.DeleteKeys(context.Background(), []string{"key"})
					finished <- err
				}
			}()
			unblock()
			if err := <-read; err != nil {
				t.Fatal(err)
			}
			if err := <-finished; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOrderedLiveReadsShareGate(t *testing.T) {
	store := storage.New()
	store.Put("key", storage.Entry{Value: "value"})
	// Unknown persistence keeps the conservative ordered mutation gate.
	db := New(store, &checkpointPersistence{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	first := make(chan error, 1)
	go func() {
		first <- db.Read(context.Background(), "key", func(storage.Entry, bool, int64) { close(entered); <-release })
	}()
	<-entered
	second := make(chan error, 1)
	go func() {
		entry, exists, err := db.Get(context.Background(), "key")
		if err == nil && (!exists || entry.Value != "value") {
			err = fmt.Errorf("read = %v, %v", entry, exists)
		}
		second <- err
	}()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ordered live reads serialized each other")
	}
	if db.mu.TryLock() {
		db.mu.Unlock()
		t.Fatal("read did not exclude ordered publication")
	}
	unblock()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}
