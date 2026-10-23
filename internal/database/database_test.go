package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/storage"
)

// Hiding Xsync's command-facing methods verifies that Backend is independent
// of KeyStore and that the database can coordinate only the raw primitives.
type rawOnlyBackend struct{ storage.Backend }

func TestMemoryBackendsUseDatabaseClock(t *testing.T) {
	for _, rawOnly := range []bool{false, true} {
		for _, operation := range []string{"Get", "Read", "Update", "DeleteKeys"} {
			for _, now := range []int64{1000, 1001} {
				t.Run(fmt.Sprintf("raw-only=%t/%s/now=%d", rawOnly, operation, now), func(t *testing.T) {
					var backend storage.Backend = storage.NewXsync()
					if rawOnly {
						backend = rawOnlyBackend{backend}
					}
					original := storage.Entry{Value: "value", ExpiresAt: 1000}
					backend.Put("key\x00\xff", original)
					db := New(backend, nil, WithClock(func() time.Time { return time.UnixMilli(now) }))
					t.Cleanup(func() { _ = db.Close() })
					ctx := context.Background()
					wantExists := now == original.ExpiresAt
					check := func(e storage.Entry, exists bool, snapshot int64) {
						t.Helper()
						if exists != wantExists || (exists && e != original) {
							t.Errorf("entry = %v, %v; want live=%t", e, exists, wantExists)
						}
						if snapshot != now {
							t.Errorf("clock snapshot = %d, want %d", snapshot, now)
						}
					}
					var err error
					switch operation {
					case "Get":
						var e storage.Entry
						var exists bool
						e, exists, err = db.Get(ctx, "key\x00\xff")
						check(e, exists, now)
					case "Read":
						err = db.Read(ctx, "key\x00\xff", check)
					case "Update":
						err = db.Update(ctx, "key\x00\xff", func(e storage.Entry, exists bool, snapshot int64) (storage.Entry, bool, error) {
							check(e, exists, snapshot)
							return e, exists, storage.ErrUnchanged
						})
					case "DeleteKeys":
						var count int64
						count, err = db.DeleteKeys(ctx, []string{"key\x00\xff", "key\x00\xff"})
						wantCount := int64(0)
						if wantExists {
							wantCount = 1
						}
						if count != wantCount {
							t.Errorf("deletion count = %d, want %d", count, wantCount)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					_, remains := backend.Load("key\x00\xff")
					if want := wantExists && operation != "DeleteKeys"; remains != want {
						t.Fatalf("physical entry remains = %t, want %t", remains, want)
					}
				})
			}
		}
	}
}

func TestMemoryOnlyBackends(t *testing.T) {
	for name, newStore := range map[string]func() storage.Backend{
		"xsync":    func() storage.Backend { return storage.NewXsync() },
		"raw-only": func() storage.Backend { return rawOnlyBackend{storage.New()} },
	} {
		t.Run(name, func(t *testing.T) {
			store := newStore()
			db := New(store, nil)
			const workers, iterations = 4, 50
			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					for range iterations {
						if err := db.Update(context.Background(), "counter", func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
							n := 0
							if exists {
								n = e.Value.(int)
							}
							return storage.Entry{Value: n + 1}, true, nil
						}); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
			e, ok, err := db.Get(context.Background(), "counter")
			if err != nil || !ok || e.Value != workers*iterations {
				t.Fatalf("counter = %v, %v, %v", e, ok, err)
			}
			if raw, exists := store.Load("counter"); !exists || raw != e {
				t.Fatal("database did not use supplied backend")
			}
			if err := db.StartRewrite(); !errors.Is(err, ErrPersistenceDisabled) {
				t.Fatal(err)
			}
			if err := db.Rewrite(context.Background()); !errors.Is(err, ErrPersistenceDisabled) {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := db.Get(context.Background(), "counter"); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
			if err := db.Update(context.Background(), "counter", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
				t.Fatal("callback after close")
				return storage.Entry{}, false, nil
			}); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}

func TestRawOnlyBackendExpirationAndAtomicErrors(t *testing.T) {
	ctx := context.Background()
	backend := rawOnlyBackend{storage.New()}
	if _, ok := any(backend).(storage.KeyStore); ok {
		t.Fatal("raw backend unexpectedly exposes command operations")
	}
	db := New(backend, nil, WithClock(func() time.Time { return time.UnixMilli(1000) }))
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	key := "binary\x00\xff"
	backend.Put(key, storage.Entry{Value: "expired", ExpiresAt: 999})
	if _, exists, err := db.Get(ctx, key); err != nil || exists {
		t.Fatalf("expired Get = %v, %v", exists, err)
	}
	if _, exists := backend.Load(key); exists {
		t.Fatal("expired entry remained in raw backend")
	}
	if err := db.Update(ctx, key, func(e storage.Entry, exists bool, now int64) (storage.Entry, bool, error) {
		if exists || now != 1000 {
			t.Fatalf("update view = %v, %d", exists, now)
		}
		return storage.Entry{Value: "live", ExpiresAt: now}, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("abort update")
	if err := db.Update(ctx, key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
		return storage.Entry{Value: "must not publish"}, true, cause
	}); !errors.Is(err, cause) {
		t.Fatalf("update error = %v", err)
	}
	if err := db.Read(ctx, key, func(e storage.Entry, exists bool, now int64) {
		if !exists || e.Value != "live" || e.ExpiresAt != now || now != 1000 {
			t.Fatalf("Read = %v, %v, %d", e, exists, now)
		}
	}); err != nil {
		t.Fatal(err)
	}
	backend.Put("expired", storage.Entry{Value: "old", ExpiresAt: 999})
	if count, err := db.DeleteKeys(ctx, []string{key, key, "expired", "missing"}); err != nil || count != 1 {
		t.Fatalf("DeleteKeys = %d, %v", count, err)
	}
	if len(backend.Snapshot()) != 0 {
		t.Fatal("deletion left raw entries")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := db.Update(canceled, key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
		t.Fatal("canceled update invoked callback")
		return storage.Entry{}, false, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Update = %v", err)
	}
	if err := db.Read(canceled, key, func(storage.Entry, bool, int64) {
		t.Fatal("canceled read invoked callback")
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Read = %v", err)
	}
}

func TestRawSnapshotIncludesExpiredEntriesAndCopiesMap(t *testing.T) {
	for name, store := range map[string]storage.Backend{"xsync": storage.NewXsync()} {
		t.Run(name, func(t *testing.T) {
			for i := range 300 {
				store.Put(fmt.Sprint(i), storage.Entry{Value: fmt.Sprint(i), ExpiresAt: 1})
			}
			snapshot := store.Snapshot()
			if len(snapshot) != 300 {
				t.Fatalf("snapshot entries = %d", len(snapshot))
			}
			delete(snapshot, "1")
			store.Put("0", storage.Entry{Value: "replacement"})
			if snapshot["0"].Value != "0" {
				t.Fatal("snapshot changed after publication")
			}
			if e, exists := store.Load("1"); !exists || e.Value != "1" {
				t.Fatal("snapshot mutation changed backend")
			}
			store.Delete("1")
			if _, exists := store.Load("1"); exists {
				t.Fatal("raw deletion failed")
			}
		})
	}
}
