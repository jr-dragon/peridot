package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jr-dragon/peridot/internal/storage"
)

func TestMemoryOnlyBackends(t *testing.T) {
	for name, newStore := range map[string]func() storage.Backend{
		"xsync": func() storage.Backend { return storage.NewXsync() },
		"dlht":  func() storage.Backend { return storage.NewDLHT() },
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

func TestRawSnapshotIncludesExpiredEntriesAndCopiesMap(t *testing.T) {
	for name, store := range map[string]storage.Backend{"xsync": storage.NewXsync(), "dlht": storage.NewDLHT()} {
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
