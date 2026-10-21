package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type storeHarness struct {
	Store
	now   *func() time.Time
	load  func(string) (Entry, bool)
	store func(string, Entry)
}

func forEachStore(t *testing.T, run func(*testing.T, storeHarness)) {
	t.Helper()
	t.Run("xsync", func(t *testing.T) {
		s := NewXsync()
		run(t, storeHarness{Store: s, now: &s.now, load: s.entries.Load, store: s.entries.Store})
	})
	t.Run("dlht", func(t *testing.T) {
		s := NewDLHT()
		run(t, storeHarness{Store: s, now: &s.now, load: func(key string) (Entry, bool) {
			return s.shardFor(key).get(key)
		}, store: func(key string, entry Entry) {
			shard := s.shardFor(key)
			shard.mu.Lock()
			defer shard.mu.Unlock()
			if _, updated := shard.entries.Put(key, entry); !updated {
				shard.entries.Insert(key, entry)
			}
		}})
	})
}

func TestStorePassiveExpiration(t *testing.T) {
	forEachStore(t, func(t *testing.T, m storeHarness) {
		var now atomic.Int64
		now.Store(1000)
		*m.now = func() time.Time { return time.UnixMilli(now.Load()) }
		m.store("key", Entry{Value: "old", ExpiresAt: 1010})
		now.Store(1010)
		if entry, exists, err := m.Get(context.Background(), "key"); err != nil || !exists || entry.Value != "old" {
			t.Fatalf("at deadline: %v, %v, %v", entry, exists, err)
		}
		now.Store(1011)
		if _, exists := m.load("key"); !exists {
			t.Fatal("entry was deleted without access")
		}
		if _, exists, err := m.Get(context.Background(), "key"); err != nil || exists {
			t.Fatalf("expired read: exists=%v, err=%v", exists, err)
		}
		if _, exists := m.load("key"); exists {
			t.Fatal("expired read did not physically delete entry")
		}
		for _, fail := range []bool{false, true} {
			m.store("key", Entry{Value: "old", ExpiresAt: 1000})
			cause := errors.New("update failed")
			err := m.Update(context.Background(), "key", func(entry Entry, exists bool, snapshot int64) (Entry, bool, error) {
				if exists || entry.Value != nil || snapshot != 1011 {
					t.Fatalf("expired update input: %v, %v, %d", entry, exists, snapshot)
				}
				if fail {
					return Entry{}, false, cause
				}
				return Entry{Value: "new"}, true, nil
			})
			if fail {
				if !errors.Is(err, cause) {
					t.Fatalf("lost error: %v", err)
				}
				if _, exists := m.load("key"); exists {
					t.Fatal("failed access retained expired entry")
				}
			} else if entry, exists, err := m.Get(context.Background(), "key"); err != nil || !exists || entry.Value != "new" {
				t.Fatalf("expired replacement: %v, %v, %v", entry, exists, err)
			}
		}
	})
}

func TestStoreExpirationDoesNotDeleteConcurrentReplacement(t *testing.T) {
	forEachStore(t, func(t *testing.T, m storeHarness) {
		m.store("key", Entry{Value: "expired", ExpiresAt: 1})
		loaded, resume := make(chan struct{}), make(chan struct{})
		var calls atomic.Int64
		*m.now = func() time.Time {
			if calls.Add(1) == 1 {
				close(loaded)
				<-resume
			}
			return time.UnixMilli(1000)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			entry, exists, err := m.Get(context.Background(), "key")
			if err != nil || !exists || entry.Value != "replacement" {
				t.Errorf("racing read: %v, %v, %v", entry, exists, err)
			}
		}()
		<-loaded
		m.store("key", Entry{Value: "replacement"})
		close(resume)
		<-done
		if entry, exists := m.load("key"); !exists || entry.Value != "replacement" {
			t.Fatalf("replacement deleted: %v, %v", entry, exists)
		}
	})
}

func TestStoreAtomicUpdatesAndErrors(t *testing.T) {
	forEachStore(t, func(t *testing.T, m storeHarness) {
		const workers, iterations = 16, 100
		var calls atomic.Int64
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				for range iterations {
					if err := m.Update(context.Background(), "counter", func(current Entry, exists bool, _ int64) (Entry, bool, error) {
						calls.Add(1)
						value := 0
						if exists {
							value = current.Value.(int)
						}
						return Entry{Value: value + 1}, true, nil
					}); err != nil {
						t.Error(err)
					}
				}
			})
		}
		wg.Wait()
		if calls.Load() != workers*iterations {
			t.Fatalf("update callback calls = %d, want %d", calls.Load(), workers*iterations)
		}
		entry, exists, err := m.Get(context.Background(), "counter")
		if err != nil || !exists || entry.Value != workers*iterations {
			t.Fatalf("counter: %v, %v, %v", entry, exists, err)
		}
		cause := errors.New("abort")
		if err := m.Update(context.Background(), "counter", func(Entry, bool, int64) (Entry, bool, error) {
			return Entry{Value: -1}, true, cause
		}); !errors.Is(err, cause) {
			t.Fatalf("lost update error: %v", err)
		}
		entry, _, _ = m.Get(context.Background(), "counter")
		if entry.Value != workers*iterations {
			t.Fatal("failed update changed live value")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := m.Get(ctx, "counter"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Get cancellation: %v", err)
		}
		if err := m.Update(ctx, "counter", func(Entry, bool, int64) (Entry, bool, error) {
			t.Fatal("canceled update called callback")
			return Entry{}, false, nil
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Update cancellation: %v", err)
		}
	})
}

func TestStoreConcurrentGrowthAndDeletion(t *testing.T) {
	forEachStore(t, func(t *testing.T, m storeHarness) {
		const workers, keys = 16, 256
		var calls atomic.Int64
		var wg sync.WaitGroup
		for worker := range workers {
			wg.Go(func() {
				for i := range keys {
					key := fmt.Sprintf("worker%d\x00key%d\xff", worker, i)
					if err := m.Update(context.Background(), key, func(Entry, bool, int64) (Entry, bool, error) {
						calls.Add(1)
						return Entry{Value: key}, true, nil
					}); err != nil {
						t.Error(err)
						return
					}
					entry, exists, err := m.Get(context.Background(), key)
					if err != nil || !exists || entry.Value != key {
						t.Errorf("concurrent read: %v, %v, %v", entry, exists, err)
						return
					}
					if i%2 == 0 {
						if err := m.Update(context.Background(), key, func(Entry, bool, int64) (Entry, bool, error) {
							return Entry{}, false, nil
						}); err != nil {
							t.Error(err)
							return
						}
					}
				}
			})
		}
		wg.Wait()
		if calls.Load() != workers*keys {
			t.Fatalf("update callback calls = %d, want %d", calls.Load(), workers*keys)
		}
		for worker := range workers {
			for i := range keys {
				key := fmt.Sprintf("worker%d\x00key%d\xff", worker, i)
				entry, exists, err := m.Get(context.Background(), key)
				if err != nil || exists != (i%2 != 0) || exists && entry.Value != key {
					t.Fatalf("after growth/deletion: %q = %v, %v, %v", key, entry, exists, err)
				}
			}
		}
	})
}

func TestUnchangedUpdatePreservesLiveEntriesAndExpiresDeadEntries(t *testing.T) {
	forEachStore(t, func(t *testing.T, m storeHarness) {
		*m.now = func() time.Time { return time.UnixMilli(1000) }
		m.store("live", Entry{Value: "original", ExpiresAt: 2000})
		m.store("expired", Entry{Value: "old", ExpiresAt: 999})
		for _, key := range []string{"live", "expired", "absent"} {
			if err := m.Update(context.Background(), key, func(Entry, bool, int64) (Entry, bool, error) {
				return Entry{Value: "must not publish"}, true, fmt.Errorf("condition: %w", ErrUnchanged)
			}); err != nil {
				t.Fatal(err)
			}
			entry, exists := m.load(key)
			if key == "live" {
				if !exists || entry != (Entry{Value: "original", ExpiresAt: 2000}) {
					t.Fatal(entry, exists)
				}
			} else if exists {
				t.Fatalf("no-op retained or created %s", key)
			}
		}
	})
}
