package database

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/storage"
)

type expirationBackend struct {
	storage.Backend
	loads int
	delay time.Duration
}

func (b *expirationBackend) Load(key string) (storage.Entry, bool) {
	b.loads++
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	return b.Backend.Load(key)
}

// Manual cycles share production indexing and mutation paths but no timer.
func manualExpirationDB(t *testing.T, store storage.Backend, p Persistence) *DB {
	t.Helper()
	db := New(store, p, WithClock(func() time.Time { return time.UnixMilli(1000) }))
	WithActiveExpiration(10, false)(db)
	for key, entry := range store.Snapshot() {
		db.trackExpiration(key, entry)
	}
	close(db.expiration.done)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func assertExpirationIndex(t *testing.T, db *DB) {
	t.Helper()
	index := db.expiration
	want := 0
	for key, entry := range db.store.Snapshot() {
		if entry.ExpiresAt != 0 {
			want++
			position, exists := index.positions[key]
			if !exists || position >= len(index.keys) || index.keys[position] != key {
				t.Fatalf("missing or invalid index entry for %q", key)
			}
		} else if _, exists := index.positions[key]; exists {
			t.Fatalf("persistent key %q in expiration index", key)
		}
	}
	if len(index.keys) != want || len(index.positions) != want {
		t.Fatalf("index lengths = %d/%d, want %d", len(index.keys), len(index.positions), want)
	}
}

func TestExpirationSamplingThreshold(t *testing.T) {
	for _, tc := range []struct{ expired, loads int }{{0, 20}, {4, 20}, {5, 20}, {6, 34}, {20, 20}} {
		t.Run(fmt.Sprint(tc.expired), func(t *testing.T) {
			store := &expirationBackend{Backend: storage.New()}
			for i := range 20 {
				deadline := int64(2000)
				if i < tc.expired {
					deadline = 999
				}
				store.Put(fmt.Sprint(i), storage.Entry{Value: "value", ExpiresAt: deadline})
			}
			store.Put("persistent", storage.Entry{Value: "value"})
			db := manualExpirationDB(t, store, nil)
			hz, err := db.expireCycle(10)
			if err != nil || hz != 10 || store.loads != tc.loads {
				t.Fatalf("cycle = %v, %v, loads=%d; want %d", hz, err, store.loads, tc.loads)
			}
			if len(store.Snapshot()) != 21-tc.expired || db.dirty != uint64(tc.expired) {
				t.Fatal("incorrect deletion count")
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestExpirationRepeatsAndSamplesWithoutReplacement(t *testing.T) {
	for _, backend := range []storage.Backend{storage.NewXsync(), storage.NewDLHT()} {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			for i := range 201 {
				backend.Put(fmt.Sprint(i), storage.Entry{Value: "value", ExpiresAt: 999})
			}
			db := manualExpirationDB(t, backend, nil)
			for range 10 {
				keys := db.expiration.sample()
				seen := make(map[string]bool)
				for _, key := range keys {
					if seen[key] {
						t.Fatal("duplicate sample")
					}
					seen[key] = true
				}
				if len(keys) != expirationSampleSize {
					t.Fatal(len(keys))
				}
				assertExpirationIndex(t, db)
			}
			if _, err := db.expireCycle(10); err != nil {
				t.Fatal(err)
			}
			if len(backend.Snapshot()) != 0 || db.dirty != 201 {
				t.Fatal("high deletion ratio did not repeat")
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestExpirationIndexTracksMutationsAndBoundary(t *testing.T) {
	for _, backend := range []storage.Backend{storage.NewXsync(), storage.NewDLHT()} {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			db := manualExpirationDB(t, backend, nil)
			key := "binary\x00\xff"
			update := func(expires int64, keep bool, failure error) {
				t.Helper()
				err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
					return storage.Entry{Value: "new", ExpiresAt: expires}, keep, failure
				})
				if errors.Is(failure, storage.ErrUnchanged) {
					failure = nil
				}
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				assertExpirationIndex(t, db)
			}
			update(1000, true, nil)
			if _, err := db.expireCycle(10); err != nil {
				t.Fatal(err)
			}
			if _, exists := backend.Load(key); !exists {
				t.Fatal("deleted at deadline instead of strictly after it")
			}
			update(2000, true, nil)
			update(0, false, storage.ErrUnchanged)
			update(0, false, errors.New("callback failed"))
			update(0, true, nil)
			if len(db.expiration.keys) != 0 {
				t.Fatal("replacing TTL retained index entry")
			}
			update(2000, true, nil)
			update(0, false, nil)
			update(1000, true, nil)
			db.now = func() time.Time { return time.UnixMilli(1001) }
			if _, ok, err := db.Get(context.Background(), key); ok || err != nil {
				t.Fatal("passive expiration failed", ok, err)
			}
			assertExpirationIndex(t, db)
		})
	}
}

type expirationPersistence struct {
	checkpointPersistence
	restored  map[string]storage.Entry
	appendErr error
	onDelete  func(string)
	deleted   []string
}

func (p *expirationPersistence) Restore() map[string]storage.Entry { return p.restored }
func (p *expirationPersistence) AppendDelete(keys []string) error {
	if p.appendErr != nil {
		return p.appendErr
	}
	for _, key := range keys {
		if p.onDelete != nil {
			p.onDelete(key)
		}
	}
	p.deleted = append(p.deleted, keys...)
	return nil
}
func (p *expirationPersistence) Append(key string, _ storage.Entry, keep bool) error {
	if p.appendErr != nil {
		return p.appendErr
	}
	if !keep {
		if p.onDelete != nil {
			p.onDelete(key)
		}
		p.deleted = append(p.deleted, key)
	}
	return nil
}

func TestExpirationRecoveryAndDurableFailure(t *testing.T) {
	store := storage.New()
	p := &expirationPersistence{restored: map[string]storage.Entry{
		"old":        {Value: "skip", ExpiresAt: 999},
		"ttl":        {Value: "restored", ExpiresAt: 1001},
		"persistent": {Value: "keep"},
	}}
	db := manualExpirationDB(t, store, p)
	assertExpirationIndex(t, db)
	if len(db.expiration.keys) != 1 {
		t.Fatal("restored TTLs not indexed")
	}
	db.now = func() time.Time { return time.UnixMilli(1002) }
	failure := errors.New("disk failed")
	p.appendErr = failure
	if _, err := db.expireCycle(10); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, exists := store.Load("ttl"); !exists || db.dirty != 0 || len(p.deleted) != 0 {
		t.Fatal("failed append published deletion")
	}
	assertExpirationIndex(t, db)
	p.appendErr = nil
	p.onDelete = func(key string) {
		if _, exists := store.Load(key); !exists {
			t.Error("memory deletion preceded durable append")
		}
	}
	if _, err := db.expireCycle(10); err != nil {
		t.Fatal(err)
	}
	if len(p.deleted) != 1 || p.deleted[0] != "ttl" || db.dirty != 1 {
		t.Fatal("durable expiration missing", p.deleted, db.dirty)
	}
	assertExpirationIndex(t, db)
}

func TestExpirationBudgetAndBusyGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &expirationBackend{Backend: storage.New(), delay: time.Millisecond}
		for i := range 100 {
			store.Put(fmt.Sprint(i), storage.Entry{Value: "value", ExpiresAt: 999})
		}
		db := manualExpirationDB(t, store, nil)
		start := time.Now()
		if _, err := db.expireCycle(10); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != expirationBudget || store.loads != 25 {
			t.Fatalf("budget = %v, loads=%d", time.Since(start), store.loads)
		}
		assertExpirationIndex(t, db)
		db.mu.Lock()
		start = time.Now()
		_, err := db.expireCycle(10)
		db.mu.Unlock()
		if err != nil || time.Since(start) != 0 {
			t.Fatal("busy gate blocked cleanup", err)
		}
	})
}

func TestDynamicExpirationHZ(t *testing.T) {
	for _, baseline := range []float64{1, 10, 500} {
		for _, count := range []int{0, 1, 20, 1000, 1000000, math.MaxInt} {
			previous := baseline * 2
			for _, distance := range []float64{0, 1, 1000, 60000, math.MaxInt64} {
				hz := expirationHZ(baseline, count, distance)
				if hz < baseline/2 || hz > baseline*2 || hz > previous {
					t.Fatalf("bounds or distance monotonicity: %v/%v/%v: %v", baseline, count, distance, hz)
				}
				if hz != expirationHZ(baseline, count, -distance) {
					t.Fatal("distance must include overdue deadlines")
				}
				previous = hz
			}
		}
		previous := 0.0
		for _, count := range []int{0, 1, 20, 1000, 1000000} {
			hz := expirationHZ(baseline, count, 0)
			if hz <= previous {
				t.Fatal("frequency did not increase with TTL population")
			}
			previous = hz
		}
	}
	if hz := expirationHZ(10, 1000, 1000); hz != 8.75 {
		t.Fatal(hz)
	}
	if got := medianDistance([]int64{200, 100, 900, 300}, 100); got != 150 {
		t.Fatal("even median", got)
	}
	if got := medianDistance([]int64{900, 100, 200}, 100); got != 100 {
		t.Fatal("odd median", got)
	}
}

func TestExpirationWorkerFrequencyAndClose(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		t.Run(fmt.Sprint(dynamic), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := storage.New()
				db := New(store, nil, WithActiveExpiration(1, dynamic))
				defer db.Close()
				synctest.Wait()
				// The first empty tick changes dynamic frequency to 0.5 Hz.
				time.Sleep(time.Second)
				synctest.Wait()
				if err := db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
					return storage.Entry{Value: "value", ExpiresAt: time.Now().Add(time.Millisecond).UnixMilli()}, true, nil
				}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if _, exists := store.Load("key"); exists != dynamic {
					t.Fatal("worker ignored fixed/dynamic interval", exists)
				}
				if dynamic {
					time.Sleep(time.Second)
					synctest.Wait()
					if _, exists := store.Load("key"); exists {
						t.Fatal("unread expired key not reclaimed")
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-db.expiration.done:
				default:
					t.Fatal("Close did not join expiration worker")
				}
			})
		})
	}
}

func TestExpirationConcurrentReplacement(t *testing.T) {
	for _, backend := range []storage.Backend{storage.NewXsync(), storage.NewDLHT()} {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			db := manualExpirationDB(t, backend, nil)
			var clock atomic.Int64
			clock.Store(1000)
			db.now = func() time.Time { return time.UnixMilli(clock.Add(1)) }
			var wg sync.WaitGroup
			for worker := range 4 {
				wg.Go(func() {
					key := fmt.Sprint(worker)
					for range 100 {
						if err := db.Update(context.Background(), key, func(_ storage.Entry, _ bool, now int64) (storage.Entry, bool, error) {
							return storage.Entry{Value: "old", ExpiresAt: now}, true, nil
						}); err != nil {
							t.Error(err)
						}
						put(t, db, key, "replacement")
					}
				})
			}
			wg.Go(func() {
				for range 100 {
					if _, err := db.expireCycle(10); err != nil {
						t.Error(err)
					}
				}
			})
			wg.Wait()
			for worker := range 4 {
				if entry, ok := backend.Load(fmt.Sprint(worker)); !ok || entry.Value != "replacement" {
					t.Fatal("replacement lost", entry, ok)
				}
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestExpirationCycleUsesSampleMedian(t *testing.T) {
	store := storage.New()
	for i := range 20 {
		deadline := int64(1001)
		if i >= 10 {
			deadline = 3001
		}
		store.Put(fmt.Sprint(i), storage.Entry{Value: "value", ExpiresAt: deadline})
	}
	db := manualExpirationDB(t, store, nil)
	db.expiration.dynamic = true
	got, err := db.expireCycle(10)
	if want := expirationHZ(10, 20, 1001); err != nil || got != want {
		t.Fatalf("cycle hz=%v, want %v, err=%v", got, want, err)
	}
}

func TestExpirationCloseWaitsForInFlightAppend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		store := storage.New()
		for _, key := range []string{"one", "two"} {
			store.Put(key, storage.Entry{Value: "value", ExpiresAt: 1})
		}
		p := &expirationPersistence{onDelete: func(string) { close(entered); <-release }}
		db := New(store, p, WithActiveExpiration(10, false))
		<-entered
		closed := make(chan error, 1)
		go func() { closed <- db.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatal("Close did not wait for append", err)
		default:
		}
		close(release)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if len(p.deleted) != 1 || len(store.Snapshot()) != 1 {
			t.Fatal("worker started another deletion after shutdown")
		}
	})
}

func TestExpirationTriggersSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.New()
		store.Put("key", storage.Entry{Value: "value", ExpiresAt: time.Now().Add(time.Millisecond).UnixMilli()})
		p := &checkpointPersistence{rules: []data.SaveRule{{Seconds: 1, Changes: 1}}}
		db := New(store, p, WithActiveExpiration(10, false))
		defer db.Close()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if len(p.snapshots) != 1 || len(p.snapshots[0]) != 0 {
			t.Fatal("active deletion did not trigger snapshot", p.snapshots)
		}
	})
}

func TestActiveExpirationContinuesAfterAOFFailure(t *testing.T) {
	for name, backend := range map[string]storage.Backend{"xsync": storage.NewXsync(), "dlht": storage.NewDLHT()} {
		t.Run(name, func(t *testing.T) {
			p := &expirationPersistence{}
			db := manualExpirationDB(t, backend, p)
			for i := range 45 {
				key := fmt.Sprint(i)
				entry := storage.Entry{Value: "expired", ExpiresAt: 999}
				backend.Put(key, entry)
				db.trackExpiration(key, entry)
			}
			backend.Put("live", storage.Entry{Value: "value"})
			failure := errors.Join(ErrAOFFailed, errors.New("disk failed"))
			p.appendErr, p.writeErr = failure, failure
			if _, err := db.expireCycle(10); err != nil {
				t.Fatal(err)
			}
			if len(backend.Snapshot()) != 1 || db.dirty != 45 || len(db.expiration.keys) != 0 {
				t.Fatalf("expired entries retained: %d, dirty=%d, indexed=%d", len(backend.Snapshot()), db.dirty, len(db.expiration.keys))
			}
			assertExpirationIndex(t, db)
			if !errors.Is(db.WriteError(), failure) {
				t.Fatal("expiration cleared write protection")
			}
			if err := db.Update(context.Background(), "live", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
				t.Fatal("write callback after AOF failure")
				return storage.Entry{}, false, nil
			}); !errors.Is(err, failure) {
				t.Fatal(err)
			}
		})
	}
}
