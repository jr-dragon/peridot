package database

import (
	"context"
	"fmt"
	"hash/maphash"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/jr-dragon/peridot/internal/storage"
)

type snapshotOnlyPersistence struct{ checkpointPersistence }

func (*snapshotOnlyPersistence) RequiresOrderedMutations() bool { return false }

func TestIndependentKeyUpdatesAndSnapshotBoundary(t *testing.T) {
	for name, factory := range map[string]func() storage.Backend{
		"xsync": func() storage.Backend { return storage.NewXsync() },
		"dlht":  func() storage.Backend { return storage.NewDLHT() },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &snapshotOnlyPersistence{checkpointPersistence: checkpointPersistence{
					entered: make(chan struct{}, 1), release: make(chan struct{}),
				}}
				db := New(factory(), p)
				defer db.Close()
				// Lock stripes may collide. Select two independent stripes so the
				// test requires concurrency without assuming collision-free keys.
				keys := []string{"first"}
				for i := 0; ; i++ {
					key := fmt.Sprint(i)
					if maphash.String(db.seed, key)%uint64(len(db.keys)) != maphash.String(db.seed, keys[0])%uint64(len(db.keys)) {
						keys = append(keys, key)
						break
					}
				}
				entered, release := make(chan string, 2), make(chan struct{})
				updated := make(chan error, 2)
				for _, key := range keys {
					go func() {
						updated <- db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
							entered <- key
							<-release
							return storage.Entry{Value: key}, true, nil
						})
					}()
				}
				// A global mutation lock would deadlock before both callbacks enter.
				<-entered
				<-entered
				started := make(chan error, 1)
				requested := make(chan struct{})
				go func() { close(requested); started <- db.StartSave() }()
				<-requested
				select {
				case err := <-started:
					t.Fatal("snapshot crossed an unfinished mutation", err)
				default:
				}
				close(release)
				for range keys {
					if err := <-updated; err != nil {
						t.Fatal(err)
					}
				}
				if err := <-started; err != nil {
					t.Fatal(err)
				}
				<-p.entered
				put(t, db, "tail", "after capture")
				close(p.release)
				synctest.Wait()
				if len(p.snapshots) != 1 || len(p.snapshots[0]) != len(keys) {
					t.Fatalf("snapshot boundary: %v", p.snapshots)
				}
				for _, key := range keys {
					if p.snapshots[0][key].Value != key {
						t.Fatalf("snapshot missed completed mutation %q", key)
					}
				}
				if db.dirty.Load() != 1 {
					t.Fatal("snapshot discarded a later mutation's dirty count")
				}
			})
		})
	}
}

func TestConcurrentSnapshotOnlyUpdatesKeepTTLIndexAndDirtyCount(t *testing.T) {
	for name, factory := range map[string]func() storage.Backend{
		"xsync": func() storage.Backend { return storage.NewXsync() },
		"dlht":  func() storage.Backend { return storage.NewDLHT() },
	} {
		t.Run(name, func(t *testing.T) {
			db := manualExpirationDB(t, factory(), &snapshotOnlyPersistence{})
			const workers, iterations = 8, 100
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Go(func() {
					for i := range iterations {
						for _, key := range []string{"counter", fmt.Sprint(worker)} {
							if err := db.Update(context.Background(), key, func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
								n := 0
								if exists {
									n = e.Value.(int)
								}
								deadline := int64(0)
								if i%2 != 0 {
									deadline = 2000
								}
								return storage.Entry{Value: n + 1, ExpiresAt: deadline}, true, nil
							}); err != nil {
								t.Error(err)
								return
							}
						}
					}
				})
			}
			wg.Wait()
			entry, exists, err := db.Get(context.Background(), "counter")
			if err != nil || !exists || entry.Value != workers*iterations {
				t.Fatalf("non-atomic counter: %v, %v, %v", entry, exists, err)
			}
			if db.dirty.Load() != workers*iterations*2 {
				t.Fatalf("lost dirty increments: %d", db.dirty.Load())
			}
			assertExpirationIndex(t, db)
		})
	}
}

type pausingAppendPersistence struct {
	checkpointPersistence
	entered chan string
	release chan struct{}
}

func (p *pausingAppendPersistence) Append(key string, _ storage.Entry, _ bool) error {
	p.entered <- key
	<-p.release
	return nil
}

func TestOrderedAppendPrecedesPublicationAndNextMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &pausingAppendPersistence{entered: make(chan string, 2), release: make(chan struct{})}
		backend := storage.New()
		db := New(backend, p)
		defer db.Close()
		finished := make(chan error, 2)
		update := func(key string) {
			finished <- db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
				return storage.Entry{Value: key}, true, nil
			})
		}
		go update("first")
		if key := <-p.entered; key != "first" {
			t.Fatal(key)
		}
		requested := make(chan struct{})
		go func() { close(requested); update("second") }()
		<-requested
		select {
		case key := <-p.entered:
			t.Fatal("append reordered before prior publication", key)
		default:
		}
		if len(backend.Snapshot()) != 0 {
			t.Fatal("memory publication preceded append completion")
		}
		close(p.release)
		for range 2 {
			if err := <-finished; err != nil {
				t.Fatal(err)
			}
		}
		if key := <-p.entered; key != "second" {
			t.Fatal(key)
		}
		if len(backend.Snapshot()) != 2 {
			t.Fatal("completed mutations not published")
		}
	})
}
