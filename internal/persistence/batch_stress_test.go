package persistence

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/storage"
)

// Exercise map growth/shrink and buffer reuse, rather than only overwrites in a
// prefilled map. A rewrite captures the large values before concurrent deletes.
func TestAOFConcurrentGrowthDeletionAndLargeValues(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			config := data.DefaultConfig()
			config.Dir, config.AppendOnly, config.AppendFsync, config.Save = t.TempDir(), true, policy, nil
			p, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			backend := storage.New()
			db := database.New(backend, p, database.WithActiveExpiration(10, false))
			defer db.Close()
			const workers, keysPerWorker = 8, 128
			for cycle := range 3 {
				var wg sync.WaitGroup
				for worker := range workers {
					wg.Go(func() {
						for i := range keysPerWorker {
							key := fmt.Sprintf("%d/%d/%d\x00\xff", cycle, worker, i)
							value := key + "\r\n"
							if i == 0 {
								value += strings.Repeat("\xff\x00\r\n", 20000)
							}
							entry := storage.Entry{Value: value}
							if i%2 != 0 {
								entry.ExpiresAt = 4102444800000
							}
							if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
								return entry, true, nil
							}); err != nil {
								t.Error(err)
								return
							}
							if got, ok, err := db.Get(context.Background(), key); err != nil || !ok || got != entry {
								t.Errorf("changed acknowledged value: %#v, %v, %v", got, ok, err)
								return
							}
						}
					})
				}
				wg.Wait()
				if t.Failed() {
					return
				}
				if cycle == 0 {
					if err := db.StartRewrite(); err != nil {
						t.Fatal(err)
					}
				}
				for worker := range workers {
					wg.Go(func() {
						for i := range keysPerWorker {
							key := fmt.Sprintf("%d/%d/%d\x00\xff", cycle, worker, i)
							// Compute deletion also exercises the backend's shrink path.
							if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
								return storage.Entry{}, false, nil
							}); err != nil {
								t.Error(err)
								return
							}
						}
						if err := db.Update(context.Background(), "counter", func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
							n := 0
							if exists {
								n, _ = strconv.Atoi(e.Value.(string))
							}
							return storage.Entry{Value: strconv.Itoa(n + 1)}, true, nil
						}); err != nil {
							t.Error(err)
						}
					})
				}
				wg.Wait()
			}
			want := map[string]storage.Entry{"counter": {Value: strconv.Itoa(workers * 3)}}
			if got := backend.Snapshot(); !reflect.DeepEqual(got, want) {
				t.Fatalf("live state: %#v, want %#v", got, want)
			}
			log := p.(*Engine).aof.AOF
			log.mu.Lock()
			retained := cap(log.pending)
			log.mu.Unlock()
			if retained > 64<<10 {
				t.Fatalf("retained an oversized append buffer: %d", retained)
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
				t.Fatalf("recovered state: %#v, want %#v", got, want)
			}
		})
	}
}
