package server

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/storage"
)

func TestRedisLoadsConcurrentAOFAppends(t *testing.T) {
	if os.Getenv("REDIS_REFERENCE") == "" {
		t.Skip("set REDIS_REFERENCE to a Redis 8.10.2 executable")
	}
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.AppendOnly, config.AppendFsync, config.Save = true, policy, nil
			p, err := persistence.New(config)
			if err != nil {
				t.Fatal(err)
			}
			store := storage.New()
			db := database.New(store, p)
			defer db.Close()
			const workers, updates = 16, 20
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Go(func() {
					for range updates {
						if err := db.Update(context.Background(), "shared", func(e storage.Entry, exists bool, _ int64) (storage.Entry, bool, error) {
							n := 0
							if exists {
								n, _ = strconv.Atoi(e.Value.(string))
							}
							return storage.Entry{Value: strconv.Itoa(n + 1)}, true, nil
						}); err != nil {
							t.Error(err)
							return
						}
						if err := db.Update(context.Background(), fmt.Sprintf("key\x00\xff%d", worker), func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
							return storage.Entry{Value: "\xff\r\n\x00", ExpiresAt: 4102444800000}, true, nil
						}); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			if err := db.StartRewrite(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			want := store.Snapshot()
			if want["shared"].Value != strconv.Itoa(workers*updates) {
				t.Fatal("lost concurrent mutations", want)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			ref := startRedisFiles(t, persistenceFiles(t, config.Dir, true), "--appendfsync", policy)
			assertReferenceState(t, ref, want)
			if got := exchange(t, ref.conn, ref.reader, 1, "DBSIZE"); got != fmt.Sprintf(":%d\r\n", len(want)) {
				t.Fatal("Redis recovered a different key count", got)
			}
		})
	}
}
