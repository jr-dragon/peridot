package persistence_test

import (
	"context"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/storage"
)

func TestPersistencePreservesSelectedBackend(t *testing.T) {
	for name, newStore := range map[string]func() storage.Backend{
		"xsync": func() storage.Backend { return storage.NewXsync() },
		"dlht":  func() storage.Backend { return storage.NewDLHT() },
	} {
		t.Run(name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				t.Run(map[bool]string{false: "memory", true: "aof"}[enabled], func(t *testing.T) {
					config := data.Config{}
					if enabled {
						config.AppendOnly = true
						config.Dir, config.AppendFilename = t.TempDir(), "data.aof"
					}
					store := newStore()
					p, err := persistence.New(config)
					if err != nil {
						t.Fatal(err)
					}
					if (p != nil) != enabled {
						t.Fatalf("persistence = %v, enabled = %v", p, enabled)
					}
					db := database.New(store, p)
					defer db.Close()
					if err := db.Update(context.Background(), "key", func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
						return storage.Entry{Value: "value"}, true, nil
					}); err != nil {
						t.Fatal(err)
					}
					if e, ok := store.Load("key"); !ok || e.Value != "value" {
						t.Fatal("persistence replaced the selected backend")
					}
				})
			}
		})
	}
}
