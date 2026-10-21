package persistence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

func TestSnapshotAOFDoesNotEnableLogging(t *testing.T) {
	for _, preamble := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "rdb"}[preamble], func(t *testing.T) {
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.Save = nil
			config.AOFUseRDBPreamble = preamble
			p, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			db := database.New(storage.New(), p)
			defer db.Close()
			put := func(key, value string) {
				t.Helper()
				if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
					return storage.Entry{Value: value}, true, nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			put("key", "before")
			if err := db.StartRewrite(); err != nil {
				t.Fatal(err)
			}
			// StartRewrite must capture before returning; subsequent writes are neither
			// part of that snapshot nor logged while appendonly remains disabled.
			put("key", "after")
			put("tail", "not logged")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(config.Dir, config.AppendDirname)
			f, err := os.Open(filepath.Join(dir, config.AppendFilename+".manifest"))
			if err != nil {
				t.Fatal(err)
			}
			m, err := readManifest(f)
			f.Close()
			if err != nil || m.Base == nil || len(m.Incr) != 0 {
				t.Fatal(m, err)
			}
			state := make(map[string]aof.String)
			if err := loadSegment(filepath.Join(dir, m.Base.Name), state, true, false); err != nil {
				t.Fatal(err)
			}
			if len(state) != 1 || state["key"].Value != "before" {
				t.Fatal(state)
			}
			// Restarting with appendonly no must still recover from RDB, not this AOF.
			p, err = New(config)
			if err != nil {
				t.Fatal(err)
			}
			restarted := database.New(storage.New(), p)
			defer restarted.Close()
			if _, exists, err := restarted.Get(context.Background(), "key"); err != nil || exists {
				t.Fatal(exists, err)
			}
		})
	}
}

func TestSnapshotAOFPublicationAndOwnership(t *testing.T) {
	dir := t.TempDir()
	name := "appendonly.aof"
	first := map[string]storage.Entry{"old": {Value: "value"}}
	finish, err := prepareSnapshotAOF(context.Background(), dir, name, false, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenMultipartAOF(dir, name, "", "always", false); err == nil {
		t.Fatal("active logger bypassed snapshot ownership")
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name+".manifest")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	aborted, err := prepareSnapshotAOF(ctx, dir, name, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := aborted(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != string(before) {
		t.Fatal("cancellation replaced manifest", err)
	}
	// A later rewrite replaces the full previous dataset without replaying it.
	finish, err = prepareSnapshotAOF(context.Background(), dir, name, true, map[string]storage.Entry{"new": {Value: "replacement"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	log, err := OpenMultipartAOF(dir, name, "", "always", true)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	state := log.Restore()
	if len(state) != 1 || state["new"].Value != "replacement" {
		t.Fatal(state)
	}
}
