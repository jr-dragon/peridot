package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func persistenceFiles(t *testing.T, dir string, multipart bool) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte)
	if !multipart {
		b, err := os.ReadFile(filepath.Join(dir, "dump.rdb"))
		if err != nil {
			t.Fatal(err)
		}
		result["dump.rdb"] = b
		return result
	}
	// Copy only the active manifest and referenced files, never flatten segments.
	path := filepath.Join("appendonlydir", "appendonly.aof.manifest")
	b, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		t.Fatal(err)
	}
	result[path] = b
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		words, err := data.SplitArguments(line)
		if err != nil {
			t.Fatal(err)
		}
		if words[5] == "h" {
			continue
		}
		name := filepath.Join("appendonlydir", words[1])
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		result[name] = b
	}
	return result
}
func putPersistenceFiles(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for name, b := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func persistenceFixture() map[string]storage.Entry {
	return map[string]storage.Entry{
		"": {Value: ""}, "key\x00\xff": {Value: "\x00\r\n\xff"}, "long": {Value: strings.Repeat("compressible-", 10000)},
		"int8": {Value: "-12"}, "int16": {Value: "12345"}, "int32": {Value: "-123456789"}, "int64": {Value: "9223372036854775807"},
		"future": {Value: "ttl", ExpiresAt: 4102444800000}, "overwritten": {Value: "original"},
	}
}
func assertReferenceState(t *testing.T, ref redisAOFReference, want map[string]storage.Entry) {
	t.Helper()
	for k, e := range want {
		if got := exchange(t, ref.conn, ref.reader, 1, "GET", k); got != string(resp.NewBulkString(e.Value.(string)).Marshal()) {
			t.Fatalf("Redis GET %q mismatch: %q", k, got)
		}
		deadline := e.ExpiresAt
		if deadline == 0 {
			deadline = -1
		}
		if got := exchange(t, ref.conn, ref.reader, 1, "PEXPIRETIME", k); got != fmt.Sprintf(":%d\r\n", deadline) {
			t.Fatalf("Redis expiration %q: %q", k, got)
		}
	}
}
func assertDatabaseState(t *testing.T, db *database.DB, want map[string]storage.Entry) {
	t.Helper()
	for k, e := range want {
		got, ok, err := db.Get(context.Background(), k)
		if err != nil || !ok || got != e {
			t.Fatalf("Peridot entry %q: %#v, %v, %v; want %#v", k, got, ok, err, e)
		}
	}
}
func openPersistenceDB(t *testing.T, config data.Config, newStore func() storage.Backend) *database.DB {
	t.Helper()
	p, err := persistence.New(config)
	if err != nil {
		t.Fatal(err)
	}
	db := database.New(newStore(), p)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func referenceRewrite(t *testing.T, ref redisAOFReference) {
	t.Helper()
	before, err := os.ReadFile(filepath.Join(ref.dir, "appendonlydir", "appendonly.aof.manifest"))
	if err != nil {
		t.Fatal(err)
	}
	if got := exchange(t, ref.conn, ref.reader, 1, "BGREWRITEAOF"); got != "+Background append only file rewriting started\r\n" {
		t.Fatal(got)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		info := exchange(t, ref.conn, ref.reader, 1, "INFO", "persistence")
		after, _ := os.ReadFile(filepath.Join(ref.dir, "appendonlydir", "appendonly.aof.manifest"))
		if string(after) != string(before) && strings.Contains(info, "aof_rewrite_in_progress:0\r\n") && strings.Contains(info, "aof_last_bgrewrite_status:ok\r\n") {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("reference rewrite timed out: %s", info)
		}
	}
}

// These tests pass complete Redis-produced artifact sets in both directions.
func TestPersistenceRedisBidirectional(t *testing.T) {
	for backend, newStore := range map[string]func() storage.Backend{"xsync": func() storage.Backend { return storage.NewXsync() }, "dlht": func() storage.Backend { return storage.NewDLHT() }} {
		for _, mode := range []string{"rdb", "multipart-rdb", "multipart-plain"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				multipart := mode != "rdb"
				preamble := mode != "multipart-plain"
				args := []string{"--appendonly", map[bool]string{true: "yes", false: "no"}[multipart], "--aof-use-rdb-preamble", map[bool]string{true: "yes", false: "no"}[preamble]}
				ref := startRedisFiles(t, nil, args...)
				want := persistenceFixture()
				for k, e := range want {
					command := []string{"SET", k, e.Value.(string)}
					if e.ExpiresAt != 0 {
						command = append(command, "PXAT", fmt.Sprint(e.ExpiresAt))
					}
					if got := exchange(t, ref.conn, ref.reader, 1, command...); got != "+OK\r\n" {
						t.Fatal(got)
					}
				}
				if multipart {
					referenceRewrite(t, ref)
					for _, command := range [][]string{{"SET", "overwritten", "after-base"}, {"SET", "tail", "incremental"}, {"SET", "gone", "temporary"}, {"DEL", "gone"}, {"PERSIST", "future"}} {
						if got := exchange(t, ref.conn, ref.reader, 1, command...); strings.HasPrefix(got, "-") {
							t.Fatal(got)
						}
					}
					want["overwritten"] = storage.Entry{Value: "after-base"}
					want["tail"] = storage.Entry{Value: "incremental"}
					want["future"] = storage.Entry{Value: "ttl"}
				} else if got := exchange(t, ref.conn, ref.reader, 1, "SAVE"); got != "+OK\r\n" {
					t.Fatal(got)
				}
				ref.stop()
				config := data.DefaultConfig()
				config.Dir = t.TempDir()
				config.Save = nil
				config.AppendOnly = multipart
				config.AOFUseRDBPreamble = preamble
				config.AppendFsync = "always"
				putPersistenceFiles(t, config.Dir, persistenceFiles(t, ref.dir, multipart))
				// AOF must take precedence over a corrupt standalone snapshot.
				if multipart {
					if err := os.WriteFile(filepath.Join(config.Dir, "dump.rdb"), []byte("not an RDB"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				db := openPersistenceDB(t, config, newStore)
				assertDatabaseState(t, db, want)
				if _, ok, err := db.Get(context.Background(), "gone"); err != nil || ok {
					t.Fatal("deleted key returned", err)
				}
				_, conn := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
				reader := resp.NewReader(conn)
				if got := exchange(t, conn, reader, 1, "SET", "peridot", "exported", "PXAT", "4102444900000"); got != "+OK\r\n" {
					t.Fatal(got)
				}
				want["peridot"] = storage.Entry{Value: "exported", ExpiresAt: 4102444900000}
				if multipart {
					if err := db.Rewrite(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else if got := exchange(t, conn, reader, 1, "SAVE"); got != "+OK\r\n" {
					t.Fatal(got)
				}
				if got := exchange(t, conn, reader, 1, "LASTSAVE"); !strings.HasPrefix(got, ":") {
					t.Fatal(got)
				}
				if multipart {
					if got := exchange(t, conn, reader, 1, "SET", "after-rewrite", "durable"); got != "+OK\r\n" {
						t.Fatal(got)
					}
					want["after-rewrite"] = storage.Entry{Value: "durable"}
				}
				conn.Close()
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				restarted := openPersistenceDB(t, config, newStore)
				assertDatabaseState(t, restarted, want)
				if err := restarted.Close(); err != nil {
					t.Fatal(err)
				}
				exported := persistenceFiles(t, config.Dir, multipart)
				consumer := startRedisFiles(t, exported, args...)
				assertReferenceState(t, consumer, want)
				if multipart {
					referenceRewrite(t, consumer)
				} else if got := exchange(t, consumer.conn, consumer.reader, 1, "SAVE"); got != "+OK\r\n" {
					t.Fatal(got)
				}
				consumer.stop()
				// Redis reserializes the imported data, then Peridot loads it again.
				config.Dir = t.TempDir()
				putPersistenceFiles(t, config.Dir, persistenceFiles(t, consumer.dir, multipart))
				final := openPersistenceDB(t, config, newStore)
				assertDatabaseState(t, final, want)
			})
		}
	}
}

func TestRedisLoadsMultipartAtUnfinishedRewriteBoundary(t *testing.T) {
	for _, preamble := range []bool{true, false} {
		t.Run(fmt.Sprint("rdb-base=", preamble), func(t *testing.T) {
			// Keep the old base + old increment + newly rotated increment authoritative.
			dir := t.TempDir()
			s, err := persistence.OpenMultipartAOF(filepath.Join(dir, "appendonlydir"), "appendonly.aof", "", "always", preamble)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			original := storage.Entry{Value: "before", ExpiresAt: 4102444800000}
			if err := s.Append("key", original, true); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finish, err := s.BeginRewrite(ctx, map[string]storage.Entry{"key": original})
			if err != nil {
				t.Fatal(err)
			}
			updated := storage.Entry{Value: "during\x00\xff"}
			if err := s.Append("key", updated, true); err != nil {
				t.Fatal(err)
			}
			// Copy the complete file set while no writer is mutating it. No base has
			// been produced for this rewrite yet; recovery must use both increments.
			files := persistenceFiles(t, dir, true)
			ref := startRedisFiles(t, files, "--aof-use-rdb-preamble", map[bool]string{true: "yes", false: "no"}[preamble])
			assertReferenceState(t, ref, map[string]storage.Entry{"key": updated})
			cancel()
			if err := finish(); err != context.Canceled {
				t.Fatal(err)
			}
		})
	}
}
