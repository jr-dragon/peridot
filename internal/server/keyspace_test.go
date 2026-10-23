package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func TestKeyspacePipeline(t *testing.T) {
	forEachStringStore(t, func(t *testing.T, newStore func() storage.KeyStore) {
		for _, protocol := range []string{"2", "3"} {
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(newStore(), nil)))
			reader := resp.NewReader(peer)
			exchange(t, peer, reader, 1, "HELLO", protocol)
			null := "$-1\r\n"
			if protocol == "3" {
				null = "_\r\n"
			}
			commands := [][]string{
				{"select", "0"}, {"SET", "key\x00\xff", "Hello world"}, {"DiGeSt", "key\x00\xff"},
				{"TTL", "key\x00\xff"}, {"EXPIRE", "key\x00\xff", "100", "NX"}, {"TTL", "key\x00\xff"},
				{"EXPIRE", "key\x00\xff", "bad"}, {"DEL", "key\x00\xff", "key\x00\xff", "missing"},
				{"TTL", "key\x00\xff"}, {"DIGEST", "key\x00\xff"}, {"GET", "key\x00\xff"}, {"PING"},
			}
			want := "-ERR SELECT is not allowed: Peridot does not support multiple databases\r\n+OK\r\n$16\r\nb6acb9d84a38ff74\r\n:-1\r\n:1\r\n:100\r\n-ERR value is not an integer or out of range\r\n:1\r\n:-2\r\n" + null + null + "+PONG\r\n"
			persistencePipeline(t, peer, want, commands...)
		}
	})
}

func TestKeyspaceAgainstRedis(t *testing.T) {
	forEachStringStore(t, func(t *testing.T, newStore func() storage.KeyStore) {
		for _, protocol := range []string{"2", "3"} {
			t.Run("RESP"+protocol, func(t *testing.T) {
				ref := startRedisFiles(t, nil, "--appendonly", "no")
				store := newStore()
				_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(store, nil)))
				reader := resp.NewReader(peer)
				exchange(t, ref.conn, ref.reader, 1, "HELLO", protocol)
				exchange(t, peer, reader, 1, "HELLO", protocol)
				count := 0
				compare := func(args ...string) {
					t.Helper()
					want := exchange(t, ref.conn, ref.reader, 1, args...)
					got := exchange(t, peer, reader, 1, args...)
					count++
					if got != want {
						t.Fatalf("%q: Peridot=%q Redis=%q", args, got, want)
					}
				}
				for _, name := range []string{"DEL", "TTL", "EXPIRE", "DIGEST"} {
					compare(name)
				}
				compare("TTL", "missing", "extra")
				compare("DIGEST", "missing", "extra")
				compare("EXPIRE", "missing")
				for _, value := range []string{"", "Hello world", "123", "00123", "-9223372036854775808", "\x00\r\n\xff", strings.Repeat("x", 17), strings.Repeat("x", 129), strings.Repeat("x", 241), strings.Repeat("x", 1025), strings.Repeat("x", 4096)} {
					compare("SET", "binary\x00\xff", value)
					compare("DIGEST", "binary\x00\xff")
					compare("TTL", "binary\x00\xff")
					compare("DEL", "binary\x00\xff", "binary\x00\xff", "missing")
					compare("DIGEST", "binary\x00\xff")
				}
				for _, seconds := range []string{"bad", "+1", "01", "-0", "1.0", " 1", "1\x00", "9223372036854775808", "9223372036854775807", "-9223372036854775808", "9223372036854775", "-9223372036854775", "0", "-1", "100"} {
					for _, key := range []string{"key", "missing"} {
						compare("SET", "key", "value")
						compare("EXPIRE", key, seconds)
						compare("GET", key)
					}
				}
				for _, options := range [][]string{nil, {"NX"}, {"XX"}, {"GT"}, {"LT"}, {"XX", "GT"}, {"XX", "LT"}, {"NX", "NX"}, {"gT", "GT"}, {"LT", "LT"}, {"NX", "XX"}, {"NX", "GT"}, {"NX", "LT"}, {"GT", "LT"}, {"XX", "NX", "GT", "LT"}, {"INVALID"}, {"weird\r\noption"}, {"nx\x00suffix"}, {"é"}} {
					for _, initial := range []string{"persistent", "expiring", "missing"} {
						compare("DEL", "key")
						if initial != "missing" {
							compare("SET", "key", "value")
						}
						if initial == "expiring" {
							compare("EXPIRE", "key", "200")
						}
						compare(append([]string{"EXPIRE", "key", "100"}, options...)...)
						compare("TTL", "key")
						compare("GET", "key")
						compare(append([]string{"EXPIRE", "key", "0"}, options...)...)
						compare("GET", "key")
					}
					compare(append([]string{"EXPIRE", "missing", "bad"}, options...)...)
				}
				// Non-string values still support keyspace operations, but not DIGEST.
				backend := store.(storage.Backend)
				backend.Put("list", storage.Entry{Value: 42})
				exchange(t, ref.conn, ref.reader, 1, "LPUSH", "list", "item")
				compare("DIGEST", "list")
				compare("TTL", "list")
				compare("EXPIRE", "list", "200")
				compare("DEL", "list")
				compare("PING")
				t.Logf("compared %d replies against Redis 8.10.2", count)
			})
		}
	})
}

func (s failingStringStore) Read(context.Context, string, storage.ReadFunc) error { return s.cause }
func (s failingStringStore) DeleteKeys(context.Context, []string) (int64, error)  { return 0, s.cause }

func TestKeyspaceStorageFailuresStayInternal(t *testing.T) {
	cause := errors.New("secret storage failure")
	h := NewHandler(cmd.NewCommands(failingStringStore{cause: cause}, nil))
	for _, args := range [][]string{{"DEL", "key"}, {"TTL", "key"}, {"EXPIRE", "key", "1"}, {"DIGEST", "key"}} {
		value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader(string(stringRequest(args...)))))
		if !errors.Is(err, cause) || string(value.Marshal()) != "-ERR internal server error\r\n" {
			t.Fatalf("%q: %v, %v", args, value, err)
		}
	}
}

func TestKeyspacePersistence(t *testing.T) {
	for _, appendOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(appendOnly), func(t *testing.T) {
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.Save = nil
			config.AppendOnly = appendOnly
			config.AppendFsync = "always"
			db := openPersistenceDB(t, config)
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
			reader := resp.NewReader(peer)
			for _, key := range []string{"keep", "deleted", "zero", "negative"} {
				exchange(t, peer, reader, 1, "SET", key, "binary\x00\xff")
			}
			for _, args := range [][]string{{"EXPIRE", "keep", "3600", "NX"}, {"DEL", "deleted", "deleted"}, {"EXPIRE", "zero", "0"}, {"EXPIRE", "negative", "-1"}} {
				if got := exchange(t, peer, reader, 1, args...); got != ":1\r\n" {
					t.Fatal(got)
				}
			}
			e, exists, err := db.Get(context.Background(), "keep")
			if err != nil || !exists {
				t.Fatal(e, exists, err)
			}
			if !appendOnly {
				if err := db.Save(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			restored := openPersistenceDB(t, config)
			assertDatabaseState(t, restored, map[string]storage.Entry{"keep": e})
			for _, key := range []string{"deleted", "zero", "negative"} {
				if _, ok, err := restored.Get(context.Background(), key); err != nil || ok {
					t.Fatalf("restored deleted key %q", key)
				}
			}
			// Redis consumes the actual files produced by these commands.
			t.Run("RedisInterop", func(t *testing.T) {
				mode := "no"
				if appendOnly {
					mode = "yes"
				}
				ref := startRedisFiles(t, persistenceFiles(t, config.Dir, appendOnly), "--appendonly", mode)
				assertReferenceState(t, ref, map[string]storage.Entry{"keep": e})
				for _, key := range []string{"deleted", "zero", "negative"} {
					if got := exchange(t, ref.conn, ref.reader, 1, "GET", key); got != "$-1\r\n" {
						t.Fatal(got)
					}
				}
			})
		})
	}
}
