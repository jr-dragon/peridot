package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func waitPersistence(t *testing.T, ready func() bool) {
	t.Helper()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("persistence condition did not become ready")
		}
	}
}

func TestSnapshotThresholdAgainstRedis(t *testing.T) {
	ref := startRedisFiles(t, nil, "--appendonly", "no", "--save", "1", "2")
	config := data.DefaultConfig()
	config.Dir = t.TempDir()
	config.Save = []data.SaveRule{{Seconds: 1, Changes: 2}}
	db := openPersistenceDB(t, config)
	_, peer := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
	reader := resp.NewReader(peer)
	for _, args := range [][]string{{"SET", "key", "value"}, {"SET", "key", "value", "NX"}, {"SET", "key", "value"}} {
		want := exchange(t, ref.conn, ref.reader, 1, args...)
		if got := exchange(t, peer, reader, 1, args...); got != want {
			t.Fatalf("%v: got %q, Redis %q", args, got, want)
		}
	}
	// Both snapshots must complete before shutdown can create a final snapshot.
	waitPersistence(t, func() bool {
		_, a := os.Stat(filepath.Join(config.Dir, "dump.rdb"))
		_, b := os.Stat(filepath.Join(ref.dir, "dump.rdb"))
		return a == nil && b == nil
	})
	peridotFiles := persistenceFiles(t, config.Dir, false)
	loaded := startRedisFiles(t, peridotFiles, "--appendonly", "no")
	if got := exchange(t, loaded.conn, loaded.reader, 1, "GET", "key"); got != "$5\r\nvalue\r\n" {
		t.Fatal(got)
	}
}

func TestPingSnapshotFailureAgainstRedis(t *testing.T) {
	for _, mode := range []string{"blocked", "blocked-aof", "stop-writes-disabled", "save-disabled"} {
		t.Run(mode, func(t *testing.T) {
			rules, stop := "3600 1", "yes"
			if mode == "stop-writes-disabled" {
				stop = "no"
			}
			if mode == "save-disabled" {
				rules = ""
			}
			appendOnly := "no"
			if mode == "blocked-aof" {
				appendOnly = "yes"
			}
			ref := startRedisFiles(t, nil, "--appendonly", appendOnly, "--save", rules, "--stop-writes-on-bgsave-error", stop)
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.AppendOnly = appendOnly == "yes"
			config.AppendFsync = "always"
			config.AOFUseRDBPreamble = false
			config.StopWritesOnBGSaveError = stop == "yes"
			if rules == "" {
				config.Save = nil
			} else {
				config.Save = []data.SaveRule{{Seconds: 3600, Changes: 1}}
			}
			db := openPersistenceDB(t, config)
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
			reader := resp.NewReader(peer)
			compare := func(args ...string) string {
				t.Helper()
				want := exchange(t, ref.conn, ref.reader, 1, args...)
				got := exchange(t, peer, reader, 1, args...)
				if got != want {
					t.Fatalf("%v: got %q, Redis %q", args, got, want)
				}
				return got
			}
			compare("SET", "key", "before\x00\xff")
			paths := []string{filepath.Join(ref.dir, "dump.rdb"), filepath.Join(config.Dir, "dump.rdb")}
			for _, path := range paths {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				for _, path := range paths {
					_ = os.Remove(path)
				}
			})
			compare("BGSAVE")
			waitPersistence(t, func() bool {
				info := exchange(t, ref.conn, ref.reader, 1, "INFO", "persistence")
				if !strings.Contains(info, "rdb_last_bgsave_status:err\r\n") {
					return false
				}
				// SAVE cannot overlap BGSAVE; its reply gives a deterministic completion
				// barrier even when configured policy intentionally allows writes.
				reply := exchange(t, peer, reader, 1, "SAVE")
				return reply == "-ERR\r\n"
			})
			// Foreground failure must preserve the completed BGSAVE failure on both servers.
			if got := compare("SAVE"); got != "-ERR\r\n" {
				t.Fatal(got)
			}
			wantPing := "+PONG\r\n"
			if strings.HasPrefix(mode, "blocked") {
				wantPing = "-MISCONF "
			}
			if got := compare("PING"); !strings.HasPrefix(got, wantPing) {
				t.Fatal(got)
			}
			compare("PING", "\x00\r\n\xff")
			compare("PING", "one", "two")
			compare("SET", "key", "value")
			for _, args := range [][]string{
				{"SET"}, {"SET", "key"}, {"SET", "key", "value", "INVALID"},
				{"SET", "key", "value", "NX", "XX"}, {"SET", "key", "value", "EX"},
				{"SET", "key", "value", "PX", "bad"}, {"SET", "key", "value", "EX", "0"},
				{"EXPIRE", "key"}, {"EXPIRE", "key", "bad"},
				{"EXPIRE", "key", "100", "INVALID"}, {"EXPIRE", "key", "9223372036854775807"},
			} {
				compare(args...)
			}
			compare("GET", "key")
			// RDB failure must not prevent a healthy AOF from exporting the data.
			if got := compare("BGREWRITEAOF"); got != "+Background append only file rewriting started\r\n" {
				t.Fatal(got)
			}
			waitPersistence(t, func() bool {
				info := exchange(t, ref.conn, ref.reader, 1, "INFO", "persistence")
				manifest, _ := os.ReadFile(filepath.Join(config.Dir, "appendonlydir", "appendonly.aof.manifest"))
				return strings.Contains(string(manifest), "type b") && strings.Contains(info, "aof_rewrite_in_progress:0\r\n") && strings.Contains(info, "aof_last_bgrewrite_status:ok\r\n")
			})
			loaded := startRedisFiles(t, persistenceFiles(t, config.Dir, true))
			if got, want := exchange(t, loaded.conn, loaded.reader, 1, "GET", "key"), compare("GET", "key"); got != want {
				t.Fatalf("rewritten data: got %q, want %q", got, want)
			}
			loaded.stop()
			if got := compare("PING"); !strings.HasPrefix(got, wantPing) {
				t.Fatalf("AOF rewrite changed RDB failure state: %q", got)
			}
			// Every error must remain recoverable on this same connection.
			for _, path := range paths {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			compare("SAVE")
			if got := compare("PING"); got != "+PONG\r\n" {
				t.Fatal(got)
			}
			compare("SET", "key", "recovered")
			compare("GET", "key")
		})
	}
}

func TestDisabledAOFRewriteAgainstRedis(t *testing.T) {
	for _, preamble := range []bool{false, true} {
		t.Run(fmt.Sprint(preamble), func(t *testing.T) {
			setting := map[bool]string{false: "no", true: "yes"}[preamble]
			ref := startRedisFiles(t, nil, "--appendonly", "no", "--aof-use-rdb-preamble", setting)
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.Save = nil
			config.AOFUseRDBPreamble = preamble
			db := openPersistenceDB(t, config)
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
			reader := resp.NewReader(peer)
			compare := func(args ...string) {
				t.Helper()
				want := exchange(t, ref.conn, ref.reader, 1, args...)
				if got := exchange(t, peer, reader, 1, args...); got != want {
					t.Fatalf("%v: got %q, Redis %q", args, got, want)
				}
			}
			compare("SET", "key\x00\xff", "before\x00\xff", "PXAT", "4102444800000")
			compare("BGREWRITEAOF")
			// Writes immediately after the reply must not enter the exported snapshot.
			compare("SET", "key\x00\xff", "after")
			compare("SET", "tail", "not logged")
			manifest := filepath.Join("appendonlydir", "appendonly.aof.manifest")
			waitPersistence(t, func() bool {
				info := exchange(t, ref.conn, ref.reader, 1, "INFO", "persistence")
				_, a := os.Stat(filepath.Join(config.Dir, manifest))
				_, b := os.Stat(filepath.Join(ref.dir, manifest))
				return a == nil && b == nil && strings.Contains(info, "aof_rewrite_in_progress:0\r\n") && strings.Contains(info, "aof_last_bgrewrite_status:ok\r\n")
			})
			for _, dir := range []string{config.Dir, ref.dir} {
				contents, err := os.ReadFile(filepath.Join(dir, manifest))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(contents), "type i") {
					t.Fatalf("disabled AOF produced increment: %s", contents)
				}
				files := persistenceFiles(t, dir, true)
				loaded := startRedisFiles(t, files, "--aof-use-rdb-preamble", setting)
				if got := exchange(t, loaded.conn, loaded.reader, 1, "GET", "key\x00\xff"); got != "$8\r\nbefore\x00\xff\r\n" {
					t.Fatal(got)
				}
				if got := exchange(t, loaded.conn, loaded.reader, 1, "PEXPIRETIME", "key\x00\xff"); got != ":4102444800000\r\n" {
					t.Fatal(got)
				}
				if got := exchange(t, loaded.conn, loaded.reader, 1, "GET", "tail"); got != "$-1\r\n" {
					t.Fatal(got)
				}
				loaded.stop()
			}
			// A repeated rewrite must replace the previous snapshot with current data.
			before, _ := os.ReadFile(filepath.Join(config.Dir, manifest))
			compare("BGREWRITEAOF")
			waitPersistence(t, func() bool {
				after, _ := os.ReadFile(filepath.Join(config.Dir, manifest))
				return string(after) != string(before)
			})
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			loaded := startRedisFiles(t, persistenceFiles(t, config.Dir, true))
			if got := exchange(t, loaded.conn, loaded.reader, 1, "GET", "tail"); got != "$10\r\nnot logged\r\n" {
				t.Fatal(got)
			}
		})
	}
}
