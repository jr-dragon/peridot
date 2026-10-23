package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type redisAOFReference struct {
	dir    string
	conn   net.Conn
	reader *resp.Reader
	stop   func()
}

func startRedisAOF(t *testing.T, legacy []byte) redisAOFReference {
	files := make(map[string][]byte)
	if legacy != nil {
		files["appendonly.aof"] = legacy
	}
	return startRedisFiles(t, files)
}

func startRedisFiles(t *testing.T, files map[string][]byte, extra ...string) redisAOFReference {
	t.Helper()
	path := os.Getenv("REDIS_REFERENCE")
	if path == "" {
		t.Skip("set REDIS_REFERENCE to a Redis 8.10.2 executable")
	}
	version, err := exec.Command(path, "--version").Output()
	if err != nil || !strings.Contains(string(version), "v=8.10.2 ") {
		t.Fatalf("reference must be Redis 8.10.2: %s, %v", version, err)
	}
	t.Logf("reference: %s", strings.TrimSpace(string(version)))
	dir, err := os.MkdirTemp("/tmp", "peridot-aof-redis-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	for name, content := range files {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(dir, "redis.sock")
	args := []string{"--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "yes",
		"--aof-use-rdb-preamble", "no", "--appendfsync", "always", "--auto-aof-rewrite-percentage", "0",
		"--aof-timestamp-enabled", "yes", "--dir", dir, "--logfile", filepath.Join(dir, "redis.log")}
	process := exec.Command(path, append(args, extra...)...)
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = process.Wait(); close(done) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = process.Process.Signal(os.Interrupt)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = process.Process.Kill()
				<-done
			}
		})
	}
	t.Cleanup(stop)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		conn, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
		if err == nil {
			t.Cleanup(func() { conn.Close() })
			reader := resp.NewReader(conn)
			// Redis accepts sockets during legacy-AOF upgrade before loading has
			// finished. Readiness requires PONG, not merely a connected socket.
			for {
				reply := exchange(t, conn, reader, 1, "PING")
				if reply == "+PONG\r\n" {
					break
				}
				if !strings.HasPrefix(reply, "-LOADING ") {
					t.Fatalf("Redis readiness: %q", reply)
				}
				select {
				case <-ticker.C:
				case <-deadline.C:
					t.Fatal("Redis did not finish loading")
				}
			}
			return redisAOFReference{dir: dir, conn: conn, reader: reader, stop: stop}
		}
		select {
		case <-ticker.C:
		case <-done:
			log, _ := os.ReadFile(filepath.Join(dir, "redis.log"))
			t.Fatalf("Redis exited: %s", log)
		case <-deadline.C:
			log, _ := os.ReadFile(filepath.Join(dir, "redis.log"))
			t.Fatalf("Redis did not start: %s", log)
		}
	}
}

// redisCommandFiles extracts actual Redis-emitted command bytes in manifest
// order. The product under test receives one plain file, never a manifest.
func redisCommandFiles(t *testing.T, dir string) (all, base []byte, baseName string) {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(dir, "appendonlydir", "appendonly.aof.manifest"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(manifest), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 6 || fields[0] != "file" {
			t.Fatalf("unexpected reference manifest: %q", line)
		}
		kind := fields[5]
		if kind != "b" && kind != "i" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, "appendonlydir", fields[1]))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.HasPrefix(content, []byte("REDIS")) {
			t.Fatal("reference unexpectedly produced an RDB preamble")
		}
		all = append(all, content...)
		if kind == "b" {
			base = content
			baseName = fields[1]
		}
	}
	return
}

func waitRedisRewrite(t *testing.T, ref redisAOFReference, previousBase string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		info := exchange(t, ref.conn, ref.reader, 1, "INFO", "persistence")
		_, _, base := redisCommandFiles(t, ref.dir)
		if base != previousBase && strings.Contains(info, "aof_rewrite_in_progress:0\r\n") && strings.Contains(info, "aof_last_bgrewrite_status:ok\r\n") {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("Redis rewrite did not finish: %s", info)
		}
	}
}

func TestAOFLoadsRedisOriginalAndRewrittenCommands(t *testing.T) {
	ref := startRedisAOF(t, nil)
	const deadline = "4102444800000"
	key, value := "key\x00\xff", "\x00\r\n\xff"
	commands := [][]string{
		{"SET", key, value}, {"SET", "", ""},
		{"SET", "conditional", "old", "NX"}, {"SET", "conditional", "ignored", "NX"},
		{"SET", "conditional", "new", "IFEQ", "old"}, {"SET", "conditional", "final", "IFNE", "old"},
		{"SET", "nul-option", "old", "NX\x00suffix"}, {"SET", "nul-option", "new", "IFEQ\x00suffix", "old"},
		{"SET", "keep", "old", "PXAT", deadline}, {"SET", "keep", "new", "KEEPTTL"}, {"PERSIST", "keep"},
		{"SET", "future", "old", "PXAT", deadline}, {"SET", "future", "new", "PXAT", deadline, "XX", "GET"},
		{"SET", "deleted", "old"}, {"SET", "deleted", "gone", "PXAT", "1"},
		{"SET", "recreate", "old"}, {"DEL", "recreate"}, {"SETNX", "recreate", "new"},
		{"SET", "unlinked", "old"}, {"UNLINK", "unlinked"},
		{"MULTI"}, {"SET", "transaction", "yes"}, {"PEXPIREAT", "transaction", deadline}, {"EXEC"},
	}
	for _, args := range commands {
		got := exchange(t, ref.conn, ref.reader, 1, args...)
		if strings.HasPrefix(got, "-") {
			t.Fatalf("Redis rejected fixture %q: %q", args, got)
		}
	}
	keys := []string{key, "", "conditional", "nul-option", "keep", "future", "deleted", "recreate", "unlinked", "transaction"}
	// These successful writes retain options in Redis's original AOF that
	// must survive import even before Redis rewrites them into plain state.
	retainedSets := [][]string{
		{"SET", "short-digest", "value", "IFDNE", "bad"},
		{"SET", "empty-digest", "value", "IFDNE", ""},
		{"SET", "repeated-digest", "old"},
		{"SET", "repeated-digest", "value", "IFDNE", "bad", "IFDNE", "0000000000000000"},
		{"SET", "repeated-pxat", "value", "PXAT", "bad", "PXAT", deadline},
	}
	for _, args := range retainedSets {
		if got := exchange(t, ref.conn, ref.reader, 1, args...); got != "+OK\r\n" {
			t.Fatalf("Redis %q = %q, want OK", args, got)
		}
	}
	keys = append(keys, "short-digest", "empty-digest", "repeated-digest", "repeated-pxat")
	var retainedExpires [][]string
	for i, flags := range [][]string{{"NX"}, {"XX"}, {"GT"}, {"LT"}, {"XX", "GT"}, {"XX", "LT"}, {"NX", "NX"}, {"nx\x00suffix"}} {
		k := fmt.Sprintf("expire-flags-%d", i)
		initial := []string{"SET", k, "value"}
		if flags[0] == "XX" || flags[0] == "GT" || flags[0] == "LT" {
			initial = append(initial, "PXAT", deadline)
		}
		if got := exchange(t, ref.conn, ref.reader, 1, initial...); got != "+OK\r\n" {
			t.Fatalf("Redis %q = %q, want OK", initial, got)
		}
		expiresAt := "4102444900000"
		if flags[len(flags)-1] == "LT" {
			expiresAt = "4102444700000"
		}
		args := append([]string{"PEXPIREAT", k, expiresAt}, flags...)
		if got := exchange(t, ref.conn, ref.reader, 1, args...); got != ":1\r\n" {
			t.Fatalf("Redis %q = %q, want 1", args, got)
		}
		retainedExpires = append(retainedExpires, args)
		keys = append(keys, k)
	}
	want := make(map[string]string)
	expiry := make(map[string]int64)
	for _, k := range keys {
		want[k] = exchange(t, ref.conn, ref.reader, 1, "GET", k)
		wire := exchange(t, ref.conn, ref.reader, 1, "PEXPIRETIME", k)
		n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(wire, ":"), "\r\n"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		expiry[k] = n
	}
	original, _, baseName := redisCommandFiles(t, ref.dir)
	for _, commands := range [][][]string{retainedSets, retainedExpires} {
		for _, args := range commands {
			if !bytes.Contains(original, stringRequest(args...)) {
				t.Fatalf("reference AOF did not retain %q", args)
			}
		}
	}
	if got := exchange(t, ref.conn, ref.reader, 1, "BGREWRITEAOF"); got != "+Background append only file rewriting started\r\n" {
		t.Fatal(got)
	}
	waitRedisRewrite(t, ref, baseName)
	_, rewritten, _ := redisCommandFiles(t, ref.dir)
	ref.stop()
	for name, input := range map[string][]byte{"original-stream": original, "rewritten-base": rewritten} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "import.aof")
			if err := os.WriteFile(path, input, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := openAOFDatabase(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(s, s)))
			reader := resp.NewReader(peer)
			for _, k := range keys {
				if got := exchange(t, peer, reader, 1, "GET", k); got != want[k] {
					t.Fatalf("GET %q: Peridot=%q, Redis=%q", k, got, want[k])
				}
				e, ok, err := s.Get(context.Background(), k)
				if err != nil {
					t.Fatal(err)
				}
				if expiry[k] >= 0 && (!ok || e.ExpiresAt != expiry[k]) || expiry[k] == -1 && (!ok || e.ExpiresAt != 0) || expiry[k] == -2 && ok {
					t.Fatalf("expiry %q: %v,%v, reference=%d", k, e, ok, expiry[k])
				}
			}
			if err := s.Rewrite(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAOFRedisLoadsPeridotOriginalAndRewrite(t *testing.T) {
	if os.Getenv("REDIS_REFERENCE") == "" {
		t.Skip("set REDIS_REFERENCE to a Redis 8.10.2 executable")
	}
	path := filepath.Join(t.TempDir(), "export.aof")
	s, err := openAOFDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(s, s)))
	reader := resp.NewReader(peer)
	const deadline = "4102444800000"
	key, value := "\x00\xffkey", "\x00\r\n\xff"
	commands := [][]string{
		{"SET", key, "old"}, {"SET", key, value}, {"SET", "", ""},
		{"SET", "future", "old", "PXAT", deadline}, {"SET", "future", "new", "KEEPTTL"},
		{"SET", "persist", "old", "PXAT", deadline}, {"SET", "persist", "new"},
		{"SET", "deleted", "old"}, {"SET", "deleted", "gone", "PXAT", "1"},
		{"SET", "conditional", "first", "NX"}, {"SET", "conditional", "ignored", "NX"},
		{"SET", "absent", "ignored", "XX"},
	}
	for _, args := range commands {
		got := exchange(t, peer, reader, 1, args...)
		if strings.HasPrefix(got, "-") {
			t.Fatalf("Peridot rejected fixture %q: %q", args, got)
		}
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := exchange(t, peer, reader, 1, "BGREWRITEAOF"); got != "+Background append only file rewriting started\r\n" {
		t.Fatal(got)
	}
	// Close waits for the scheduled job before the rewritten file is inspected.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rewritten, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewritten) >= len(original) {
		t.Fatalf("rewrite did not compact history: %d >= %d", len(rewritten), len(original))
	}
	for name, input := range map[string][]byte{"original": original, "rewritten": rewritten} {
		t.Run(name, func(t *testing.T) {
			ref := startRedisAOF(t, input)
			for k, v := range map[string]string{key: value, "": "", "future": "new", "persist": "new", "conditional": "first"} {
				want := string(resp.NewBulkString(v).Marshal())
				if got := exchange(t, ref.conn, ref.reader, 1, "GET", k); got != want {
					t.Fatalf("GET %q: Redis=%q, want=%q", k, got, want)
				}
			}
			for _, k := range []string{"deleted", "absent"} {
				if got := exchange(t, ref.conn, ref.reader, 1, "GET", k); got != "$-1\r\n" {
					t.Fatalf("GET %q=%q", k, got)
				}
			}
			for k, expiry := range map[string]string{"future": deadline, "persist": "-1"} {
				if got := exchange(t, ref.conn, ref.reader, 1, "PEXPIRETIME", k); got != fmt.Sprintf(":%s\r\n", expiry) {
					t.Fatalf("expiry %q=%q", k, got)
				}
			}
		})
	}
}

func openAOFDatabase(path string) (*database.DB, error) {
	p, err := persistence.OpenAOF(path)
	if err != nil {
		return nil, err
	}
	return database.New(storage.New(), p), nil
}
