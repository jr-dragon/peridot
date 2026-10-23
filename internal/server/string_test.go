package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
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

func stringRequest(args ...string) []byte {
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.NewBulkString(arg)
	}
	return resp.NewArray(values).Marshal()
}

func exchange(t *testing.T, conn net.Conn, reader *resp.Reader, replies int, args ...string) string {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(stringRequest(args...)); err != nil {
		t.Fatalf("%q: write request: %v", args, err)
	}
	var wire strings.Builder
	for range replies {
		value, err := reader.Read()
		if err != nil {
			t.Fatalf("%q: read reply: %v", args, err)
		}
		wire.Write(value.Marshal())
	}
	return wire.String()
}

func TestStringPipelineAndProtocolVersions(t *testing.T) {
	forEachStringStore(t, testStringPipelineAndProtocolVersions)
}

func testStringPipelineAndProtocolVersions(t *testing.T, newStore func() storage.KeyStore) {
	store := newStore()
	handler := NewHandler(cmd.NewCommands(store, nil))
	_, first := startRESPPipe(t, handler)
	_, second := startRESPPipe(t, handler)
	for _, protocol := range []int{3, 2} {
		reader := resp.NewReader(first)
		hello := exchange(t, first, reader, 1, "HELLO", fmt.Sprint(protocol))
		if protocol == 3 && !strings.HasPrefix(hello, "%7\r\n") || protocol == 2 && !strings.HasPrefix(hello, "*14\r\n") {
			t.Fatalf("HELLO: %q", hello)
		}
		key := fmt.Sprintf("key-%d\x00\xff", protocol)
		requests := [][]string{
			{"GET", key}, {"SET", key, "\x00\r\n\xff"}, {"GET", key},
			{"SET", key, "new", "NX", "GET"}, {"SET", key, "new", "NX"},
			{"SET", key, "new", "EX", "0"}, {"SET", key, "new", "NX", "XX"},
			{"GET", key}, {"PING"},
		}
		var input []byte
		for _, args := range requests {
			input = append(input, stringRequest(args...)...)
		}
		null := "$-1\r\n"
		if protocol == 3 {
			null = "_\r\n"
		}
		want := null + "+OK\r\n$4\r\n\x00\r\n\xff\r\n$4\r\n\x00\r\n\xff\r\n" + null +
			"-ERR invalid expire time in 'set' command\r\n-ERR syntax error\r\n$4\r\n\x00\r\n\xff\r\n+PONG\r\n"
		if _, err := first.Write(input); err != nil {
			t.Fatal(err)
		}
		actual := make([]byte, len(want))
		if _, err := io.ReadFull(first, actual); err != nil || string(actual) != want {
			t.Fatalf("pipeline = %q, %v; want %q", actual, err, want)
		}
		// Storage is shared, while protocol negotiation is connection-local.
		secondReader := resp.NewReader(second)
		if got := exchange(t, second, secondReader, 1, "GET", key); got != "$4\r\n\x00\r\n\xff\r\n" {
			t.Fatalf("shared value: %q", got)
		}
		if got := exchange(t, second, secondReader, 1, "GET", "missing"); got != "$-1\r\n" {
			t.Fatalf("protocol leaked: %q", got)
		}
	}
}

type failingStringStore struct {
	storage.KeyStore
	cause error
}

func (s failingStringStore) Get(context.Context, string) (storage.Entry, bool, error) {
	return storage.Entry{}, false, s.cause
}
func (s failingStringStore) Update(context.Context, string, storage.UpdateFunc) error { return s.cause }

func TestStringStorageFailuresStayInternal(t *testing.T) {
	cause := errors.New("secret storage failure")
	h := NewHandler(cmd.NewCommands(failingStringStore{cause: cause}, nil))
	for _, args := range [][]string{{"GET", "key"}, {"SET", "key", "value"}} {
		value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader(string(stringRequest(args...)))))
		if !errors.Is(err, cause) || !errors.Is(err, ErrServer) || string(value.Marshal()) != "-ERR internal server error\r\n" {
			t.Fatalf("storage failure = %v, %v", value, err)
		}
	}
}

// TestStringsAgainstRedis is opt-in so ordinary tests need no installed Redis.
// REDIS_REFERENCE must name a Redis 8.10.2 executable; no existing server is used.
func TestStringsAgainstRedis(t *testing.T) {
	forEachStringStore(t, testStringsAgainstRedis)
}

func TestSnapshotOnlyStringsAgainstRedis(t *testing.T) {
	for name, factory := range map[string]func() storage.Backend{
		"xsync": func() storage.Backend { return storage.NewXsync() },
	} {
		t.Run(name, func(t *testing.T) {
			testStringsAgainstRedis(t, func() storage.KeyStore {
				config := data.DefaultConfig()
				config.Dir, config.Save = t.TempDir(), nil
				p, err := persistence.New(config)
				if err != nil {
					t.Fatal(err)
				}
				db := database.New(factory(), p, database.WithActiveExpiration(config.HZ, config.DynamicHZ))
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				return db
			})
		})
	}
}

func testStringsAgainstRedis(t *testing.T, newStore func() storage.KeyStore) {
	path := os.Getenv("REDIS_REFERENCE")
	if path == "" {
		t.Skip("set REDIS_REFERENCE to a Redis 8.10.2 executable")
	}
	version, err := exec.Command(path, "--version").Output()
	if err != nil || !strings.Contains(string(version), "v=8.10.2 ") {
		t.Fatalf("reference must be Redis 8.10.2: %s, %v", version, err)
	}
	t.Logf("reference: %s", strings.TrimSpace(string(version)))
	// Keep the Unix socket path below macOS's sockaddr_un path limit.
	dir, err := os.MkdirTemp("/tmp", "peridot-redis-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "redis.sock")
	process := exec.Command(path, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no", "--dir", dir, "--logfile", filepath.Join(dir, "redis.log"), "--enable-debug-command", "yes")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	t.Cleanup(func() { process.Process.Kill(); <-done })
	var reference net.Conn
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for reference == nil {
		conn, err := net.DialTimeout("unix", socket, time.Second)
		if err == nil {
			reference = conn
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			log, _ := os.ReadFile(filepath.Join(dir, "redis.log"))
			t.Fatalf("Redis did not start: %s", log)
		}
	}
	t.Cleanup(func() { reference.Close() })
	refReader := resp.NewReader(reference)
	if got := exchange(t, reference, refReader, 1, "DEBUG", "SET-ACTIVE-EXPIRE", "0"); got != "+OK\r\n" {
		t.Fatalf("disable active expiration: %q", got)
	}
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprintf("RESP%d", protocol), func(t *testing.T) {
			store := newStore()
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(store, nil)))
			reader := resp.NewReader(peer)
			exchange(t, reference, refReader, 1, "HELLO", fmt.Sprint(protocol))
			exchange(t, peer, reader, 1, "HELLO", fmt.Sprint(protocol))
			var count int
			compare := func(replies int, args ...string) {
				t.Helper()
				want := exchange(t, reference, refReader, replies, args...)
				got := exchange(t, peer, reader, replies, args...)
				count++
				if got != want {
					t.Fatalf("%q: Peridot=%q, Redis=%q", args, got, want)
				}
			}
			keyNumber := 0
			freshKey := func() string { keyNumber++; return fmt.Sprintf("p%d-k%d", protocol, keyNumber) }
			for _, value := range []string{"", "old", "123", "00123", "-9223372036854775808", "\x00\xff\r\n", strings.Repeat("x", 17), strings.Repeat("x", 129), strings.Repeat("x", 241), strings.Repeat("x", 1025), strings.Repeat("x", 4096)} {
				key := freshKey()
				compare(1, "SET", key, value)
				digestValue := exchange(t, reference, refReader, 1, "DIGEST", key)
				digest := strings.Split(digestValue, "\r\n")[1]
				compare(1, "GET", key)
				compare(1, "SET", key, "new", "IFDEQ", strings.ToUpper(digest), "GET")
				compare(1, "SET", key, value)
				compare(1, "SET", key, "new", "IFDNE", digest, "GET")
				compare(1, "SET", key, "new", "IFEQ", value, "GET")
			}
			conditions := [][]string{nil, {"NX"}, {"XX"}, {"IFEQ", "old"}, {"IFEQ", "different"}, {"IFNE", "old"}, {"IFNE", "different"}, {"IFDEQ", "0000000000000000"}, {"IFDNE", "0000000000000000"}, {"IFDEQ", "bad"}, {"IFDNE", "bad"}, {"IFDNE", "zzzzzzzzzzzzzzzz"}}
			expirations := []struct {
				args    []string
				invalid bool
			}{
				{nil, false}, {[]string{"KEEPTTL"}, false}, {[]string{"EX", "10"}, false},
				{[]string{"PX", "10000"}, false}, {[]string{"EXAT", "4102444800"}, false},
				{[]string{"PXAT", "4102444800000"}, false}, {[]string{"EXAT", "1"}, false},
				{[]string{"PXAT", "1"}, false}, {[]string{"EX", "0"}, true},
				{[]string{"PX", "9223372036854775807"}, false}, {[]string{"EX", "9223372036854776"}, true},
				{[]string{"PXAT", "9223372036854775807"}, false}, {[]string{"EX", "+1"}, true},
				{[]string{"EX", "01"}, true}, {[]string{"PX", "-0"}, true}, {[]string{"PX", "1\x00"}, true},
			}
			for _, exists := range []bool{false, true} {
				for _, get := range []bool{false, true} {
					for _, condition := range conditions {
						for _, expiration := range expirations {
							key := freshKey()
							if exists {
								compare(1, "SET", key, "old", "PX", "100000")
							}
							args := append([]string{"SET", key, "new"}, condition...)
							args = append(args, expiration.args...)
							if get {
								args = append(args, "GET")
							}
							replies := 1
							if exists && get && len(condition) == 2 && (condition[0] == "IFDEQ" || condition[0] == "IFDNE") && condition[1] == "bad" && !expiration.invalid {
								replies = 2
							}
							compare(replies, args...)
							compare(1, "GET", key)
						}
					}
				}
			}
			for _, options := range [][]string{{"NX", "NX"}, {"XX", "XX"}, {"GET", "GET"}, {"EX", "bad", "EX", "10"}, {"PX", "10", "PX", "1000"}, {"KEEPTTL", "KEEPTTL"}, {"IFEQ", "bad", "IFEQ", "old"}, {"NX", "XX"}, {"EX", "1", "PX", "2"}, {"NX\x00"}, {"IFEQ\x00suffix", "old"}, {"KEEPTTL\x00suffix"}, {"İFEQ", "old"}, {"EX"}, {"IFEQ"}} {
				key := freshKey()
				compare(1, "SET", key, "old")
				compare(1, append([]string{"SET", key, "new"}, options...)...)
				compare(1, "GET", key)
			}
			for _, condition := range conditions {
				for _, get := range []bool{false, true} {
					key := freshKey()
					exchange(t, reference, refReader, 1, "LPUSH", key, "item")
					if err := store.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
						return storage.Entry{Value: 1}, true, nil
					}); err != nil {
						t.Fatal(err)
					}
					compare(1, "GET", key)
					args := append([]string{"SET", key, "new"}, condition...)
					if get {
						args = append(args, "GET")
					}
					compare(1, args...)
					compare(1, "GET", key)
				}
			}
			t.Logf("compared %d command replies against Redis 8.10.2", count)
		})
	}
}

func forEachStringStore(t *testing.T, run func(*testing.T, func() storage.KeyStore)) {
	t.Helper()
	t.Run("xsync", func(t *testing.T) { run(t, func() storage.KeyStore { return storage.NewXsync() }) })
}
