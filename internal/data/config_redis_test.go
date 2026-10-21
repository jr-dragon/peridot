package data

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/pkg/resp"
)

// This opt-in differential test requires the project's exact reference version.
func TestRedisConfigCompatibility(t *testing.T) {
	binary := os.Getenv("PERIDOT_REDIS_SERVER")
	if binary == "" {
		t.Skip("set PERIDOT_REDIS_SERVER to a Redis 8.10.2 executable")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || !strings.Contains(string(version), "v=8.10.2 ") {
		t.Fatalf("Redis 8.10.2 required: %s, %v", version, err)
	}
	for _, tc := range []struct {
		name, content string
		args          []string
	}{
		{"defaults", "", nil},
		{"expiration", "hz 50\ndynamic-hz no\n", nil},
		{"expiration minimum", "hz 0\n", nil},
		{"expiration maximum", "hz 2147483647\n", nil},
		{"expiration override", "hz 30\ndynamic-hz no\n", []string{"--hz", "75", "--dynamic-hz", "yes"}},
		{"snapshot paths", "appenddirname logs\ndbfilename snapshot.rdb\naof-use-rdb-preamble no\nstop-writes-on-bgsave-error no\n", nil},
		{"fsync no", "appendfsync no\n", nil},
		{"fsync always", "appendfsync ALWAYS\n", nil},
		{"fsync everysec", "appendfsync everysec\n", nil},
		{"fsync override", "appendfsync always\n", []string{"--appendfsync", "everysec"}},
		{"file and CLI", "port 6380\nport 6381\nbind 127.0.0.1 -::1\nappendfilename \"file name.aof\"\n", []string{"--appendfilename", "CLI name.aof"}},
		{"escaped filename", "appendfilename \"prefix\\x41\\t\\q.aof\"\n", nil},
		{"single quotes", "appendfilename 'single\\'quote.aof'\n", nil},
		{"embedded quotes", "appendfilename prefix\" suffix.aof\"\n", nil},
		{"quoted bind", "bind \"127.0.0.1 -::1\"\n", nil},
		{"includes and dir", "include base.conf\ndir nested\ninclude child.conf\n", nil},
		{"option-like value", "", []string{"--appendfilename", "--port"}},
		{"whole argument", "", []string{"--appendfilename whole.aof"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A short path avoids Unix socket path limits on macOS.
			dir, err := os.MkdirTemp("/tmp", "peridot-config-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			t.Chdir(dir)
			if err := os.Mkdir("nested", 0700); err != nil {
				t.Fatal(err)
			}
			writeConfig(t, "base.conf", "appendfilename base.aof\n")
			writeConfig(t, "nested/child.conf", "appendfilename child.aof\n")
			writeConfig(t, "redis.conf", tc.content)
			args := append([]string{"redis.conf"}, tc.args...)
			socket := filepath.Join(dir, "redis.sock")
			// Disable persistence and TCP in the reference process; Unix sockets
			// allow inspecting configuration without reserving a TCP port.
			args = append(args, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no", "--daemonize", "no", "--logfile", "")
			got, err := LoadConfig(args, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, args...)
			logfile, err := os.Create(filepath.Join(dir, "redis.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer logfile.Close()
			command.Stdout, command.Stderr = logfile, logfile
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var runErr error
			go func() { runErr = command.Wait(); close(done) }()
			t.Cleanup(func() { command.Process.Kill(); <-done })
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			var conn net.Conn
			for conn == nil {
				conn, err = net.DialTimeout("unix", socket, 10*time.Millisecond)
				if err == nil {
					break
				}
				select {
				case <-done:
					logs, _ := os.ReadFile(filepath.Join(dir, "redis.log"))
					t.Fatalf("Redis exited: %v\n%s", runErr, logs)
				case <-ctx.Done():
					t.Fatal("Redis startup timed out")
				case <-tick.C:
				}
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(time.Second))
			expectedDir := got.Dir
			if !filepath.IsAbs(expectedDir) {
				expectedDir = filepath.Join(dir, expectedDir)
			}
			// Redis reports a canonical working directory.
			expectedDir, err = filepath.EvalSymlinks(expectedDir)
			if err != nil {
				t.Fatal(err)
			}
			reader := resp.NewReader(conn)
			for key, want := range map[string]string{
				"hz":                          strconv.Itoa(got.HZ),
				"dynamic-hz":                  map[bool]string{true: "yes", false: "no"}[got.DynamicHZ],
				"port":                        strconv.Itoa(got.Port),
				"bind":                        strings.Join(got.Bind, " "),
				"appendfilename":              got.AppendFilename,
				"dir":                         expectedDir,
				"appendonly":                  "no",
				"appendfsync":                 got.AppendFsync,
				"appenddirname":               got.AppendDirname,
				"dbfilename":                  got.DBFilename,
				"aof-use-rdb-preamble":        map[bool]string{true: "yes", false: "no"}[got.AOFUseRDBPreamble],
				"stop-writes-on-bgsave-error": map[bool]string{true: "yes", false: "no"}[got.StopWritesOnBGSaveError],
			} {
				if _, err := io.WriteString(conn, "CONFIG GET "+key+"\r\n"); err != nil {
					t.Fatal(err)
				}
				reply, err := reader.Read()
				if err != nil {
					t.Fatal(err)
				}
				expected := resp.NewArray([]resp.Value{resp.NewBulkString(key), resp.NewBulkString(want)})
				if string(reply.Marshal()) != string(expected.Marshal()) {
					t.Errorf("%s: Redis=%q, Peridot=%q", key, reply.Marshal(), expected.Marshal())
				}
			}
		})
	}
}
