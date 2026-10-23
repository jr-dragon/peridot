package server

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/pkg/resp"
)

// Read raw bytes so framing and connection reuse are checked together.
func persistencePipeline(t *testing.T, conn net.Conn, want string, commands ...[]string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var input []byte
	for _, args := range commands {
		input = append(input, stringRequest(args...)...)
	}
	if _, err := conn.Write(input); err != nil {
		t.Fatal(err)
	}
	actual := make([]byte, len(want))
	if _, err := io.ReadFull(conn, actual); err != nil || string(actual) != want {
		t.Fatalf("pipeline = %q, %v; want %q", actual, err, want)
	}
}

func TestRewriteStartFailureAgainstRedis(t *testing.T) {
	for _, protocol := range []string{"2", "3"} {
		t.Run(protocol, func(t *testing.T) {
			ref := startRedisFiles(t, nil, "--appendonly", "no")
			config := data.DefaultConfig()
			config.Dir, config.Save = t.TempDir(), nil
			db := openPersistenceDB(t, config)
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
			reader := resp.NewReader(peer)
			exchange(t, ref.conn, ref.reader, 1, "HELLO", protocol)
			exchange(t, peer, reader, 1, "HELLO", protocol)
			for _, dir := range []string{ref.dir, config.Dir} {
				if err := os.WriteFile(filepath.Join(dir, "appendonlydir"), []byte("not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			want := "-ERR Can't execute an AOF background rewriting. Please check the server logs for more information.\r\n+PONG\r\n+OK\r\n$3\r\na\x00b\r\n"
			commands := [][]string{{"BGREWRITEAOF"}, {"PING"}, {"SET", "key", "a\x00b"}, {"GET", "key"}}
			persistencePipeline(t, ref.conn, want, commands...)
			persistencePipeline(t, peer, want, commands...)
			for _, dir := range []string{ref.dir, config.Dir} {
				if err := os.Remove(filepath.Join(dir, "appendonlydir")); err != nil {
					t.Fatal(err)
				}
			}
			// Failed preparation must release the scheduler reservation for retry.
			want = "+Background append only file rewriting started\r\n+PONG\r\n"
			persistencePipeline(t, ref.conn, want, []string{"BGREWRITEAOF"}, []string{"PING"})
			persistencePipeline(t, peer, want, []string{"BGREWRITEAOF"}, []string{"PING"})
		})
	}
}

func TestForegroundSaveFailureAgainstRedis(t *testing.T) {
	for _, appendOnly := range []string{"no", "yes"} {
		t.Run(appendOnly, func(t *testing.T) {
			ref := startRedisFiles(t, nil, "--appendonly", appendOnly, "--save", "3600", "1")
			config := data.DefaultConfig()
			config.Dir = t.TempDir()
			config.Save = []data.SaveRule{{Seconds: 3600, Changes: 1}}
			config.AppendOnly, config.AppendFsync = appendOnly == "yes", "always"
			db := openPersistenceDB(t, config)
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommandsFromConfig(config, db)))
			paths := []string{filepath.Join(ref.dir, "dump.rdb"), filepath.Join(config.Dir, "dump.rdb")}
			for _, path := range paths {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			// Restore the destination before cleanup tries the final snapshot.
			t.Cleanup(func() {
				for _, path := range paths {
					_ = os.Remove(path)
				}
			})
			want := "-ERR\r\n+PONG\r\n+OK\r\n-ERR syntax error\r\n$5\r\nvalue\r\n"
			commands := [][]string{{"SAVE"}, {"PING"}, {"SET", "key", "value"}, {"SET", "key", "bad", "INVALID"}, {"GET", "key"}}
			persistencePipeline(t, ref.conn, want, commands...)
			persistencePipeline(t, peer, want, commands...)
			for _, path := range paths {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			want = "+OK\r\n+PONG\r\n+OK\r\n"
			commands = [][]string{{"SAVE"}, {"PING"}, {"SET", "key", "recovered"}}
			persistencePipeline(t, ref.conn, want, commands...)
			persistencePipeline(t, peer, want, commands...)
		})
	}
}
