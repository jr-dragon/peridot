package server

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func TestReadFastPathsAgainstRedis(t *testing.T) {
	for _, mode := range []string{"xsync", "database-active", "database-rdb", "database-aof"} {
		for _, protocol := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/RESP%d", mode, protocol), func(t *testing.T) {
				appendOnly := "no"
				if mode == "database-aof" {
					appendOnly = "yes"
				}
				ref := startRedisFiles(t, nil, "--appendonly", appendOnly)
				backend := storage.NewXsync()
				var store storage.KeyStore = backend
				if mode != "xsync" {
					var p database.Persistence
					if mode == "database-rdb" || mode == "database-aof" {
						config := data.DefaultConfig()
						config.Dir, config.Save, config.AppendOnly = t.TempDir(), nil, mode == "database-aof"
						config.AppendFsync = "always"
						var err error
						p, err = persistence.New(config)
						if err != nil {
							t.Fatal(err)
						}
					}
					db := database.New(backend, p, database.WithActiveExpiration(10, false))
					t.Cleanup(func() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					})
					store = db
				}
				_, conn := startRESPPipe(t, NewHandler(cmd.NewCommands(store, nil)))
				exchange(t, conn, resp.NewReader(conn), 1, "HELLO", fmt.Sprint(protocol))
				exchange(t, ref.conn, ref.reader, 1, "HELLO", fmt.Sprint(protocol))
				key, value := "key\x00\xff", "\x00\r\n\xff"
				requests := [][]string{
					{"SET", key, value}, {"GET", key}, {"TTL", key},
					{"SET", "expired", "old", "PXAT", "1"}, {"GET", "expired"}, {"TTL", "expired"},
					{"SET", "future", value, "PXAT", "4102444800000"}, {"GET", "future"},
					{"DEL", key, key, "expired", "missing"}, {"GET", key}, {"TTL", key},
					{"DEL", "future", "future"}, {"GET", "future"}, {"PING"},
				}
				var input []byte
				for _, args := range requests {
					input = append(input, stringRequest(args...)...)
				}
				null := "$-1\r\n"
				if protocol == 3 {
					null = "_\r\n"
				}
				bulk := "$4\r\n" + value + "\r\n"
				want := "+OK\r\n" + bulk + ":-1\r\n+OK\r\n" + null + ":-2\r\n+OK\r\n" + bulk + ":1\r\n" + null + ":-2\r\n:1\r\n" + null + "+PONG\r\n"
				for _, peer := range []struct {
					name string
					conn net.Conn
				}{{"Redis", ref.conn}, {"Peridot", conn}} {
					if err := peer.conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
						t.Fatal(err)
					}
					if _, err := peer.conn.Write(input); err != nil {
						t.Fatal(err)
					}
					wire := make([]byte, len(want))
					if _, err := io.ReadFull(peer.conn, wire); err != nil || string(wire) != want {
						t.Fatalf("%s pipeline = %q, %v; want %q", peer.name, wire, err, want)
					}
					if got := exchange(t, peer.conn, resp.NewReader(peer.conn), 1, "PING"); got != "+PONG\r\n" {
						t.Fatalf("%s follow-up = %q", peer.name, got)
					}
				}
			})
		}
	}
}
