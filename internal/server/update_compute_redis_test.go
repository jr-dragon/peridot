package server

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

// Concurrent contenders need not have the same winner on the two servers, but
// each server must have exactly one successful conditional write and retain
// that winner's binary-safe value and expiration.
func TestConcurrentConditionalUpdatesAgainstRedis(t *testing.T) {
	for _, mode := range []string{"memory", "active", "rdb", "aof"} {
		for _, protocol := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/RESP%d", mode, protocol), func(t *testing.T) {
				appendOnly := "no"
				if mode == "aof" {
					appendOnly = "yes"
				}
				ref := startRedisFiles(t, nil, "--appendonly", appendOnly)
				backend := storage.NewXsync()
				var p database.Persistence
				if mode == "rdb" || mode == "aof" {
					config := data.DefaultConfig()
					config.Dir, config.Save, config.AppendOnly = t.TempDir(), nil, mode == "aof"
					config.AppendFsync = "always"
					var err error
					p, err = persistence.New(config)
					if err != nil {
						t.Fatal(err)
					}
				}
				var options []database.Option
				if mode != "memory" {
					options = append(options, database.WithActiveExpiration(10, false))
				}
				db := database.New(backend, p, options...)
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				handler := NewHandler(cmd.NewCommands(db, nil))
				_, control := startRESPPipe(t, handler)
				for _, peer := range []struct {
					name    string
					control net.Conn
					connect func() net.Conn
				}{
					{"Peridot", control, func() net.Conn { _, conn := startRESPPipe(t, handler); return conn }},
					{"Redis", ref.conn, func() net.Conn {
						conn, err := net.DialTimeout("unix", filepath.Join(ref.dir, "redis.sock"), time.Second)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = conn.Close() })
						return conn
					}},
				} {
					t.Run(peer.name, func(t *testing.T) {
						reader := resp.NewReader(peer.control)
						for _, scenario := range []string{"expired-nx", "live-nx", "ifeq-keepttl"} {
							key := "conditional\x00\xff:" + scenario
							initial := []string{"SET", key, "old"}
							deadline := int64(0)
							if scenario == "expired-nx" {
								initial = append(initial, "PXAT", "1")
							}
							if scenario == "ifeq-keepttl" {
								initial = append(initial, "PXAT", "4102444800000")
								deadline = 4102444800000
							}
							if got := exchange(t, peer.control, reader, 1, initial...); got != "+OK\r\n" {
								t.Fatal(got)
							}
							const contenders = 16
							conns := make([]net.Conn, contenders)
							for i := range conns {
								conns[i] = peer.connect()
								exchange(t, conns[i], resp.NewReader(conns[i]), 1, "HELLO", fmt.Sprint(protocol))
							}
							replies := make([]string, contenders)
							start := make(chan struct{})
							var wg sync.WaitGroup
							for i, conn := range conns {
								wg.Go(func() {
									<-start
									args := []string{"SET", key, fmt.Sprintf("value\x00\xff:%d", i), "NX"}
									if scenario == "ifeq-keepttl" {
										args = []string{"SET", key, fmt.Sprintf("value\x00\xff:%d", i), "IFEQ", "old", "KEEPTTL"}
									}
									replies[i] = exchange(t, conn, resp.NewReader(conn), 1, args...)
								})
							}
							close(start)
							wg.Wait()
							null := "$-1\r\n"
							if protocol == 3 {
								null = "_\r\n"
							}
							wins, value := 0, "old"
							for i, reply := range replies {
								if reply == "+OK\r\n" {
									wins++
									value = fmt.Sprintf("value\x00\xff:%d", i)
								} else if reply != null {
									t.Fatalf("%s reply = %q", scenario, reply)
								}
							}
							wantWins := 1
							if scenario == "live-nx" {
								wantWins = 0
							}
							if wins != wantWins {
								t.Fatalf("%s winners = %d, want %d", scenario, wins, wantWins)
							}
							if got := exchange(t, peer.control, reader, 1, "GET", key); got != string(resp.NewBulkString(value).Marshal()) {
								t.Fatal(got)
							}
							if peer.name == "Peridot" {
								if entry, exists := backend.Load(key); !exists || entry.ExpiresAt != deadline {
									t.Fatal(entry, exists)
								}
							} else {
								if deadline == 0 {
									deadline = -1
								}
								if got := exchange(t, peer.control, reader, 1, "PEXPIRETIME", key); got != fmt.Sprintf(":%d\r\n", deadline) {
									t.Fatal(got)
								}
							}
							if got := exchange(t, peer.control, reader, 1, "PING"); got != "+PONG\r\n" {
								t.Fatal(got)
							}
						}
					})
				}
			})
		}
	}
}
