package server

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type failedAOFPersistence struct {
	failure     error
	preexisting bool
}

func (*failedAOFPersistence) Restore() map[string]storage.Entry {
	return map[string]storage.Entry{
		"live":           {Value: "value"},
		"get-expired":    {Value: "old", ExpiresAt: 1500},
		"ttl-expired":    {Value: "old", ExpiresAt: 1500},
		"digest-expired": {Value: "old", ExpiresAt: 1500},
	}
}
func (p *failedAOFPersistence) Append(string, storage.Entry, bool) error { return p.failure }
func (p *failedAOFPersistence) AppendDelete([]string) error              { return p.failure }
func (p *failedAOFPersistence) Err() error {
	if p.preexisting {
		return p.failure
	}
	return nil
}
func (*failedAOFPersistence) Rewrite(context.Context, map[string]storage.Entry) error { return nil }
func (*failedAOFPersistence) Close() error                                            { return nil }

func TestAOFFailureRepliesAndExpiredReadsPreservePipeline(t *testing.T) {
	for _, protocol := range []string{"2", "3"} {
		for _, preexisting := range []bool{false, true} {
			t.Run(protocol+"/"+map[bool]string{true: "known", false: "new"}[preexisting], func(t *testing.T) {
				cause := errors.Join(database.ErrAOFFailed, &os.PathError{Op: "write", Path: "private-aof-path", Err: syscall.EFBIG})
				p := &failedAOFPersistence{failure: cause, preexisting: preexisting}
				var millis atomic.Int64
				millis.Store(1000)
				backend := storage.New()
				db := database.New(backend, p, database.WithClock(func() time.Time { return time.UnixMilli(millis.Load()) }))
				defer db.Close()
				millis.Store(2000)
				_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(db, db)))
				reader := resp.NewReader(peer)
				exchange(t, peer, reader, 1, "HELLO", protocol)
				null := "$-1\r\n"
				if protocol == "3" {
					null = "_\r\n"
				}
				misconf := "-MISCONF Errors writing to the AOF file: File too large\r\n"
				// SET GET must emit only the disk error if append fails, not the old value
				// followed by an error (that would shift all remaining pipeline replies).
				want := misconf + misconf + misconf + "$5\r\nvalue\r\n" + null + ":-2\r\n" + null + "$5\r\nvalue\r\n"
				commands := [][]string{
					{"SET", "live", "replacement", "GET"}, {"DEL", "live"}, {"EXPIRE", "live", "100"},
					{"GET", "live"}, {"GET", "get-expired"}, {"TTL", "ttl-expired"}, {"DIGEST", "digest-expired"}, {"GET", "live"},
				}
				if preexisting {
					want += misconf + "-ERR wrong number of arguments for 'set' command\r\n" + misconf + "$5\r\nvalue\r\n"
					commands = append(commands, []string{"PING"}, []string{"SET"}, []string{"SET", "live", "x", "INVALID"}, []string{"GET", "live"})
				}
				persistencePipeline(t, peer, want, commands...)
				for _, key := range []string{"get-expired", "ttl-expired", "digest-expired"} {
					if _, exists := backend.Load(key); exists {
						t.Fatalf("expired key retained: %s", key)
					}
				}
			})
		}
	}
}
