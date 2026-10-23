package cmd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

type keyspacePersistence struct {
	err    error
	writes int
}

func (*keyspacePersistence) Restore() map[string]storage.Entry                       { return nil }
func (p *keyspacePersistence) Append(string, storage.Entry, bool) error              { p.writes++; return p.err }
func (p *keyspacePersistence) AppendDelete([]string) error                           { p.writes++; return p.err }
func (p *keyspacePersistence) Err() error                                            { return p.err }
func (*keyspacePersistence) Rewrite(context.Context, map[string]storage.Entry) error { return nil }
func (*keyspacePersistence) Close() error                                            { return nil }

func TestSelectAlwaysRejected(t *testing.T) {
	for _, index := range []string{"0", "1", "-1", "invalid", "\x00"} {
		if got := commandReply(t, &Select{}, "SELECT", index); got != "-ERR SELECT is not allowed: Peridot does not support multiple databases\r\n" {
			t.Fatal(got)
		}
	}
}

func TestKeyspaceArity(t *testing.T) {
	store := storage.NewXsync()
	for _, c := range []Command{&Select{}, NewDel(store), NewTTL(store), NewExpire(store), NewDigest(store)} {
		for _, args := range [][]string{{c.Name()}, {c.Name(), "key", "1", "extra"}} {
			if len(args) > 1 && (c.Name() == "DEL" || c.Name() == "EXPIRE") {
				continue
			}
			if got := commandReply(t, c, args...); got != string(newWrongNumberOfArgumentsError(c).Marshal()) {
				t.Fatalf("%v: %q", args, got)
			}
		}
	}
}

func TestKeyspaceCommands(t *testing.T) {
	forEachKeyspaceStore(t, func(t *testing.T, newStore func() storage.KeyStore) {
		store := newStore()
		seed(t, store, "binary\x00\xff", storage.Entry{Value: "Hello world"})
		seed(t, store, "other", storage.Entry{Value: 42})
		for _, tc := range []struct {
			command Command
			args    []string
			want    string
		}{
			{NewTTL(store), []string{"TTL", "missing"}, ":-2\r\n"},
			{NewTTL(store), []string{"TTL", "other"}, ":-1\r\n"},
			{NewDigest(store), []string{"DIGEST", "missing"}, "$-1\r\n"},
			{NewDigest(store), []string{"DIGEST", "other"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
			{NewDigest(store), []string{"DIGEST", "binary\x00\xff"}, "$16\r\nb6acb9d84a38ff74\r\n"},
			{NewExpire(store), []string{"EXPIRE", "other", "3600", "NX"}, ":1\r\n"},
			{NewExpire(store), []string{"EXPIRE", "other", "0", "NX"}, ":0\r\n"},
			{NewDel(store), []string{"DEL", "binary\x00\xff", "missing", "binary\x00\xff", "other"}, ":2\r\n"},
			{NewDel(store), []string{"DEL", "other"}, ":0\r\n"},
		} {
			if got := commandReply(t, tc.command, tc.args...); got != tc.want {
				t.Fatalf("%q: %q, want %q", tc.args, got, tc.want)
			}
		}
	})
}

func TestKeyspaceExpirationSnapshot(t *testing.T) {
	const now = int64(1700000000000)
	for _, backend := range []storage.Backend{storage.NewXsync()} {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			p := &keyspacePersistence{}
			db := database.New(backend, p, database.WithClock(func() time.Time { return time.UnixMilli(now) }))
			t.Cleanup(func() { db.Close() })
			for _, tc := range []struct{ deadline, want int64 }{{0, -1}, {now - 1, -2}, {now, 0}, {now + 499, 0}, {now + 500, 1}, {now + 1499, 1}, {now + 1500, 2}} {
				backend.Put("key", storage.Entry{Value: "value", ExpiresAt: tc.deadline})
				if got := commandReply(t, NewTTL(db), "TTL", "key"); got != fmt.Sprintf(":%d\r\n", tc.want) {
					t.Fatalf("deadline %d: %q", tc.deadline, got)
				}
			}
			for _, tc := range []struct {
				deadline int64
				seconds  string
				opts     []string
				want     int64
			}{
				{0, "10", nil, 1}, {0, "10", []string{"GT"}, 0}, {0, "10", []string{"LT"}, 1},
				{0, "10", []string{"XX"}, 0}, {0, "10", []string{"NX", "NX"}, 1},
				{now + 10000, "10", []string{"GT"}, 0}, {now + 10000, "10", []string{"LT"}, 0},
				{now + 10000, "20", []string{"XX", "GT"}, 1}, {now + 10000, "5", []string{"XX", "LT"}, 1},
				{now + 10000, "0", []string{"NX"}, 0}, {now + 10000, "0", []string{"LT"}, 1},
				{0, "-9223372036854775", nil, 1}, {now - 1, "10", nil, 0},
			} {
				backend.Put("key", storage.Entry{Value: "value", ExpiresAt: tc.deadline})
				args := append([]string{"EXPIRE", "key", tc.seconds}, tc.opts...)
				if got := commandReply(t, NewExpire(db), args...); got != fmt.Sprintf(":%d\r\n", tc.want) {
					t.Fatalf("%v deadline %d: %q", args, tc.deadline, got)
				}
				e, exists := backend.Load("key")
				if tc.want == 1 && tc.seconds == "0" && exists {
					t.Fatal("zero expiry retained key")
				}
				if tc.want == 0 && tc.deadline >= now && e.ExpiresAt != tc.deadline {
					t.Fatal("failed condition changed expiry")
				}
			}
			backend.Put("expired", storage.Entry{Value: "value", ExpiresAt: now - 1})
			backend.Put("live", storage.Entry{Value: "value", ExpiresAt: now})
			if got := commandReply(t, NewDel(db), "DEL", "expired", "live", "live"); got != ":1\r\n" {
				t.Fatal(got)
			}
		})
	}
}

func TestKeyspacePersistenceFailures(t *testing.T) {
	p := &keyspacePersistence{}
	backend := storage.New()
	db := database.New(backend, p)
	defer db.Close()
	backend.Put("key", storage.Entry{Value: "Hello world"})
	p.err = database.ErrSnapshotFailed
	for _, c := range []Command{NewDel(db), NewExpire(db)} {
		args := []string{c.Name(), "key"}
		if c.Name() == "EXPIRE" {
			args = append(args, "bad", "INVALID")
		}
		bulk := make([]resp.BulkString, len(args))
		for i, a := range args {
			bulk[i] = resp.NewBulkString(a)
		}
		_, err := c.Exec(context.Background(), bulk)
		if !errors.Is(err, ErrCommand) || !errors.Is(err, database.ErrSnapshotFailed) {
			t.Fatal(err)
		}
	}
	if got := commandReply(t, NewTTL(db), "TTL", "key"); got != ":-1\r\n" {
		t.Fatal(got)
	}
	if got := commandReply(t, NewDigest(db), "DIGEST", "key"); got != "$16\r\nb6acb9d84a38ff74\r\n" {
		t.Fatal(got)
	}
	if p.writes != 0 {
		t.Fatal("reads or rejected writes appended to persistence")
	}
	if _, ok := backend.Load("key"); !ok {
		t.Fatal("failed deletion changed memory")
	}
}

func TestKeyspaceConcurrentConditions(t *testing.T) {
	forEachKeyspaceStore(t, func(t *testing.T, newStore func() storage.KeyStore) {
		store := newStore()
		seed(t, store, "key", storage.Entry{Value: "value"})
		c := NewExpire(store)
		var successes atomic.Int64
		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() {
				if commandReply(t, c, "EXPIRE", "key", "3600", "NX") == ":1\r\n" {
					successes.Add(1)
				}
			})
		}
		wg.Wait()
		if successes.Load() != 1 {
			t.Fatal(successes.Load())
		}
		for i := range 100 {
			seed(t, store, fmt.Sprint(i), storage.Entry{Value: "value"})
		}
		args := []string{"DEL"}
		for i := range 100 {
			args = append(args, fmt.Sprint(i))
		}
		for range 16 {
			wg.Go(func() {
				got := commandReply(t, NewDel(store), args...)
				if got != ":0\r\n" && got != ":100\r\n" {
					t.Errorf("partial concurrent deletion: %q", got)
				}
			})
		}
		wg.Wait()
	})
}

func forEachKeyspaceStore(t *testing.T, run func(*testing.T, func() storage.KeyStore)) {
	t.Helper()
	forEachStringStore(t, run)
	for _, durable := range []bool{false, true} {
		for name, factory := range map[string]func() storage.Backend{"xsync": func() storage.Backend { return storage.NewXsync() }} {
			t.Run(fmt.Sprintf("database-%s-durable-%t", name, durable), func(t *testing.T) {
				run(t, func() storage.KeyStore {
					var p database.Persistence
					if durable {
						p = &keyspacePersistence{}
					}
					db := database.New(factory(), p)
					t.Cleanup(func() { db.Close() })
					return db
				})
			})
		}
	}
}

func TestExpireValidation(t *testing.T) {
	c := NewExpire(storage.NewXsync())
	for _, tc := range []struct {
		args    []string
		message string
	}{
		{[]string{"1", "NX", "XX"}, "NX and XX, GT or LT options at the same time are not compatible"},
		{[]string{"1", "NX", "GT"}, "NX and XX, GT or LT options at the same time are not compatible"},
		{[]string{"1", "NX", "LT"}, "NX and XX, GT or LT options at the same time are not compatible"},
		{[]string{"1", "GT", "LT"}, "GT and LT options at the same time are not compatible"},
		{[]string{"bad", "INVALID"}, "Unsupported option INVALID"},
		{[]string{"bad"}, "value is not an integer or out of range"},
		{[]string{"01"}, "value is not an integer or out of range"},
		{[]string{"+1"}, "value is not an integer or out of range"},
		{[]string{"-0"}, "value is not an integer or out of range"},
		{[]string{"9223372036854775808"}, "value is not an integer or out of range"},
		{[]string{"9223372036854776"}, "invalid expire time in 'expire' command"},
		{[]string{"-9223372036854776"}, "invalid expire time in 'expire' command"},
		{[]string{"9223372036854775"}, "invalid expire time in 'expire' command"},
	} {
		args := append([]string{"EXPIRE", "missing"}, tc.args...)
		if got := commandReply(t, c, args...); got != "-ERR "+tc.message+"\r\n" {
			t.Fatalf("%q: %q", args, got)
		}
	}
}
