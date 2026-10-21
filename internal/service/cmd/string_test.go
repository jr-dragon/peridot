package cmd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func commandReply(t *testing.T, command Command, args ...string) string {
	t.Helper()
	bulk := make([]resp.BulkString, len(args))
	for i, arg := range args {
		bulk[i] = resp.NewBulkString(arg)
	}
	value, err := command.Exec(context.Background(), bulk)
	if err != nil {
		if !errors.Is(err, ErrCommand) {
			t.Fatalf("unexpected internal error: %v", err)
		}
		if value == nil && !errors.As(err, &value) {
			t.Fatalf("command error has no reply: %v", err)
		}
	} else if value == nil {
		value = resp.NullBulkStringValue
	}
	return string(value.Marshal())
}

func seed(t *testing.T, store storage.Store, key string, entry storage.Entry) {
	t.Helper()
	if err := store.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
		return entry, true, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGetSet(t *testing.T) {
	forEachStringStore(t, testGetSet)
}

func testGetSet(t *testing.T, newStore func() storage.Store) {
	const wrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	for _, tc := range []struct {
		name    string
		initial any
		args    []string
		want    string
		stored  any
	}{
		{"get missing", nil, []string{"GET", "key"}, "$-1\r\n", nil},
		{"get empty", "", []string{"GET", "key"}, "$0\r\n\r\n", ""},
		{"get binary", "\x00\r\n\xff", []string{"GET", "key"}, "$4\r\n\x00\r\n\xff\r\n", "\x00\r\n\xff"},
		{"get arity", nil, []string{"GET"}, "-ERR wrong number of arguments for 'get' command\r\n", nil},
		{"get extra arg", nil, []string{"GET", "key", "extra"}, "-ERR wrong number of arguments for 'get' command\r\n", nil},
		{"get wrong type", 1, []string{"GET", "key"}, wrongType, 1},
		{"set arity", nil, []string{"SET", "key"}, "-ERR wrong number of arguments for 'set' command\r\n", nil},
		{"set new", nil, []string{"SET", "key", "new"}, "+OK\r\n", "new"},
		{"set overwrite", "old", []string{"SET", "key", "new"}, "+OK\r\n", "new"},
		{"set overwrite type", 1, []string{"SET", "key", "new"}, "+OK\r\n", "new"},
		{"set get type", 1, []string{"SET", "key", "new", "GET"}, wrongType, 1},
		{"get before nx", 1, []string{"SET", "key", "new", "GET", "NX"}, wrongType, 1},
		{"get old", "old", []string{"SET", "key", "new", "GET"}, "$3\r\nold\r\n", "new"},
		{"get absent", nil, []string{"SET", "key", "new", "GET"}, "$-1\r\n", "new"},
		{"nx present", "old", []string{"SET", "key", "new", "NX"}, "$-1\r\n", "old"},
		{"nx absent", nil, []string{"SET", "key", "new", "nx"}, "+OK\r\n", "new"},
		{"xx absent", nil, []string{"SET", "key", "new", "XX"}, "$-1\r\n", nil},
		{"xx present", "old", []string{"SET", "key", "new", "XX"}, "+OK\r\n", "new"},
		{"nx get failure", "old", []string{"SET", "key", "new", "NX", "GET"}, "$3\r\nold\r\n", "old"},
		{"ifeq succeeds", "old", []string{"SET", "key", "new", "IFEQ", "old"}, "+OK\r\n", "new"},
		{"ifeq fails", "old", []string{"SET", "key", "new", "IFEQ", "OLD", "GET"}, "$3\r\nold\r\n", "old"},
		{"ifeq missing", nil, []string{"SET", "key", "new", "IFEQ", "old"}, "$-1\r\n", nil},
		{"ifne missing", nil, []string{"SET", "key", "new", "IFNE", "old"}, "+OK\r\n", "new"},
		{"ifne fails", "old", []string{"SET", "key", "new", "IFNE", "old"}, "$-1\r\n", "old"},
		{"ifne succeeds", "old", []string{"SET", "key", "new", "IFNE", "other"}, "+OK\r\n", "new"},
		{"condition wrong type", 1, []string{"SET", "key", "new", "IFEQ", "1"}, wrongType, 1},
		{"digest absent equal", nil, []string{"SET", "key", "new", "IFDEQ", "bad"}, "$-1\r\n", nil},
		{"digest absent unequal", nil, []string{"SET", "key", "new", "IFDNE", "bad"}, "+OK\r\n", "new"},
		{"digest invalid length", "old", []string{"SET", "key", "new", "IFDEQ", "bad"}, "-ERR must be exactly 16 hexadecimal characters\r\n", "old"},
		{"digest error after get", "old", []string{"SET", "key", "new", "IFDEQ", "bad", "GET"}, "$3\r\nold\r\n-ERR must be exactly 16 hexadecimal characters\r\n", "old"},
		{"digest invalid hex", "old", []string{"SET", "key", "new", "IFDNE", "zzzzzzzzzzzzzzzz"}, "+OK\r\n", "new"},
		{"repeated same condition", "old", []string{"SET", "key", "new", "IFEQ", "wrong", "IFEQ", "old"}, "+OK\r\n", "new"},
		{"repeated get nx", "old", []string{"SET", "key", "new", "GET", "NX", "GET", "NX"}, "$3\r\nold\r\n", "old"},
		{"conflicting condition", "old", []string{"SET", "key", "new", "NX", "XX"}, "-ERR syntax error\r\n", "old"},
		{"conflicting expiration", "old", []string{"SET", "key", "new", "EX", "1", "PX", "1"}, "-ERR syntax error\r\n", "old"},
		{"missing expiration", "old", []string{"SET", "key", "new", "EX"}, "-ERR syntax error\r\n", "old"},
		{"missing condition value", "old", []string{"SET", "key", "new", "IFEQ"}, "-ERR syntax error\r\n", "old"},
		{"unknown option", "old", []string{"SET", "key", "new", "INVALID"}, "-ERR syntax error\r\n", "old"},
		{"past absolute", "old", []string{"SET", "key", "new", "PXAT", "1", "GET"}, "$3\r\nold\r\n", nil},
		{"past absolute blocked", "old", []string{"SET", "key", "new", "PXAT", "1", "NX"}, "$-1\r\n", "old"},
		{"expiry before wrongtype", 1, []string{"SET", "key", "new", "EX", "0", "GET"}, "-ERR invalid expire time in 'set' command\r\n", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore()
			if tc.initial != nil {
				seed(t, store, "key", storage.Entry{Value: tc.initial})
			}
			var command Command = NewSet(store)
			if tc.args[0] == "GET" {
				command = NewGet(store)
			}
			if got := commandReply(t, command, tc.args...); got != tc.want {
				t.Fatalf("reply = %q, want %q", got, tc.want)
			}
			entry, exists, err := store.Get(context.Background(), "key")
			if err != nil || exists != (tc.stored != nil) || entry.Value != tc.stored {
				t.Fatalf("stored = %v, %v, %v, want %v", entry, exists, err, tc.stored)
			}
		})
	}
}

func TestSetExpirationValidation(t *testing.T) {
	const now = int64(1700000000000)
	for _, tc := range []struct {
		option, ttl string
		want        int64
		errorText   string
	}{
		{"EX", "1", now + 1000, ""}, {"PX", "1", now + 1, ""},
		{"EXAT", "1", 1000, ""}, {"PXAT", "1", 1, ""},
		{"PXAT", strconv.FormatInt(math.MaxInt64, 10), math.MaxInt64, ""},
		{"EXAT", "9223372036854775", 9223372036854775000, ""},
		{"EXAT", "9223372036854776", 0, "invalid expire time"},
		{"PX", strconv.FormatInt(math.MaxInt64-now, 10), math.MaxInt64, ""},
		{"PX", strconv.FormatInt(math.MaxInt64-now+1, 10), math.MinInt64, ""},
		{"EX", "9223372036854775807", 0, "invalid expire time"},
		{"PX", "0", 0, "invalid expire time"}, {"PX", "-1", 0, "invalid expire time"},
		{"PX", "-9223372036854775808", 0, "invalid expire time"},
		{"PX", "+1", 0, "value is not an integer"}, {"PX", "01", 0, "value is not an integer"},
		{"PX", "-0", 0, "value is not an integer"}, {"PX", "1.0", 0, "value is not an integer"},
		{"PX", " 1", 0, "value is not an integer"}, {"PX", "1\x00", 0, "value is not an integer"},
		{"PX", "9223372036854775808", 0, "value is not an integer"},
	} {
		t.Run(fmt.Sprintf("%s/%s", tc.option, tc.ttl), func(t *testing.T) {
			got, err := (setOptions{expiration: tc.option, ttl: tc.ttl}).expiresAt(now)
			if tc.errorText == "" {
				if err != nil || got != tc.want {
					t.Fatalf("expiresAt = %d, %v; want %d", got, err, tc.want)
				}
			} else if !errors.Is(err, ErrCommand) || !strings.Contains(err.Error(), tc.errorText) {
				t.Fatalf("error = %v, want %q", err, tc.errorText)
			}
		})
	}
}

func TestSetTTLPreservation(t *testing.T) {
	forEachStringStore(t, testSetTTLPreservation)
}

func testSetTTLPreservation(t *testing.T, newStore func() storage.Store) {
	for _, options := range [][]string{nil, {"GET"}, {"KEEPTTL"}, {"KEEPTTL", "GET"}, {"NX"}, {"IFEQ", "different"}, {"EX", "10"}, {"PX", "10000"}, {"EXAT", "4102444800"}, {"PXAT", "4102444800000"}, {"EX", "bad", "EX", "10"}} {
		t.Run(strings.Join(options, " "), func(t *testing.T) {
			store := newStore()
			deadline := time.Now().Add(time.Hour).UnixMilli()
			seed(t, store, "key", storage.Entry{Value: "old", ExpiresAt: deadline})
			before := time.Now().UnixMilli()
			commandReply(t, NewSet(store), append([]string{"SET", "key", "new"}, options...)...)
			after := time.Now().UnixMilli()
			entry, _, _ := store.Get(context.Background(), "key")
			switch {
			case len(options) == 0 || options[0] == "GET":
				if entry.ExpiresAt != 0 {
					t.Fatal("SET retained expiration")
				}
			case options[0] == "KEEPTTL" || options[0] == "NX" || options[0] == "IFEQ":
				if entry.ExpiresAt != deadline {
					t.Fatal("SET changed retained expiration")
				}
			case options[0] == "EXAT" || options[0] == "PXAT":
				if entry.ExpiresAt != 4102444800000 {
					t.Fatalf("absolute expiration = %d", entry.ExpiresAt)
				}
			default:
				if entry.ExpiresAt < before+10000 || entry.ExpiresAt > after+10000 {
					t.Fatalf("relative expiration = %d", entry.ExpiresAt)
				}
			}
		})
	}
}

func TestSetConcurrentConditions(t *testing.T) {
	forEachStringStore(t, testSetConcurrentConditions)
}

func testSetConcurrentConditions(t *testing.T, newStore func() storage.Store) {
	for _, condition := range [][]string{{"NX"}, {"IFEQ", "old"}, {"IFDEQ", "0f3ca07a8cc8bf40"}} {
		t.Run(condition[0], func(t *testing.T) {
			store := newStore()
			if condition[0] != "NX" {
				seed(t, store, "key", storage.Entry{Value: "old"})
			}
			command := NewSet(store)
			var successes atomic.Int64
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() {
					if commandReply(t, command, append([]string{"SET", "key", "new"}, condition...)...) == "+OK\r\n" {
						successes.Add(1)
					}
				})
			}
			wg.Wait()
			if successes.Load() != 1 {
				t.Fatalf("successful writes = %d, want 1", successes.Load())
			}
		})
	}
}

func forEachStringStore(t *testing.T, run func(*testing.T, func() storage.Store)) {
	t.Helper()
	t.Run("xsync", func(t *testing.T) { run(t, func() storage.Store { return storage.NewXsync() }) })
	t.Run("dlht", func(t *testing.T) { run(t, func() storage.Store { return storage.NewDLHT() }) })
}
