package cmd

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/persistence/rdb"
	"github.com/jr-dragon/peridot/internal/storage"
)

func TestSnapshotCommandRepliesAndFailureRecovery(t *testing.T) {
	config := data.DefaultConfig()
	config.Dir = t.TempDir()
	p, err := persistence.New(config)
	if err != nil {
		t.Fatal(err)
	}
	db := database.New(storage.New(), p)
	defer db.Close()
	ping := &Ping{writeStatus: db}
	save, bgsave, lastsave := &Save{db}, &BGSave{db}, &LastSave{db}
	for _, tc := range []struct {
		c    Command
		args []string
		want string
	}{
		{save, []string{"SAVE", "extra"}, "-ERR wrong number of arguments for 'save' command\r\n"},
		{lastsave, []string{"LASTSAVE", "extra"}, "-ERR wrong number of arguments for 'lastsave' command\r\n"},
		{bgsave, []string{"BGSAVE", "bad"}, "-ERR syntax error\r\n"},
		{bgsave, []string{"BGSAVE", "SCHEDULE", "extra"}, "-ERR syntax error\r\n"},
		{save, []string{"SAVE"}, "+OK\r\n"},
	} {
		if got := commandReply(t, tc.c, tc.args...); got != tc.want {
			t.Fatalf("%v: %q, want %q", tc.args, got, tc.want)
		}
	}
	path := filepath.Join(config.Dir, config.DBFilename)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if got := commandReply(t, save, "SAVE"); got != "-ERR\r\n" {
		t.Fatal(got)
	}
	if got := commandReply(t, NewSet(db), "SET", "key", "value"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "+PONG\r\n"},
		{[]string{"PING", "payload"}, "$7\r\npayload\r\n"},
		{[]string{"PING", "one", "two"}, "-ERR wrong number of arguments for 'ping' command\r\n"},
	} {
		if got := commandReply(t, ping, tc.args...); got != tc.want {
			t.Fatalf("PING after foreground failure: %q, want %q", got, tc.want)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got := commandReply(t, save, "SAVE"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := commandReply(t, NewSet(db), "SET", "key", "value"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := commandReply(t, ping, "PING"); got != "+PONG\r\n" {
		t.Fatal(got)
	}
	if got := commandReply(t, bgsave, "BGSAVE", "schedule"); got != "+Background saving started\r\n" {
		t.Fatal(got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = persistence.New(config)
	if err != nil {
		t.Fatal(err)
	}
	restarted := database.New(storage.New(), p)
	defer restarted.Close()
	if e, ok, err := restarted.Get(context.Background(), "key"); err != nil || !ok || e.Value != "value" {
		t.Fatal(e, ok, err)
	}
}

func TestSameValueSetTriggersSnapshot(t *testing.T) {
	for _, options := range [][]string{nil, {"GET"}, {"XX", "KEEPTTL"}, {"IFEQ", "value"}, {"IFNE", "other"}} {
		t.Run(strings.Join(options, "-"), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config := data.DefaultConfig()
				config.Dir = t.TempDir()
				config.Save = []data.SaveRule{{Seconds: 1, Changes: 2}}
				p, err := persistence.New(config)
				if err != nil {
					t.Fatal(err)
				}
				db := database.New(storage.New(), p)
				defer db.Close()
				set := NewSet(db)
				if got := commandReply(t, set, "SET", "key", "value"); got != "+OK\r\n" {
					t.Fatal(got)
				}
				for _, args := range [][]string{
					{"SET", "key", "value", "NX"}, {"SET", "missing", "value", "XX"},
					{"SET", "key", "value", "IFEQ", "other"}, {"SET", "key", "value", "IFNE", "value"},
				} {
					if got := commandReply(t, set, args...); got != "$-1\r\n" {
						t.Fatal(got)
					}
				}
				time.Sleep(2 * time.Second)
				synctest.Wait()
				path := filepath.Join(config.Dir, config.DBFilename)
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("conditional no-ops triggered snapshot: %v", err)
				}
				args := append([]string{"SET", "key", "value"}, options...)
				want := "+OK\r\n"
				if slices.Contains(options, "GET") {
					want = "$5\r\nvalue\r\n"
				}
				if got := commandReply(t, set, args...); got != want {
					t.Fatal(got)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("same-value SET did not trigger snapshot: %v", err)
				}
				defer f.Close()
				state, err := rdb.Load(f)
				if err != nil || len(state) != 1 || state["key"].Value != "value" {
					t.Fatal(state, err)
				}
			})
		})
	}
}

func TestBackgroundSnapshotFailureAndCommandPriority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := data.DefaultConfig()
		config.Dir = t.TempDir()
		p, err := persistence.New(config)
		if err != nil {
			t.Fatal(err)
		}
		db := database.New(storage.New(), p)
		path := filepath.Join(config.Dir, config.DBFilename)
		defer func() {
			_ = os.Remove(path)
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()
		set, ping := NewSet(db), &Ping{writeStatus: db}
		if got := commandReply(t, set, "SET", "key", "before"); got != "+OK\r\n" {
			t.Fatal(got)
		}
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		bgsave := &BGSave{db}
		if got := commandReply(t, bgsave, "BGSAVE"); got != "+Background saving started\r\n" {
			t.Fatal(got)
		}
		synctest.Wait()
		const misconf = "-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n"
		if got := commandReply(t, ping, "PING"); got != misconf {
			t.Fatal(got)
		}
		if got := commandReply(t, &Save{db}, "SAVE"); got != "-ERR\r\n" {
			t.Fatal(got)
		}
		for _, tc := range []struct {
			command Command
			args    []string
			want    string
		}{
			{ping, []string{"PING"}, misconf},
			{ping, []string{"PING", "one", "two"}, misconf},
			{set, []string{"SET", "key"}, "-ERR wrong number of arguments for 'set' command\r\n"},
			{set, []string{"SET", "key", "value"}, misconf},
			{set, []string{"SET", "key", "value", "INVALID"}, misconf},
			{set, []string{"SET", "key", "value", "NX", "XX"}, misconf},
			{set, []string{"SET", "key", "value", "EX"}, misconf},
			{set, []string{"SET", "key", "value", "PX", "bad"}, misconf},
			{set, []string{"SET", "key", "value", "EX", "0"}, misconf},
			{NewGet(db), []string{"GET", "key"}, "$6\r\nbefore\r\n"},
		} {
			if got := commandReply(t, tc.command, tc.args...); got != tc.want {
				t.Fatalf("%v: got %q, want %q", tc.args, got, tc.want)
			}
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if got := commandReply(t, bgsave, "BGSAVE"); got != "+Background saving started\r\n" {
			t.Fatal(got)
		}
		synctest.Wait()
		if got := commandReply(t, ping, "PING"); got != "+PONG\r\n" {
			t.Fatalf("successful BGSAVE did not clear failure: %q", got)
		}
		if got := commandReply(t, set, "SET", "key", "value", "INVALID"); got != "-ERR syntax error\r\n" {
			t.Fatal(got)
		}
	})
}
