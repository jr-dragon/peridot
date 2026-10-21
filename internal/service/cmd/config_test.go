package cmd

import (
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/storage"
)

func TestNewCommandsFromConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		appendOnly bool
		want       string
	}{
		{"rdb", false, "+Background append only file rewriting started\r\n"},
		{"aof", true, "+Background append only file rewriting started\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := data.DefaultConfig()
			config.AppendOnly, config.Dir, config.AppendFilename = tc.appendOnly, t.TempDir(), "data.aof"
			config.Save = nil
			p, err := persistence.New(config)
			if err != nil {
				t.Fatal(err)
			}
			db := database.New(storage.New(), p)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			commands := make(map[string]Command)
			for _, command := range NewCommandsFromConfig(config, db) {
				commands[command.Name()] = command
			}
			for _, name := range []string{"PING", "HELLO", "GET", "SET", "SELECT", "DEL", "TTL", "EXPIRE", "DIGEST", "BGREWRITEAOF"} {
				if commands[name] == nil {
					t.Fatalf("missing command %s", name)
				}
			}
			if got := commandReply(t, commands["SET"], "SET", "key", "value"); got != "+OK\r\n" {
				t.Fatalf("SET reply = %q", got)
			}
			if got := commandReply(t, commands["GET"], "GET", "key"); got != "$5\r\nvalue\r\n" {
				t.Fatalf("GET reply = %q", got)
			}
			if got := commandReply(t, commands["BGREWRITEAOF"], "BGREWRITEAOF"); got != tc.want {
				t.Fatalf("BGREWRITEAOF reply = %q, want %q", got, tc.want)
			}
		})
	}
}
