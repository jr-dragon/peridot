package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

func TestConfiguredActiveExpirationPersistsDEL(t *testing.T) {
	for _, dynamic := range []string{"no", "yes"} {
		t.Run(dynamic, func(t *testing.T) {
			dir := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				config, err := data.LoadConfig([]string{"-"}, strings.NewReader(
					"hz 2\ndynamic-hz "+dynamic+"\nappendonly yes\nappendfsync no\nsave \"\"\ndir \""+dir+"\"\n"))
				if err != nil {
					t.Fatal(err)
				}
				app, err := NewApp(config)
				if err != nil {
					t.Fatal(err)
				}
				defer app.database.Close()
				synctest.Wait()
				// First empty cycle selects 1 Hz in dynamic mode, or retains 2 Hz.
				time.Sleep(500 * time.Millisecond)
				synctest.Wait()
				key := "unread\x00\xff"
				err = app.database.Update(context.Background(), key, func(_ storage.Entry, _ bool, now int64) (storage.Entry, bool, error) {
					return storage.Entry{Value: "value", ExpiresAt: now + 1}, true, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				files, err := filepath.Glob(filepath.Join(config.Dir, config.AppendDirname, "*.incr.aof"))
				if err != nil || len(files) != 1 {
					t.Fatal(files, err)
				}
				assertDeleted := func(want bool) {
					t.Helper()
					contents, err := os.ReadFile(files[0])
					if err != nil {
						t.Fatal(err)
					}
					if got := bytes.HasSuffix(contents, aof.Encode("DEL", key)); got != want {
						t.Fatalf("durable DEL=%v, want %v: %q", got, want, contents)
					}
					state, err := aof.Load(bytes.NewReader(contents))
					if err != nil {
						t.Fatal(err)
					}
					if _, exists := state[key]; exists == want {
						t.Fatal("AOF replay disagrees with deletion")
					}
				}
				time.Sleep(499 * time.Millisecond)
				synctest.Wait()
				assertDeleted(false)
				time.Sleep(time.Millisecond)
				synctest.Wait()
				assertDeleted(dynamic == "no")
				if dynamic == "yes" {
					time.Sleep(500 * time.Millisecond)
					synctest.Wait()
					assertDeleted(true)
				}
				if err := app.database.Close(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}
