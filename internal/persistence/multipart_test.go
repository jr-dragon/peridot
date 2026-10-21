package persistence

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/persistence/rdb"
	"github.com/jr-dragon/peridot/internal/storage"
)

func openMultipart(t *testing.T, dir string, preamble bool) *MultipartAOF {
	t.Helper()
	s, err := OpenMultipartAOF(dir, "appendonly.aof", "", "always", preamble)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestMultipartRewriteBoundaryAndFailedRewrite(t *testing.T) {
	for _, preamble := range []bool{false, true} {
		for _, abort := range []bool{false, true} {
			t.Run(map[bool]string{true: "rdb", false: "plain"}[preamble]+map[bool]string{true: "-abort", false: "-complete"}[abort], func(t *testing.T) {
				dir := t.TempDir()
				s := openMultipart(t, dir, preamble)
				first := storage.Entry{Value: "before", ExpiresAt: 4102444800000}
				if err := s.Append("key", first, true); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				finish, err := s.BeginRewrite(ctx, map[string]storage.Entry{"key": first})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.BeginRewrite(ctx, nil); !errors.Is(err, database.ErrRewriteInProgress) {
					t.Fatal(err)
				}
				oldManifest, err := os.ReadFile(s.manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				if len(s.manifest.Incr) != 2 {
					t.Fatal("rotation did not preserve prior increments")
				}
				// Writes must finish while the checkpoint has not started its base I/O.
				if err := s.Append("key", storage.Entry{}, false); err != nil {
					t.Fatal(err)
				}
				replacement := storage.Entry{Value: "after\x00\xff"}
				if err := s.Append("key", replacement, true); err != nil {
					t.Fatal(err)
				}
				if err := s.Append("tail", first, true); err != nil {
					t.Fatal(err)
				}
				if abort {
					cancel()
				}
				err = finish()
				if abort {
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					current, _ := os.ReadFile(s.manifestPath)
					if !bytes.Equal(current, oldManifest) {
						t.Fatal("failed rewrite replaced manifest")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				restored := openMultipart(t, dir, preamble)
				want := map[string]storage.Entry{"key": replacement, "tail": first}
				if got := restored.Restore(); !reflect.DeepEqual(got, want) {
					t.Fatalf("restored %#v, want %#v", got, want)
				}
				if err := restored.Rewrite(context.Background(), want); err != nil {
					t.Fatal(err)
				}
				if len(restored.manifest.Incr) != 1 {
					t.Fatal("rewrite did not compact old increments")
				}
			})
		}
	}
}
func TestManifestValidation(t *testing.T) {
	for _, input := range []string{"", "\n", "file base seq 1 type b", "file ../bad seq 1 type b\n", "file a seq 1 type b\nfile b seq 2 type b\n", "file a seq 2 type i\nfile b seq 1 type i\n", "file a seq 1 type b\nfile a seq 1 type i\n", "file a seq 0 type i\n", "file a seq 1 type x\n", "file a seq 1 type i seq 2\n"} {
		if _, err := readManifest(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	name := "spaces \"\\\xff.aof"
	m := manifest{Base: &segment{name, 1, "b"}, Incr: []segment{{"tail", 1, "i"}}}
	var b bytes.Buffer
	if err := m.write(&b); err != nil {
		t.Fatal(err)
	}
	got, err := readManifest(&b)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal(got, err)
	}
}
func TestMultipartMissingCorruptAndHistoricalSegments(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, b []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// An expiration must remain replayable until a later increment's PERSIST.
	var base bytes.Buffer
	if err := rdb.Write(context.Background(), &base, map[string]storage.Entry{"key": {Value: "alive", ExpiresAt: 1}}); err != nil {
		t.Fatal(err)
	}
	write("base.rdb", base.Bytes())
	write("first.aof", aof.Encode("PERSIST", "key"))
	write("last.aof", aof.Encode("SET", "tail", "value"))
	manifestBytes := []byte("file base.rdb seq 1 type b\nfile deleted-history seq 2 type h\nfile first.aof seq 2 type i\nfile last.aof seq 3 type i\n")
	write("appendonly.aof.manifest", manifestBytes)
	s := openMultipart(t, dir, true)
	got := s.Restore()
	if got["key"].Value != "alive" || got["key"].ExpiresAt != 0 || got["tail"].Value != "value" {
		t.Fatal(got)
	}
	s.Close()
	if err := os.Remove(filepath.Join(dir, "last.aof")); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenMultipartAOF(dir, "appendonly.aof", "", "always", true); err == nil {
		s.Close()
		t.Fatal("created missing increment")
	}
	if _, err := os.Stat(filepath.Join(dir, "last.aof")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing segment was modified")
	}
	write("last.aof", []byte("*2\r\n$3\r\nSET\r\n"))
	if s, err := OpenMultipartAOF(dir, "appendonly.aof", "", "always", true); err == nil {
		s.Close()
		t.Fatal("accepted truncated increment")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "appendonly.aof.manifest")); !bytes.Equal(b, manifestBytes) {
		t.Fatal("invalid recovery changed manifest")
	}
}
func TestLegacyHybridMigrationAndOwnership(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "appendonly.aof")
	var b bytes.Buffer
	rdb.Write(context.Background(), &b, map[string]storage.Entry{"key": {Value: "base", ExpiresAt: 1}})
	b.Write(aof.Encode("PERSIST", "key"))
	b.Write(aof.Encode("SET", "tail", "value"))
	if err := os.WriteFile(legacy, b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	mpdir := filepath.Join(dir, "appendonlydir")
	s, err := OpenMultipartAOF(mpdir, "appendonly.aof", legacy, "always", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Restore(); got["key"].Value != "base" || got["tail"].Value != "value" {
		t.Fatal(got)
	}
	if other, err := OpenMultipartAOF(mpdir, "appendonly.aof", legacy, "always", true); err == nil {
		other.Close()
		t.Fatal("duplicate writer")
	}
	if current, _ := os.ReadFile(legacy); !bytes.Equal(current, b.Bytes()) {
		t.Fatal("migration modified legacy input")
	}
}
func TestRDBLoadAndAOFPrecedence(t *testing.T) {
	config := data.DefaultConfig()
	config.Dir = t.TempDir()
	config.Save = nil
	entries := map[string]storage.Entry{"key": {Value: "rdb"}}
	if err := writeRDB(context.Background(), filepath.Join(config.Dir, config.DBFilename), entries); err != nil {
		t.Fatal(err)
	}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.Restore(), entries) {
		t.Fatal("RDB was not loaded with save disabled")
	}
	p.Close()
	config.AppendOnly = true
	p, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Restore()) != 0 {
		t.Fatal("new AOF loaded standalone RDB")
	}
	p.Close()
	config.AppendOnly = false
	if err := os.WriteFile(filepath.Join(config.Dir, config.DBFilename), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if p, err := New(config); err == nil {
		p.Close()
		t.Fatal("accepted corrupt RDB")
	}
}

func TestSnapshotFailureStopsWritesUntilSuccessfulSave(t *testing.T) {
	config := data.DefaultConfig()
	config.Dir = t.TempDir()
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	e := p.(*Engine)
	defer e.Close()
	// Replacing the destination with a directory makes atomic rename fail.
	if err := os.Mkdir(e.snapshotPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := e.Save(context.Background(), nil, false); err == nil {
		t.Fatal("save succeeded over a directory")
	}
	if err := e.Err(); err != nil {
		t.Fatalf("foreground failure blocked writes: %v", err)
	}
	if err := e.Save(context.Background(), nil, true); err == nil {
		t.Fatal("background save succeeded over a directory")
	}
	if err := e.Err(); !errors.Is(err, database.ErrSnapshotFailed) {
		t.Fatal("write accepted after failed snapshot")
	}
	backgroundFailure := e.saveFailure
	if err := e.Save(context.Background(), nil, false); err == nil {
		t.Fatal("save succeeded over a directory")
	}
	if err := e.Err(); !errors.Is(err, backgroundFailure) || !errors.Is(err, database.ErrSnapshotFailed) {
		t.Fatalf("foreground failure replaced background status: %v", err)
	}
	if err := os.Remove(e.snapshotPath); err != nil {
		t.Fatal(err)
	}
	if err := e.Save(context.Background(), nil, false); err != nil {
		t.Fatal(err)
	}
	if err := e.Err(); err != nil {
		t.Fatalf("successful foreground save did not clear failure: %v", err)
	}
	if err := e.Append("key", storage.Entry{Value: "value"}, true); err != nil {
		t.Fatal(err)
	}
}
