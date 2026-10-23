package persistence

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

type Entry = storage.Entry

type aofDatabase struct {
	*database.DB
	*AOF
	now func() time.Time
}

func openAOFDatabase(path string) (*aofDatabase, error) {
	p, err := OpenAOF(path)
	if err != nil {
		return nil, err
	}
	s := &aofDatabase{AOF: p, now: time.Now}
	s.DB = database.New(storage.New(), p, database.WithClock(func() time.Time { return s.now() }))
	return s, nil
}

func (s *aofDatabase) Close() error                      { return s.DB.Close() }
func (s *aofDatabase) Rewrite(ctx context.Context) error { return s.DB.Rewrite(ctx) }

func testAOF(t *testing.T) *aofDatabase {
	t.Helper()
	s, err := openAOFDatabase(filepath.Join(t.TempDir(), "appendonly.aof"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func putAOF(t *testing.T, s *aofDatabase, key string, entry Entry) {
	t.Helper()
	if err := s.Update(context.Background(), key, func(Entry, bool, int64) (Entry, bool, error) { return entry, true, nil }); err != nil {
		t.Fatal(err)
	}
}

func readAOF(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAOFAppendRewriteAndRestart(t *testing.T) {
	s := testAOF(t)
	s.now = func() time.Time { return time.UnixMilli(1000) }
	key, value := "key\x00\xff", "value\r\n\x00\xff"
	putAOF(t, s, key, Entry{Value: "old"})
	putAOF(t, s, key, Entry{Value: value, ExpiresAt: 9999999999999})
	putAOF(t, s, "expired", Entry{Value: "gone", ExpiresAt: 1010})
	putAOF(t, s, "delete", Entry{Value: "gone"})
	if err := s.Update(context.Background(), "delete", func(Entry, bool, int64) (Entry, bool, error) { return Entry{}, false, nil }); err != nil {
		t.Fatal(err)
	}
	before := readAOF(t, s.path)
	want := append(aof.Replacement(key, "old", 0), aof.Replacement(key, value, 9999999999999)...)
	want = append(want, aof.Replacement("expired", "gone", 1010)...)
	want = append(want, aof.Encode("SET", "delete", "gone")...)
	want = append(want, aof.Encode("DEL", "delete")...)
	if !bytes.Equal(before, want) {
		t.Fatalf("log=%q, want=%q", before, want)
	}
	s.now = func() time.Time { return time.UnixMilli(2000) }
	if err := s.Rewrite(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readAOF(t, s.path); !bytes.Equal(got, aof.Replacement(key, value, 9999999999999)) {
		t.Fatalf("rewrite=%q", got)
	}
	putAOF(t, s, "after", Entry{Value: "rewrite"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := openAOFDatabase(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for k, v := range map[string]string{key: value, "after": "rewrite"} {
		entry, ok, err := restored.Get(context.Background(), k)
		if err != nil || !ok || entry.Value != v {
			t.Fatalf("%q = %#v,%v,%v", k, entry, ok, err)
		}
	}
	for _, k := range []string{"expired", "delete"} {
		if _, ok, err := restored.Get(context.Background(), k); err != nil || ok {
			t.Fatalf("%s resurrected: %v", k, err)
		}
	}
}

func TestAOFNoOpErrorsAndCancellation(t *testing.T) {
	s := testAOF(t)
	putAOF(t, s, "key", Entry{Value: "old"})
	before := readAOF(t, s.path)
	cause := errors.New("abort")
	for _, fail := range []bool{false, true} {
		err := s.Update(context.Background(), "key", func(e Entry, ok bool, _ int64) (Entry, bool, error) {
			if fail {
				return Entry{Value: "bad"}, true, cause
			}
			return e, ok, storage.ErrUnchanged
		})
		if fail && !errors.Is(err, cause) || !fail && err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Update(ctx, "key", func(Entry, bool, int64) (Entry, bool, error) {
		t.Fatal("canceled callback")
		return Entry{}, false, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Rewrite(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Update(context.Background(), "bad", func(Entry, bool, int64) (Entry, bool, error) { return Entry{Value: 1}, true, nil }); err == nil {
		t.Fatal("accepted unsupported type")
	}
	if !bytes.Equal(readAOF(t, s.path), before) {
		t.Fatal("no-op changed log")
	}
	putAOF(t, s, "key", Entry{Value: "old"})
	if len(readAOF(t, s.path)) <= len(before) {
		t.Fatal("successful same-value replacement did not append")
	}
}

type failingAppend struct {
	appendFile
	writeErr, syncErr error
	short             bool
	writes, syncs     int
}

func (f *failingAppend) Write(b []byte) (int, error) {
	f.writes++
	if f.short {
		return 1, nil
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.appendFile.Write(b)
}
func (f *failingAppend) Sync() error {
	f.syncs++
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.appendFile.Sync()
}

func TestAOFFailurePreventsPublicationAndFurtherWrites(t *testing.T) {
	for _, mode := range []string{"write", "short", "sync"} {
		t.Run(mode, func(t *testing.T) {
			s := testAOF(t)
			putAOF(t, s, "key", Entry{Value: "old"})
			original := s.file
			cause := errors.New("disk failure")
			f := &failingAppend{appendFile: original}
			switch mode {
			case "write":
				f.writeErr = cause
			case "short":
				f.short = true
				cause = io.ErrShortWrite
			case "sync":
				f.syncErr = cause
			}
			s.file = f
			err := s.Update(context.Background(), "key", func(Entry, bool, int64) (Entry, bool, error) { return Entry{Value: "new"}, true, nil })
			if !errors.Is(err, cause) {
				t.Fatalf("lost error: %v", err)
			}
			entry, ok, err := s.Get(context.Background(), "key")
			if err != nil || !ok || entry.Value != "old" {
				t.Fatalf("published failed write: %v,%v,%v", entry, ok, err)
			}
			if err := s.Update(context.Background(), "key", func(Entry, bool, int64) (Entry, bool, error) {
				t.Fatal("poisoned callback")
				return Entry{}, false, nil
			}); !errors.Is(err, cause) {
				t.Fatal(err)
			}
			if err := s.Rewrite(context.Background()); !errors.Is(err, cause) {
				t.Fatal(err)
			}
			if f.writes != 1 {
				t.Fatalf("writes=%d", f.writes)
			}
			if err := s.Close(); !errors.Is(err, cause) {
				t.Fatalf("close lost failure: %v", err)
			}
		})
	}
}

func TestAOFLoadFailureDoesNotModifyInputOrKeepLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.aof")
	input := append(aof.Encode("SET", "key", "old"), []byte("*1\r\n$3\r\nSE")...)
	if err := os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if s, err := openAOFDatabase(path); !errors.Is(err, aof.ErrFormat) || s != nil {
			t.Fatalf("open=%v,%v", s, err)
		}
	}
	if !bytes.Equal(readAOF(t, path), input) {
		t.Fatal("modified failed input")
	}
	if err := os.WriteFile(path, aof.Encode("SET", "key", "new"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := openAOFDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestAOFExclusiveOwnershipAcrossRewrite(t *testing.T) {
	s := testAOF(t)
	for range 2 {
		if other, err := openAOFDatabase(s.path); err == nil {
			other.Close()
			t.Fatal("accepted second writer")
		}
		if err := s.Rewrite(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.StartRewrite(); !errors.Is(err, database.ErrClosed) {
		t.Fatal(err)
	}
	if _, _, err := s.Get(context.Background(), "key"); !errors.Is(err, database.ErrClosed) {
		t.Fatal(err)
	}
}

func TestAOFSymlinkOwnershipRewriteAndRestart(t *testing.T) {
	for _, directoryLink := range []bool{false, true} {
		for _, openAlias := range []bool{false, true} {
			t.Run(fmt.Sprintf("directory=%v/owner-alias=%v", directoryLink, openAlias), func(t *testing.T) {
				dir := t.TempDir()
				realDir := filepath.Join(dir, "data")
				if err := os.Mkdir(realDir, 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(realDir, "appendonly.aof")
				link := filepath.Join(dir, "alias")
				alias := link
				if directoryLink {
					if err := os.Symlink("data", link); err != nil {
						t.Fatal(err)
					}
					alias = filepath.Join(link, "appendonly.aof")
				} else {
					if err := os.WriteFile(path, nil, 0600); err != nil {
						t.Fatal(err)
					}
					// Exercise a relative symlink chain, not only a direct link.
					if err := os.Symlink(filepath.Join("data", "appendonly.aof"), filepath.Join(dir, "intermediate")); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink("intermediate", link); err != nil {
						t.Fatal(err)
					}
				}
				ownerPath := path
				if openAlias {
					ownerPath = alias
				}
				s, err := openAOFDatabase(ownerPath)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				for _, phase := range []string{"before", "after"} {
					for _, candidate := range []string{path, alias} {
						if other, err := openAOFDatabase(candidate); err == nil {
							other.Close()
							t.Fatalf("accepted second writer through %s %s rewrite", candidate, phase)
						}
					}
					putAOF(t, s, phase, Entry{Value: "confirmed\x00\xff", ExpiresAt: 4102444800000})
					if phase == "before" {
						if err := s.Rewrite(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
				}
				if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("rewrite replaced symlink: %v, %v", info, err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				for _, candidate := range []string{path, alias} {
					assertAOFConfirmedEntries(t, candidate, "before", "after")
				}
			})
		}
	}
}

func assertAOFConfirmedEntries(t *testing.T, path string, keys ...string) {
	t.Helper()
	s, err := openAOFDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, key := range keys {
		e, ok, err := s.Get(context.Background(), key)
		if want := (Entry{Value: "confirmed\x00\xff", ExpiresAt: 4102444800000}); err != nil || !ok || e != want {
			t.Fatalf("restarted %s: %q = %#v,%v,%v, want %#v", path, key, e, ok, err, want)
		}
	}
}

func TestAOFHardlinkOwnershipRewriteAndRestart(t *testing.T) {
	for _, linkBeforeOpen := range []bool{false, true} {
		t.Run(fmt.Sprintf("link-before-open=%v", linkBeforeOpen), func(t *testing.T) {
			dir := t.TempDir()
			path, alias := filepath.Join(dir, "data.aof"), filepath.Join(dir, "alias.aof")
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			var s *aofDatabase
			if !linkBeforeOpen {
				var err error
				s, err = openAOFDatabase(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				putAOF(t, s, "before", Entry{Value: "confirmed\x00\xff", ExpiresAt: 4102444800000})
			}
			if err := os.Link(path, alias); err != nil {
				t.Fatal(err)
			}
			before := readAOF(t, path)
			for _, candidate := range []string{path, alias} {
				if other, err := openAOFDatabase(candidate); err == nil {
					other.Close()
					t.Fatalf("accepted hardlinked AOF: %s", candidate)
				}
			}
			if s != nil {
				if err := s.Rewrite(context.Background()); err == nil {
					t.Fatal("rewrite split a hardlinked AOF")
				}
			}
			if !bytes.Equal(readAOF(t, path), before) || !bytes.Equal(readAOF(t, alias), before) {
				t.Fatal("rejected operation changed AOF contents")
			}
			original, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			linked, err := os.Stat(alias)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(original, linked) {
				t.Fatal("rejected rewrite split AOF identity")
			}
			if err := os.Remove(alias); err != nil {
				t.Fatal(err)
			}
			if s == nil {
				var err error
				s, err = openAOFDatabase(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				putAOF(t, s, "before", Entry{Value: "confirmed\x00\xff", ExpiresAt: 4102444800000})
			}
			if err := s.Rewrite(context.Background()); err != nil {
				t.Fatal(err)
			}
			putAOF(t, s, "after", Entry{Value: "confirmed\x00\xff", ExpiresAt: 4102444800000})
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAOFConfirmedEntries(t, path, "before", "after")
		})
	}
}

func TestAOFRejectsDanglingSymlink(t *testing.T) {
	dir := t.TempDir()
	path, alias := filepath.Join(dir, "data.aof"), filepath.Join(dir, "alias.aof")
	if err := os.Symlink("data.aof", alias); err != nil {
		t.Fatal(err)
	}
	if s, err := openAOFDatabase(alias); err == nil {
		s.Close()
		t.Fatal("accepted dangling symlink")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lost missing-target error: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dangling target was created: %v", err)
	}
	// Creating the target through its real path must make the existing link
	// usable, while sharing the same ownership lock.
	s, err := openAOFDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if other, err := openAOFDatabase(alias); err == nil {
		other.Close()
		t.Fatal("accepted second writer through formerly dangling link")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAOFConfirmedEntries(t, alias)
}

func TestAOFReplayExpirationAfterHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.aof")
	input := append(aof.Replacement("keep", "original", 1), aof.Encode("PERSIST", "keep")...)
	input = append(input, aof.Replacement("expired", "original", 1)...)
	input = append(input, aof.Encode("SET", "expired", "new", "KEEPTTL")...)
	if err := os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := openAOFDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if e, ok, err := s.Get(context.Background(), "keep"); err != nil || !ok || e.Value != "original" || e.ExpiresAt != 0 {
		t.Fatalf("lost historical state: %v,%v,%v", e, ok, err)
	}
	if _, ok, err := s.Get(context.Background(), "expired"); err != nil || ok {
		t.Fatalf("expired key restored: %v", err)
	}
}

func TestAOFExpirationIsRecordedBeforeDeletion(t *testing.T) {
	for _, operation := range []string{"read", "failed update", "replacement"} {
		t.Run(operation, func(t *testing.T) {
			s := testAOF(t)
			now := int64(1000)
			s.now = func() time.Time { return time.UnixMilli(now) }
			putAOF(t, s, "key", Entry{Value: "old", ExpiresAt: 1010})
			now = 1011
			switch operation {
			case "read":
				if _, exists, err := s.Get(context.Background(), "key"); err != nil || exists {
					t.Fatalf("expired read: %v,%v", exists, err)
				}
			case "failed update":
				cause := errors.New("command failed")
				err := s.Update(context.Background(), "key", func(e Entry, exists bool, _ int64) (Entry, bool, error) {
					if exists || e.Value != nil {
						t.Fatal("callback saw expired entry")
					}
					return Entry{}, false, cause
				})
				if !errors.Is(err, cause) {
					t.Fatal(err)
				}
			case "replacement":
				putAOF(t, s, "key", Entry{Value: "new"})
			}
			input := readAOF(t, s.path)
			if !bytes.Contains(input, aof.Encode("DEL", "key")) {
				t.Fatalf("missing expiration deletion: %q", input)
			}
			state, err := aof.Load(bytes.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			if operation == "replacement" {
				if state["key"].Value != "new" {
					t.Fatal(state)
				}
			} else if _, exists := state["key"]; exists {
				t.Fatal("expired key survived in log")
			}
		})
	}
}

func TestAOFRewriteSchedulingAndClose(t *testing.T) {
	s := testAOF(t)
	putAOF(t, s, "key", Entry{Value: "old"})
	putAOF(t, s, "key", Entry{Value: "new"})
	// Hold the persistence gate so the scheduled job cannot finish before the
	// second request checks the scheduling state.
	s.mu.Lock()
	first, second := s.StartRewrite(), s.StartRewrite()
	s.mu.Unlock()
	if first != nil || !errors.Is(second, database.ErrRewriteInProgress) {
		t.Fatalf("schedule: %v,%v", first, second)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readAOF(t, s.path); !bytes.Equal(got, aof.Encode("SET", "key", "new")) {
		t.Fatalf("close did not wait for rewrite: %q", got)
	}
}

type closeFailure struct {
	appendFile
	cause error
}

func (f closeFailure) Close() error { return errors.Join(f.appendFile.Close(), f.cause) }

func TestAOFRewriteFailureAfterRenamePoisonsWrites(t *testing.T) {
	s := testAOF(t)
	putAOF(t, s, "key", Entry{Value: "old"})
	putAOF(t, s, "key", Entry{Value: "new"})
	cause := errors.New("old descriptor close failed")
	s.file = closeFailure{s.file, cause}
	if err := s.Rewrite(context.Background()); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if got := readAOF(t, s.path); !bytes.Equal(got, aof.Encode("SET", "key", "new")) {
		t.Fatalf("replacement not published: %q", got)
	}
	if err := s.Update(context.Background(), "key", func(Entry, bool, int64) (Entry, bool, error) {
		t.Fatal("callback after uncertain publication")
		return Entry{}, false, nil
	}); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if err := s.Close(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestAOFConcurrentMutationRewriteAndClose(t *testing.T) {
	s := testAOF(t)
	const workers, iterations = 4, 20
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range iterations {
				err := s.Update(context.Background(), "counter", func(e Entry, ok bool, _ int64) (Entry, bool, error) {
					n := 0
					if ok {
						fmt.Sscan(e.Value.(string), &n)
					}
					return Entry{Value: fmt.Sprint(n + 1)}, true, nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 3 {
			if err := s.Rewrite(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	if err := s.StartRewrite(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := openAOFDatabase(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	e, ok, err := restored.Get(context.Background(), "counter")
	if err != nil || !ok || e.Value != fmt.Sprint(workers*iterations) {
		t.Fatalf("counter=%v,%v,%v", e, ok, err)
	}
}

func TestAOFRewriteFailureKeepsOriginalUsable(t *testing.T) {
	s := testAOF(t)
	putAOF(t, s, "key", Entry{Value: "old"})
	before := readAOF(t, s.path)
	// A replacement target that is a directory fails atomically at rename.
	other := filepath.Join(filepath.Dir(s.path), "directory")
	if err := os.Mkdir(other, 0700); err != nil {
		t.Fatal(err)
	}
	originalPath := s.path
	s.path = other
	if err := s.Rewrite(context.Background()); err == nil {
		t.Fatal("rewrite unexpectedly succeeded")
	}
	s.path = originalPath
	if !bytes.Equal(readAOF(t, s.path), before) {
		t.Fatal("failed rewrite changed original")
	}
	putAOF(t, s, "key", Entry{Value: "new"})
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(s.path), ".peridot-aof-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary files: %v,%v", leftovers, err)
	}
}
