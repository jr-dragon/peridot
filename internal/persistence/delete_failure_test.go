package persistence

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
)

// The boundary fits one old-style DEL, but not a complete two-key DEL.
type boundedAppendFile struct {
	appendFile
	remaining int
	calls     int
}

func (f *boundedAppendFile) Write(p []byte) (int, error) {
	f.calls++
	n := min(f.remaining, len(p))
	written, err := f.appendFile.Write(p[:n])
	f.remaining -= written
	if err == nil && written < len(p) {
		err = syscall.EFBIG
	}
	return written, err
}

func TestMultiKeyDeleteFailureIsAtomic(t *testing.T) {
	for name, backend := range map[string]func() storage.Backend{
		"xsync": func() storage.Backend { return storage.NewXsync() },
	} {
		for _, policy := range []string{"always", "everysec", "no"} {
			t.Run(name+"/"+policy, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "data.aof")
				log, err := OpenAOF(path, policy)
				if err != nil {
					t.Fatal(err)
				}
				db := database.New(backend(), log)
				defer db.Close()
				for _, key := range []string{"a", "b"} {
					if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
						return storage.Entry{Value: key}, true, nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				log.mu.Lock()
				limited := &boundedAppendFile{appendFile: log.file, remaining: len(aof.Encode("DEL", "a"))}
				log.file = limited
				log.mu.Unlock()
				count, err := db.DeleteKeys(context.Background(), []string{"a", "b", "a"})
				if count != 0 || !errors.Is(err, syscall.EFBIG) || !errors.Is(err, database.ErrAOFFailed) {
					t.Fatalf("delete = %d, %v", count, err)
				}
				for _, key := range []string{"a", "b"} {
					entry, exists, err := db.Get(context.Background(), key)
					if err != nil || !exists || entry.Value != key {
						t.Fatalf("partial deletion: %q: %v %v %v", key, entry, exists, err)
					}
				}
				if limited.calls != 1 {
					t.Fatalf("split deletion into %d writes", limited.calls)
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				// A torn batch must be rejected, never replayed as a complete prefix of
				// separately encoded DEL commands. Successful batches are checked below.
				if _, err := aof.Load(bytes.NewReader(raw)); !errors.Is(err, aof.ErrFormat) {
					t.Fatalf("partial deletion log was accepted: %v", err)
				}
			})
		}
	}
}

func TestMultiKeyDeleteIsOneRecoverableRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	log, err := OpenAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	db := database.New(storage.New(), log)
	defer db.Close()
	for _, key := range []string{"a\x00\xff", "b", "keep"} {
		if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
			return storage.Entry{Value: key}, true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := db.DeleteKeys(context.Background(), []string{"a\x00\xff", "b", "a\x00\xff", "missing"}); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(raw, aof.Encode("DEL", "a\x00\xff", "b")) {
		t.Fatalf("not a single DEL record: %q", raw)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state := reopened.Restore()
	if len(state) != 1 || state["keep"].Value != "keep" {
		t.Fatal(state)
	}
}
