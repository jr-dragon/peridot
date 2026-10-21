package persistence

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/persistence/rdb"
	"github.com/jr-dragon/peridot/internal/storage"
)

// atomicFile reports whether rename happened even if the following directory
// sync failed. Callers must not delete files referenced by an ambiguous manifest.
func atomicFile(path string, write func(io.Writer) error) (published bool, err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".peridot-persistence-*")
	if err != nil {
		return false, err
	}
	defer func() { f.Close(); os.Remove(f.Name()) }()
	w := bufio.NewWriterSize(f, 64<<10)
	if err = write(w); err != nil {
		return false, err
	}
	if err = w.Flush(); err != nil {
		return false, err
	}
	if err = f.Sync(); err != nil {
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return false, err
	}
	return true, syncDirectory(filepath.Dir(path))
}

func loadSegment(path string, state map[string]aof.String, allowRDB bool, rdbOnly bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("persistence: expected regular file")
	}
	r := bufio.NewReaderSize(f, 64<<10)
	prefix, _ := r.Peek(5)
	if string(prefix) == "REDIS" {
		if !allowRDB {
			return errors.New("aof: RDB in incremental segment")
		}
		snapshot, err := rdb.Load(r)
		if err != nil {
			return err
		}
		for k, v := range snapshot {
			state[k] = aof.String{Value: v.Value, ExpiresAt: v.ExpiresAt, HasExpiration: v.HasExpiration}
		}
	} else if rdbOnly {
		return fmt.Errorf("%w: header", rdb.ErrFormat)
	}
	if rdbOnly {
		if _, err := r.Peek(1); err != io.EOF {
			if err == nil {
				err = errors.New("rdb: trailing data")
			}
			return err
		}
		return nil
	}
	return aof.Replay(r, state)
}

func recoveredEntries(state map[string]aof.String) map[string]storage.Entry {
	entries := make(map[string]storage.Entry, len(state))
	now := time.Now().UnixMilli()
	for k, v := range state {
		if v.HasExpiration && now > v.ExpiresAt {
			continue
		}
		entries[k] = storage.Entry{Value: v.Value, ExpiresAt: v.ExpiresAt}
	}
	return entries
}

func writeRDB(ctx context.Context, path string, entries map[string]storage.Entry) error {
	_, err := atomicFile(path, func(w io.Writer) error { return rdb.Write(ctx, w, entries) })
	return err
}
