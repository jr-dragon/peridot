package persistence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/jr-dragon/peridot/internal/storage"
)

// prepareSnapshotAOF exports a standalone base and manifest without enabling
// logging, loading stale AOF data, or creating an incremental file. Hold the same
// manifest lock as an active AOF owner until publication and cleanup complete.
func prepareSnapshotAOF(ctx context.Context, dir, name string, preamble bool, state map[string]storage.Entry) (_ func() error, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := syncDirectory(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name+".manifest")
	lock, err := lockAOF(path + ".lock")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			lock.Close()
		}
	}()
	s := &MultipartAOF{AOF: &AOF{}, manifestPath: path, name: name, preamble: preamble}
	f, err := os.Open(path)
	if err == nil {
		s.manifest, err = readManifest(f)
		f.Close()
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	base, err := s.nextSegment("b")
	if err != nil {
		return nil, err
	}
	var once sync.Once
	var result error
	return func() error {
		once.Do(func() {
			defer lock.Close()
			if result = s.writeBase(ctx, base, state); result != nil {
				return
			}
			if result = ctx.Err(); result != nil {
				return
			}
			old := s.manifest
			if result = s.publish(manifest{Base: &base}); result != nil {
				return
			}
			for _, part := range old.files() {
				_ = os.Remove(filepath.Join(dir, part.Name))
			}
		})
		return result
	}, nil
}
