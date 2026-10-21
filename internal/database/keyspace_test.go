package database

import (
	"context"
	"errors"
	"testing"

	"github.com/jr-dragon/peridot/internal/storage"
)

func TestKeyspaceExpirationIndexAndAppendFailure(t *testing.T) {
	for name, backend := range map[string]storage.Backend{"xsync": storage.NewXsync(), "dlht": storage.NewDLHT()} {
		t.Run(name, func(t *testing.T) {
			p := &expirationPersistence{}
			db := manualExpirationDB(t, backend, p)
			for _, key := range []string{"a", "b"} {
				if err := db.Update(context.Background(), key, func(storage.Entry, bool, int64) (storage.Entry, bool, error) {
					return storage.Entry{Value: "value", ExpiresAt: 2000}, true, nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			assertExpirationIndex(t, db)
			before := db.dirty
			if err := db.Read(context.Background(), "a", func(e storage.Entry, exists bool, now int64) {
				if !exists || now != 1000 || e.ExpiresAt != 2000 {
					t.Fatal(e, exists, now)
				}
			}); err != nil {
				t.Fatal(err)
			}
			if db.dirty != before {
				t.Fatal("read increased dirty count")
			}
			failure := errors.New("append failed")
			p.appendErr = failure
			if _, err := db.DeleteKeys(context.Background(), []string{"a", "b"}); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if len(backend.Snapshot()) != 2 || db.dirty != before {
				t.Fatal("failed append published deletion")
			}
			assertExpirationIndex(t, db)
			p.appendErr = nil
			if n, err := db.DeleteKeys(context.Background(), []string{"a", "a", "b", "missing"}); err != nil || n != 2 {
				t.Fatal(n, err)
			}
			if db.dirty != before+2 || len(p.deleted) != 2 {
				t.Fatal("incorrect durable deletion count")
			}
			assertExpirationIndex(t, db)
			// A read of an expired key must durably delete it before reporting absence.
			backend.Put("expired", storage.Entry{Value: "value", ExpiresAt: 999})
			db.trackExpiration("expired", storage.Entry{ExpiresAt: 999})
			p.appendErr = failure
			if err := db.Read(context.Background(), "expired", func(storage.Entry, bool, int64) { t.Fatal("read callback after append failure") }); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if _, ok := backend.Load("expired"); !ok {
				t.Fatal("failed expiration removed key")
			}
			p.appendErr = nil
			if err := db.Read(context.Background(), "expired", func(_ storage.Entry, exists bool, _ int64) {
				if exists {
					t.Fatal("expired key visible")
				}
			}); err != nil {
				t.Fatal(err)
			}
			assertExpirationIndex(t, db)
		})
	}
}

func TestKeyspaceCanceledAndClosed(t *testing.T) {
	db := New(storage.New(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.DeleteKeys(ctx, []string{"key"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	read := func(storage.Entry, bool, int64) { t.Fatal("unexpected read callback") }
	if err := db.Read(ctx, "key", read); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DeleteKeys(context.Background(), []string{"key"}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := db.Read(context.Background(), "key", read); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
