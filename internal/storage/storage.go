// Package storage defines the replaceable in-memory data boundary.
package storage

import (
	"context"
	"errors"
)

// ErrUnchanged tells Update that a conditional operation did not apply. Update
// returns nil without publishing or persisting the callback's returned entry.
var ErrUnchanged = errors.New("storage: update did not apply")

// New selects the default memory backend independently of persistence.
func New() Backend { return NewXsync() }

// Entry holds an immutable value and its Unix millisecond expiration timestamp.
// ExpiresAt == 0 means no expiration. Redis strings are represented by string;
// other Redis data types use immutable TypedValue implementations. Mutable
// values must be copied before publication and never modified afterward.
type Entry struct {
	Value     any
	ExpiresAt int64
}

// UpdateFunc runs exactly once under exclusive access to a key. Expired entries
// are presented as absent. now is the operation's Unix millisecond time snapshot.
// Returning keep=false deletes the key; an error preserves the live entry.
// Return ErrUnchanged for a no-op. A nil error represents an applied mutation,
// even when the new entry is identical to the old one.
// Callbacks must not call back into the same Store or mutate published values.
type UpdateFunc func(current Entry, exists bool, now int64) (next Entry, keep bool, err error)

// ReadFunc observes a key and the same time snapshot used for expiration.
// It must not mutate the entry or call back into the store.
type ReadFunc func(Entry, bool, int64)

// KeyReader observes whole values and their key expiration. Missing and expired
// keys are absent. Read supplies the same clock snapshot used for expiration.
type KeyReader interface {
	Get(context.Context, string) (Entry, bool, error)
	Read(context.Context, string, ReadFunc) error
}

// KeyUpdater atomically changes one immutable value, regardless of Redis type.
// Conditional writes, numeric operations, key TTLs, collection operations and
// hash field TTLs are evaluated by the caller inside the callback.
type KeyUpdater interface {
	Update(context.Context, string, UpdateFunc) error
}

// KeyDeleter removes keys as one indivisible operation.
type KeyDeleter interface {
	// DeleteKeys removes keys in one operation and counts only live, distinct keys.
	DeleteKeys(context.Context, []string) (int64, error)
}

// KeyStore is the currently implemented, command-facing capability set.
// Implementations preserve error identity and concurrent replacements during
// passive expiration. Consumers should request the smallest interface they use.
type KeyStore interface {
	KeyReader
	KeyUpdater
	KeyDeleter
}

// Store describes the complete planned memory data boundary for standalone
// Redis 8.10.2. It composes capabilities, not one method per Redis command.
// Operations accept binary-safe keys and preserve errors. A context already
// canceled before an operation begins prevents callback execution and mutation.
// Cancellation during an operation must never leave a partial publication.
// Xsync implements KeyStore today; its remaining methods panic("todo").
// database.DB currently exposes KeyStore and coordinates persistence separately.
type Store interface {
	KeyStore
	MultiKeyStore
	KeyspaceStore
	AtomicStore
	MetadataStore
}

func expired(entry Entry, now int64) bool {
	// Redis 8.10.2 expires a key strictly after its millisecond deadline.
	return entry.ExpiresAt != 0 && now > entry.ExpiresAt
}
