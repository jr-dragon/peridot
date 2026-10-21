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
// other value types may be added by future commands. Mutable values must be
// copied before publication and must never be modified after publication.
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

// Store supports concurrent access, binary-safe keys, passive expiration, and
// atomic read/modify/write operations. Implementations must preserve error
// identity and must never delete a replacement value during passive expiration.
type Store interface {
	Get(context.Context, string) (Entry, bool, error)
	Update(context.Context, string, UpdateFunc) error
	Read(context.Context, string, ReadFunc) error
	// DeleteKeys removes keys in one operation and counts only live, distinct keys.
	DeleteKeys(context.Context, []string) (int64, error)
}

func expired(entry Entry, now int64) bool {
	// Redis 8.10.2 expires a key strictly after its millisecond deadline.
	return entry.ExpiresAt != 0 && now > entry.ExpiresAt
}

// Backend is a memory store with raw access for database coordination. Raw
// methods neither expire entries nor invoke command callbacks. The owner must
// exclude ordinary Store calls while coordinating durable mutations, and
// exclude all mutations while collecting a consistent Snapshot.
// Xsync and DLHT are the supported backends.
type Backend interface {
	Store
	Load(string) (Entry, bool)
	Put(string, Entry)
	Delete(string)
	Snapshot() map[string]Entry
}
