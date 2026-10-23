package storage

import "context"

// Mutation describes a resolved publication. Delete ignores Entry. Payloads
// are immutable; callers must copy mutable values before returning mutations.
type Mutation struct {
	Key    string
	Entry  Entry
	Delete bool
}

// ReadKeysFunc observes only live requested keys, once, at one clock snapshot.
// Missing keys are absent from entries. The map is borrowed and read-only.
type ReadKeysFunc func(entries map[string]Entry, now int64)

// UpdateKeysFunc receives the same view as ReadKeysFunc under exclusive access
// to all requested keys. Returned mutations must have distinct keys drawn from
// that request. Omitted keys remain unchanged. An error aborts all user changes;
// ErrUnchanged is a successful no-op. The callback must not reenter the store.
type UpdateKeysFunc func(entries map[string]Entry, now int64) ([]Mutation, error)

// MultiKeyStore supports MGET/MSET/MSETNX, RENAME/COPY, collection stores and
// list moves without exposing a partial result. Input keys are borrowed until
// return; duplicate keys are locked once. ReadKeys is a consistent read across
// requested keys. UpdateKeys resolves all expiration and validates all changes
// before publishing any user mutation; publication and persistence form one
// coordinated operation. Redis argument order is handled by the caller.
type MultiKeyStore interface {
	ReadKeys(context.Context, []string, ReadKeysFunc) error
	UpdateKeys(context.Context, []string, UpdateKeysFunc) error
}

// Cursor is an opaque scan position scoped to one store and one scan. Zero
// starts a scan; a returned zero ends it. A complete scan returns every entry
// continuously present throughout that scan and may return duplicates. Changes
// during iteration have unspecified inclusion. Count is a positive work hint,
// not a result limit; empty nonterminal pages are valid. Scans must make progress
// without materializing a complete snapshot on each call.
type Cursor uint64

type KeyEntry struct {
	Key   string
	Entry Entry
}

type EntryPage struct {
	Cursor  Cursor
	Entries []KeyEntry
}

// ReclaimMode controls reclamation after keys become immediately invisible.
// ReclaimAsync is for UNLINK/FLUSH ASYNC; it must not defer logical removal.
type ReclaimMode uint8

const (
	ReclaimSync ReclaimMode = iota
	ReclaimAsync
)

// KeyspaceStore supplies traversal, sampling, physical cardinality and clearing.
// SCAN MATCH/TYPE and KEYS filtering stay in the command layer; Scan returns
// immutable payloads so that filtering can inspect types. Scan and RandomKey
// exclude expired keys. Size counts physical keys, as Redis DBSIZE does, and
// must not force a full expiration sweep. Clear is atomic with ordinary access.
type KeyspaceStore interface {
	Scan(context.Context, Cursor, int) (EntryPage, error)
	Size(context.Context) (int64, error)
	RandomKey(context.Context) (key string, exists bool, err error)
	Clear(context.Context, ReclaimMode) error
	UnlinkKeys(context.Context, []string) (int64, error)
}

// Watch observes requested keys including absent ones. The first write,
// expiration, eviction, deletion or clear affecting a watched key permanently
// invalidates it, even if the original value is later restored. Wait blocks
// until invalidation, closure or context cancellation; cancellation does not
// release the watch. Close releases it and unblocks Wait. Watch creation and
// registration are atomic with mutations, preventing lost wakeups. An empty key
// list watches nothing. A watch belongs to the store that created it.
// Higher layers use this for WATCH, blocking commands and client tracking;
// connection state, queues and keyspace Pub/Sub delivery remain outside storage.
type Watch interface {
	Wait(context.Context) error
	Close() error
}

// Transaction is a borrowed staged keyspace view valid only during AtomicFunc.
// Get and Scan expose live values at Now's fixed Unix millisecond snapshot.
// Put/Delete/Clear affect subsequent reads within the callback. Put replaces a
// whole immutable entry, including TTL; an already expired entry becomes absent.
// Methods must not reenter the owning Store or be retained after callback return.
type Transaction interface {
	Now() int64
	Get(string) (Entry, bool)
	Put(string, Entry)
	Delete(string)
	Scan(Cursor, int) EntryPage
	Size() int64
	Clear(ReclaimMode)
}

type AtomicFunc func(Transaction) error

// AtomicStore provides an isolated keyspace callback for EXEC, scripts and
// functions that discover keys dynamically. A nil watch is unconditional. A
// foreign or closed watch is an error; an invalidated watch returns false,nil
// without invoking the callback. On success the callback runs once and all
// staged changes publish together, after required persistence. A callback error
// discards user changes; ErrUnchanged is a successful no-op. The bool reports
// whether the watch check passed. Expiration maintenance can occur on abort.
// Redis EXEC/script command errors are results collected by the caller, not
// callback errors: Redis does not roll back earlier successful commands.
type AtomicStore interface {
	Watch(context.Context, []string) (Watch, error)
	Atomic(context.Context, Watch, AtomicFunc) (bool, error)
}
