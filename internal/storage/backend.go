package storage

// ComputeOp describes the physical effect of a raw atomic computation.
type ComputeOp uint8

const (
	ComputeCancel ComputeOp = iota // Leave the physical entry unchanged.
	ComputeUpdate                  // Replace or insert the complete entry.
	ComputeDelete                  // Remove the physical entry, if present.
)

// ComputeFunc runs exactly once under exclusive access to a physical key,
// including an expired entry. It must not reenter the backend or mutate a
// published value. The owner coordinates expiration, metadata and persistence.
type ComputeFunc func(current Entry, exists bool) (Entry, ComputeOp)

// RawEntries is the physical key/value boundary. These operations never expire
// entries, update access metadata, or persist changes. Put replaces the complete
// entry (including TTL), even if the key already exists. Compute invokes its raw
// callback once and atomically publishes the returned effect.
type RawEntries interface {
	Load(string) (Entry, bool)
	Put(string, Entry)
	Delete(string)
	Compute(string, ComputeFunc)
}

// RawKeyspace traverses physical entries, including logically expired entries.
// Range stops when yield returns false. Concurrent traversal is weakly
// consistent; the owner excludes mutations for a point-in-time view.
type RawKeyspace interface {
	Len() int64
	Range(yield func(string, Entry) bool)
	// ScanEntries has the cursor guarantees documented on Cursor.
	ScanEntries(Cursor, int) EntryPage
	// SampleEntries returns up to count distinct physical entries; count > 0.
	SampleEntries(count int) []KeyEntry
	// ClearEntries detaches the entire keyspace before reclaiming old entries.
	ClearEntries(ReclaimMode)
}

// RawBatch publishes already resolved changes. The owner must exclude all
// other access for the entire operation; Apply is not a substitute for the
// command-facing atomic callback or a persistence transaction. Mutation keys
// must be distinct. No callbacks or expiration checks run during publication.
type RawBatch interface {
	Apply([]Mutation)
}

// RawSnapshots supports checkpoints and recovery. Snapshot copies the map but
// may share immutable payloads. ReplaceEntries copies the supplied map and
// replaces all physical entries, including TTLs. The owner excludes mutations
// during Snapshot and all other access during ReplaceEntries.
type RawSnapshots interface {
	Snapshot() map[string]Entry
	ReplaceEntries(map[string]Entry, ReclaimMode)
}

// RawMetadata supports object inspection and eviction policy accounting without
// turning a metadata lookup into a key access. Missing entries return false.
type RawMetadata interface {
	EntryMetadata(string) (Metadata, bool)
	TouchEntry(key string, now int64)
	// SetEntryMetadata restores access accounting (RESTORE IDLETIME/FREQ).
	// It does not change the value, key TTL or modification history.
	SetEntryMetadata(string, Metadata)
}

// Backend is the replaceable physical memory backend owned by database.DB.
// It deliberately does not embed Store: a backend need not implement command
// semantics, expiration, or persistence. Compute serializes one physical key;
// the owner coordinates metadata and excludes operations for checkpoints and
// batches. Direct Store calls are excluded while using this interface.
// Raw Load/Put/Delete/Compute must individually support concurrent calls.
type Backend interface {
	RawEntries
	RawKeyspace
	RawBatch
	RawSnapshots
	RawMetadata
}
