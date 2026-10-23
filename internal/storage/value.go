package storage

import (
	"context"
	"time"
)

// ValueType identifies a logical Redis type, independently of its encoding.
// Bitmap and HyperLogLog use strings; GEO uses a sorted set. Modules supply their
// registered name through Metadata.ModuleType. Array is native in Redis 8.10.2.
type ValueType string

const (
	TypeString    ValueType = "string"
	TypeList      ValueType = "list"
	TypeSet       ValueType = "set"
	TypeSortedSet ValueType = "zset"
	TypeHash      ValueType = "hash"
	TypeStream    ValueType = "stream"
	TypeArray     ValueType = "array"
	TypeModule    ValueType = "module"
	// TypeGCRA is conditional on Redis's ENABLE_GCRA build option.
	TypeGCRA ValueType = "gcra"
)

// TypedValue is the immutable payload boundary for non-string values.
// Collection contents, hash field TTLs, stream consumer groups/PEL/IDMP and
// array indexes belong to these values, not the concurrent key map. Updates
// create new payloads; a nested collection must never mutate published state.
type TypedValue interface {
	Type() ValueType
}

// ExpiringValue exposes nested expiration work (notably Redis hash field TTLs).
// NextExpiration returns the earliest nested deadline or zero when none exists.
// Expire returns a new immutable value and the distinct expired field names;
// empty requests removal of the containing key. The coordinator invokes this
// under atomic access, persists resolved effects before publication, and keeps
// key-level and nested-expiration indexes independently. It does not refresh TTLs.
type ExpiringValue interface {
	TypedValue
	NextExpiration() int64
	Expire(now int64) (next TypedValue, expiredFields []string, empty bool)
}

// Metadata describes a physical object's logical type and backend accounting.
// Encoding is a Redis-facing label supplied by the value adapter, independently
// of the map's physical layout. MemoryUsage is an allocation estimate in bytes.
// IdleTime and Frequency describe the selected eviction policy. Flags
// distinguish unsupported metrics from valid zeros. Inspect does not touch access metadata.
type Metadata struct {
	Type           ValueType
	ModuleType     string
	Encoding       string
	MemoryUsage    uint64
	IdleTime       time.Duration
	Frequency      uint8
	RefCount       uint64
	HasMemoryUsage bool
	HasIdleTime    bool
	HasFrequency   bool
	HasRefCount    bool
}

// MetadataStore supports TYPE/OBJECT/MEMORY inspection, TOUCH and future
// eviction policy accounting. Inspect hides expired keys. Touch returns the
// number of live key arguments, counting duplicates as Redis TOUCH does.
type MetadataStore interface {
	Inspect(context.Context, string) (Metadata, bool, error)
	Touch(context.Context, []string) (int64, error)
}
