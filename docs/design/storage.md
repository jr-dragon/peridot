# Storage capability contracts

## Boundary and implementation status

The reference is standalone Redis **8.10.2**. Storage exposes memory operations
shared by Redis features; commands retain parsing, validation, reply construction
and data-type algorithms. A method is not added for every command alias or option.
These contracts describe planned capabilities, not implemented Redis commands.

`Store` composes `KeyStore`, `MultiKeyStore`, `KeyspaceStore`, `AtomicStore` and
`MetadataStore`. Existing commands depend on `KeyReader`, `KeyUpdater` or
`KeyDeleter`; command registration needs only their aggregate, `KeyStore`.
`database.DB` currently implements `KeyStore`. It must gain coordinated versions
of the future capabilities before commands can use them with persistence.

`Backend` is independent of `Store`. It composes `RawEntries`, `RawKeyspace`,
`RawBatch`, `RawSnapshots` and `RawMetadata`. The database owns this physical
backend and coordinates expiration, locking, publication and persistence. A raw
backend can be wrapped in a database without implementing command-facing methods.
When the backend also implements `KeyStore`, the existing memory-only fast path
remains available with persistence and active expiration disabled.
An explicit database clock override uses raw coordination so expiration and
callback timestamps use that clock independently of the backend.

Xsync implements the existing single-key operations, multi-key deletion,
`Load`, `Put`, `Delete`, raw `Compute`, `Snapshot`, `Len` and `Range`. Every other new Xsync
method explicitly calls `panic("todo")` in `xsync_todo.go`. No new command is
registered and no placeholder is called by current commands. Compile-time
assertions check both the complete planned `Store` and `Backend` method sets.

## Redis feature coverage

| Redis feature | Memory capabilities | Responsibilities above the map |
| --- | --- | --- |
| Strings, numeric updates, conditional SET, ranges, bitmaps and HyperLogLog | `KeyReader`, `KeyUpdater`, `MultiKeyStore` | String operations, overflow, bit offsets, HLL algorithms and options |
| Lists and blocking/multi-list moves | `KeyUpdater`, `MultiKeyStore`, `Watch`, `Atomic` | List payload, key ordering, timeouts and blocked-client fairness |
| Sets, sorted sets, GEO, unions/intersections and destination stores | `KeyUpdater`, `MultiKeyStore` | Membership, scores, rank/lex ordering, coordinates and type validation |
| Hashes and hash field expiration | `KeyUpdater`, `TypedValue`, `ExpiringValue` | Fields, field TTL conditions, passive field expiration and an active sub-expiration index |
| Streams, groups, pending entries, claims and IDMP | `KeyUpdater`, `MultiKeyStore`, `Watch`, `Atomic` | Stream IDs, consumer state, acknowledgments and producer deduplication |
| Native sparse arrays | `KeyUpdater`, `TypedValue` | Array indexes, ranges and element operations |
| Module values and optional GCRA values | `TypedValue`, atomic callbacks | Value adapters; GCRA exists only in reference builds with `ENABLE_GCRA` |
| EXISTS, TYPE, TTL/EXPIRE/PERSIST families | `Read`, `Update`, `ReadKeys`, `Inspect` | Exact count rules, TTL units, condition precedence and type names |
| DEL, UNLINK, RENAME, COPY and SORT STORE | `DeleteKeys`, `UnlinkKeys`, `UpdateKeys` | Source/destination validation, deep-copy adapters where needed and resolved effects |
| KEYS, SCAN, RANDOMKEY and DBSIZE | `Scan`, `RandomKey`, `Size` | Binary-safe glob matching, TYPE filtering and RESP cursors |
| FLUSHDB and asynchronous reclamation | `Clear`, `ClearEntries`, `ReclaimMode` | Persistence records, watch invalidation and client notifications |
| MULTI/EXEC/WATCH, scripts and functions | `Watch`, `Atomic`, `Transaction` | Command queues, interpreters, error replies and script/function registry |
| OBJECT, MEMORY, TOUCH, eviction and RESTORE access hints | `Inspect`, `Touch`, raw metadata and `SampleEntries` | Policy selection, memory budget, eviction effects and metrics |
| DUMP/RESTORE, recovery, snapshots and rewrite | `RawEntries`, `RawBatch`, `RawSnapshots` | Redis codecs, durable records, file ownership and checkpoint lifecycle |

Bitmap and HyperLogLog values use Redis's string type; GEO uses sorted sets.
Other values supply `TypedValue.Type`, including Redis 8.10.2's Array type.
String payloads remain Go strings, preserving existing commands and persistence.
Values and nested structures are immutable after publication. Storage never
assumes that an `any` payload is comparable or safe to mutate in place.

`ExpiringValue` exposes a nested deadline and immutable expiration result. The
coordinator can schedule hashes by their earliest field deadline independently
of key TTLs. Expired field names are explicit so future persistence can record
resolved field deletions before publishing the replacement payload. Defining
this interface does not enable hash fields or change current expiration behavior.

## Atomicity, ownership and expiration

Single-key update callbacks run once under exclusive access and cannot reenter
the same store. Read callbacks observe immutable entries without excluding
concurrent replacement. Missing and expired keys are absent, and callbacks receive
one Unix millisecond clock snapshot. Returned payloads must be immutable. `ErrUnchanged`
means no user mutation; ordinary errors retain the live value. Expiration
maintenance may still remove already expired entries on an aborted update.

Multi-key callbacks receive a borrowed map containing only live requested keys.
Duplicate input keys are locked once; callers retain their original argument
order for reply construction. Read callbacks cannot modify the map or payloads.
Update callbacks return distinct-key mutations restricted to the requested keys;
omitted keys are unchanged. The coordinator validates the entire batch, resolves
expiration, persists its complete effect and then publishes it without exposing
intermediate states. The current persistence interface cannot yet append general
multi-key batches, so future durable implementations need a matching persistence
contract rather than a loop over existing single-key appends.

`Atomic` provides a staged keyspace view for operations whose keys are discovered
during execution. The optional watch must belong to that store and be open.
Watch validation, callback execution and publication share one isolation boundary.
A watch must detect expiration, deletion/recreation and changes to absent keys;
an unrelated key modification must not invalidate it. The coordinator checks
watched expiration at execution time, even if no earlier access deleted the key.
Returning a callback error discards staged user writes. Redis command errors
inside EXEC or a script are collected as command results by the caller; they
must not inadvertently roll back preceding successful commands.

Watch registration is atomic with mutations. A blocking-command implementation
registers first, tries its operation atomically, then waits; it releases the
watch and repeats after a wakeup. This prevents missed notifications. Watches
are one-shot invalidation handles, not an event log or a FIFO client scheduler.
Keyspace Pub/Sub notification payloads remain the coordinator's responsibility.

Raw operations do not apply expiration or command semantics. Raw `Compute`
executes its callback exactly once under single-key exclusion, including for
absent or physically expired entries. Its result explicitly cancels, replaces or
deletes the entry. The callback must not reenter the backend or mutate published
payloads. Non-AOF database updates use it to coordinate value and TTL-index
changes; expired reads use the same primitive to recheck replacements. The owner
excludes direct Store access during coordinated raw operations, excludes
mutations for a consistent snapshot, and excludes all other access during raw
batch/replacement publication. `Apply` does not supply transaction isolation by
itself. Snapshot and replacement copy map structure while sharing only immutable
payloads. Reclamation mode changes when memory is reclaimed, never when keys
become invisible. Cancellation cannot leave a partially published operation.

Xsync snapshots preallocate their result map from physical size and use
`RangeRelaxed` while the database gate excludes mutations. Public raw `Range`
retains xsync's stronger at-most-once traversal behavior under concurrent writes.
Direct Xsync `Read` uses `Load` for live values and rechecks expired observations
with `Compute`; direct `DeleteKeys` uses `LoadAndDelete` under its exclusive gate.
The database's durable deletion path still resolves keys before one append and
only then publishes deletions.

Database updates keep the shared checkpoint gate through publication and index
maintenance, without a database key stripe. The pure memory fast path delegates
to Xsync's command-facing Update. The coordinated path calls raw Compute; concrete
Xsync dispatch keeps callback state on the stack, while backend wrappers retain
an interface fallback with the same semantics. AOF and unknown persistence keep
the exclusive database mutation gate and append-before-publication order.

## Traversal and metadata

`Cursor` is opaque and scoped to a store and scan. Zero starts or completes a
scan. Count is a work hint; empty nonterminal pages and duplicate results are
permitted. A full iteration returns keys continuously present throughout that
iteration. Scans must make progress without allocating a complete keyspace
snapshot per page. Logical scans and random-key selection hide expired entries;
raw traversal and physical size can include them. Redis DBSIZE uses physical
cardinality rather than running a synchronous full expiration sweep.

`Range` supports early termination; `SampleEntries` returns distinct physical
candidates for eviction or maintenance. Metadata access does not implicitly
refresh idle time or LFU counters. Availability flags distinguish an unsupported
metric from zero. Encoding labels come from the Redis-facing value adapter,
independently of the concurrent map's layout. `SetEntryMetadata` reserves access
accounting restoration for RESTORE IDLETIME/FREQ without changing value or TTL.

## Composition outside a single keyspace

Each Store/Backend instance represents one logical database. A future database
registry can own several instances and coordinate MOVE, SWAPDB and FLUSHALL
using their raw boundaries under a shared gate. The current SELECT rejection
remains unchanged; this design does not enable multiple databases.

ACLs, connection/session state, Pub/Sub channels, script/function registries,
replication transport and server administration belong to their own services.
They use the memory capabilities when changing data but are not concurrent-map
methods. Cluster and Sentinel remain outside the project target. RDB/AOF remain
separate persistence services and do not implement Store or Backend.

## Pinned reference sources

- [Redis 8.10.2 object types and database API](https://github.com/redis/redis/blob/8.10.2/src/server.h)
- [Redis 8.10.2 keyspace, type names, scan and physical size](https://github.com/redis/redis/blob/8.10.2/src/db.c)
- [Redis 8.10.2 command inventory](https://github.com/redis/redis/blob/8.10.2/src/commands.def)
- [Redis 8.10.2 hash field expiration](https://github.com/redis/redis/blob/8.10.2/src/t_hash.c)
- [Redis 8.10.2 transactions and watched keys](https://github.com/redis/redis/blob/8.10.2/src/multi.c)
