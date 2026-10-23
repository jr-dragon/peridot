# RDB and multipart AOF persistence

## Scope and reference

Peridot uses Redis 8.10.2 as the interoperability reference. The implementation
follows the [official persistence documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/):
RDB is a point-in-time snapshot; multipart AOF consists of an optional base,
ordered incremental command files, and an atomically replaced manifest.

Working data remains in the Xsync memory backend, accessed through the replaceable
`storage.Backend` interface. Persistence is independent of the backend implementation.
`internal/database.DB` coordinates mutations and snapshots;
`internal/persistence.Engine` owns recovery, RDB snapshots, multipart AOF, and locks.
Wire codecs remain separate from command execution and the memory representation.
No new dependencies or project-owned CGO code are required.

Ordinary key operations share a database gate. Coordinated updates use
raw backend `Compute` for single-key exclusion, without database key stripes.
The shared gate is separated from frequently read database fields with an
architecture-sized cache-line pad, avoiding false sharing after stripe removal.
Live reads use concurrent backend loads without a stripe, including when AOF is
enabled. Expired observations reload under mutation coordination before deleting.
Both AOF and RDB persistence permit independent backend buckets to proceed concurrently; active
expiration maintains its index under a separate mutex, and dirty accounting is
atomic. Snapshots, multi-key deletion, expiration samples, and close take the
exclusive gate. AOF updates hold the backend's Compute bucket lock until append
completes, preserving same-key order and append-before-publication. Different
keys commute; their memory publications need not follow their AOF record order.
Unknown persistence implementations retain global mutation ordering and the
original Load/Append/Put path. Raw Compute remains a memory primitive; durability
comes from the persistence call inside its callback.

The supported dataset is binary-safe string keys and values in database zero,
including absolute millisecond expiration. Other databases, collection types,
functions, modules, and module key metadata fail explicitly during import.
Supporting the file container does not imply support for every Redis data type.

## Configuration and recovery

```sh
mkdir -p /tmp/peridot-data
go run ./cmd/server --dir /tmp/peridot-data --appendonly yes --appendfsync everysec
```

The default RDB path is `dir/dump.rdb`, configurable through `dbfilename`.
`appendonly yes` uses `dir/appenddirname/appendfilename.manifest`, with defaults
`appendonlydir` and `appendonly.aof`. `aof-use-rdb-preamble yes` produces RDB bases;
`no` produces plain command bases. Both forms can be imported regardless of the
current output setting.

When AOF is enabled, its manifest is the sole recovery authority. Peridot does
not fall back to `dump.rdb` on a missing, empty, or corrupt AOF dataset. Without
a manifest, an existing legacy `dir/appendfilename` is validated and imported;
plain and RDB-preamble legacy files are supported. Migration preserves the original
legacy file as a backup; subsequent starts use the manifest. With neither manifest
nor legacy log, AOF starts empty, even if a standalone snapshot exists. Changing
`appendonly` in a configuration file is not a live RDB-to-AOF conversion.

With AOF disabled, startup loads `dbfilename` if present, even with `save ""`.
A missing RDB starts empty; malformed data aborts startup before listeners open.
Recovery validates the complete dataset privately before publishing it. A manifest
may contain history entries whose files have already been deleted; they are not
replayed. Active base and incremental files must exist. Invalid sequences,
duplicate entries, unsafe paths, truncated files, and checksum errors abort startup.
No input is automatically repaired or truncated.

All AOF segments replay against one historical state. Expired entries remain
available to subsequent `PERSIST`, `KEEPTTL`, and deletion/recreation records until
all segments have loaded. Only then are expired entries removed. Supported AOF
commands remain `SELECT 0`, `SET`, `SETNX`, `DEL`, `UNLINK`, `PEXPIREAT`, `PERSIST`,
and complete `MULTI`/`EXEC` groups, plus timestamp annotations. Unsupported commands
and relative TTL input fail explicitly. Transactions cannot cross file boundaries.

## RDB format and snapshots

The RDB codec reads the raw and integer-encoded strings and LZF-compressed strings
emitted by Redis 8.10.2, expiration metadata, database selection, AUX metadata,
resize hints, and idle/frequency metadata. It verifies CRC-64/Jones checksums and
accepts the zero checksum used by Redis when checksumming is disabled. Expiration
at epoch zero is distinct from no expiration during recovery.

Output uses RDB version 15, uncompressed strings, millisecond expiration, and a
checksum. Redis may choose different encodings when it rewrites the same dataset.
The export does not reproduce Redis's memory layout or eviction metadata.

`SAVE` writes synchronously. `BGSAVE [SCHEDULE]` captures an immutable snapshot
under the database mutation gate and writes it in a goroutine. `LASTSAVE` returns
the last successful RDB save time, initially the startup time. Scheduled saves use
Redis's default `save 3600 1 300 100 60 10000` rules. The first explicit `save`
replaces defaults, subsequent rules accumulate, and `save ""` clears the rules.
The scheduler checks once per second. Only changes included in a successful
snapshot are removed from the dirty count. A failed save preserves the previous
snapshot. A background save failure, with active save rules and
`stop-writes-on-bgsave-error yes`, blocks user writes and PING until a save
succeeds. A failed foreground SAVE leaves that status unchanged. SET checks
command arity first. Valid requests check write protection once inside Update;
option validation failures check write protection before returning the error.
Thus disk errors retain precedence over invalid options. Successful same-value
SET operations count toward save thresholds; failed conditions do not. Internal filesystem
details are logged, not included in command replies.

Snapshot output is written to a temporary file in the destination directory,
flushed, fsynced, renamed, and followed by a directory fsync. The snapshot map is
copied while writes are excluded; its string values are immutable. Capturing the
map requires an O(number of keys) pause and additional memory. Disk serialization
for BGSAVE runs after the gate is released. Foreground SAVE holds the gate through
publication. This implementation does not fork a Go process.

## Multipart rewrite and durability

Every effective mutation is resolved under the database gate, appended before
memory publication, and encoded as an unconditional `SET` with optional `PXAT`,
or `DEL`. Multi-key deletions append a single `DEL` record before publishing any
of their memory changes. An append failure leaves all affected keys unchanged;
a torn record is rejected on recovery rather than replayed as a partial deletion.
Successful same-value replacements also append; conditional no-ops
explicitly return `storage.ErrUnchanged` and do not append. Existing fsync
policies are preserved:

- `always`: sync the batch containing each mutation before acknowledgment.
- `everysec` (default): write before acknowledgment, sync pending data in a worker.
  Slow storage can extend the loss window beyond one second.
- `no`: write immediately and leave ordinary flushing to the operating system.

Concurrent appenders submit resolved mutations to the existing xsync UMPSCQueue.
One consumer encodes and writes batches of at most 256 requests or approximately
256 KiB (a single larger record is kept intact). It drains already reserved
requests without a batching timer. Each synchronous caller waits for its own
batch result before memory publication; `always` also waits for Sync. Same-key
ordering still comes from the caller's backend bucket lock. The consumer never
accesses the backend, so it cannot wait on a bucket held by a producer.

Encoding copies binary-safe arguments directly into a reusable RESP buffer
without constructing intermediate RESP values. Buffers larger than 64 KiB are
discarded after writing. Requests are pooled and their payload references cleared
before completion, because queue segments can retain request pointers. The queue
adds a writer goroutine and segment storage: the first enqueue allocates two
4,096-slot segments, approximately 192 KiB for pointer and WaitGroup slots on
this platform, before request, buffer, and pool overhead. There is at most one
queued request per synchronous in-flight persistence caller; the implementation
does not acknowledge early to accumulate an unlimited asynchronous backlog.

A partial write or failed sync completes affected and queued requests with the
persistence failure and prevents publication. Each completed request retains its
own result, so a later failure cannot change an earlier successful result.
Rotation waits for active I/O, with mutations excluded by the database gate.
Close first rejects new submissions, places a sentinel after all accepted
producers, and joins the consumer before closing its descriptor. Background
`everysec` Sync can still overlap ordinary appends.
Engine status checks release their own gate before waiting on the AOF gate, so a
contended append cannot serialize unrelated keys through a nested status lock.

This batching preserves the existing memory fast paths but does not remove
same-key or same-bucket exclusion. A hot key, a single connection, or slow disk
can still limit batch size and throughput, especially with `always`.

All policies sync newly created files and their directory entries, manifest
publication, rewrite completion, and final shutdown. Append or sync failure
poisons further appends. Unacknowledged writes may still be recovered after an
ambiguous failure; an error is not proof that a write was rolled back on disk.

AOF failures produce recoverable `MISCONF Errors writing to the AOF file: ...`
replies for writes and `PING`, preserving pipelined reads and the connection.
Replies contain only an OS error description, not file paths or internal errors.
Passive and active expiration continue removing expired entries after an AOF
failure. Their persisted deadlines prevent them from reappearing during recovery;
expiration does not clear write protection.

`BGREWRITEAOF` schedules the following operation:

1. Capture a consistent live snapshot under the database gate.
2. Sync the current increment, create and sync a fresh increment, and publish a
   manifest retaining the old base and increments plus the new increment.
3. Resume mutations against the new increment while serializing the snapshot to
   a new base. The previous file set remains sufficient for recovery throughout.
4. Sync the new base and current increment, then atomically publish a manifest
   containing only the new base and increment.
5. Remove obsolete files only after the replacement manifest is durably published.

With `appendonly no`, manual `BGREWRITEAOF` captures the dataset before replying
and publishes only a base snapshot and manifest. It does not enable logging,
create an incremental file, or include subsequent writes. The next manual
rewrite replaces that export with a new snapshot; ordinary startup still loads
RDB while AOF is disabled. Both plain and RDB bases use the configured preamble
setting and the same manifest ownership and atomic publication rules.

A failed or canceled base write leaves the rotated manifest intact. The next
rewrite compacts the accumulated increments. Names are never reused over existing
orphan files. A failure after manifest rename with uncertain directory durability
poisons writes and retains possibly referenced files. Cleanup is best effort;
unreferenced files left by crashes are ignored rather than guessed into recovery.

BGSAVE and background AOF rewrite do not run together. `BGREWRITEAOF` during a
snapshot is scheduled afterward; `BGSAVE SCHEDULE` during rewrite is likewise
scheduled. Ordinary BGSAVE during rewrite returns Redis's error. Foreground SAVE
may run during AOF rewrite, matching Redis. Shutdown stops scheduling, waits for
active jobs, writes a final RDB if save rules are active and data changed, and
syncs/closes AOF. The RESP drain timeout cannot bound an operating-system disk stall.

Advisory locks protect the RDB path and manifest for the engine lifetime, and the
append descriptor is kept alive across background sync and rotation. Files must
not be externally modified while Peridot owns them. Locks coordinate Peridot
instances; Redis itself does not honor Peridot's sidecar locks. Run interoperability
processes against separate copies or stop one owner before starting the other.

## Compatibility limits

- Mutations whose own AOF append fails are rejected before memory publication.
  Redis with `appendfsync no` or `everysec` can complete that triggering command
  before reporting the disk failure on subsequent commands. Peridot retains
  write protection until restart rather than retrying failed AOF writes; it
  also does not terminate the process on `appendfsync always` failure as Redis does.
- Automatic AOF rewrite size/percentage thresholds, `INFO persistence`, runtime
  `CONFIG SET`/`CONFIG REWRITE`, the Redis 8.10 `BACKUP` family, `preload-file`, and
  the `SHUTDOWN` command are not implemented.
- Truncated AOF input fails closed, unlike Redis's default tail-repair policy.
  `aof-load-truncated` is not implemented. Backup all manifest-referenced files
  together; copying only one incremental log is not a backup.
- RDB compression/checksum output tuning is not implemented. Output is always
  uncompressed and checksummed; Redis compressed input is supported.
- Snapshot/rewrite capture copies the key map and pauses mutations. AOF rotation
  and final manifest publication also wait for fsync. Background serialization
  does not eliminate those pauses or provide Redis fork/COW memory behavior.
- Legacy `OpenAOF` remains available for existing single-file callers. Server
  configuration uses the new engine and multipart layout.

## Validation

```sh
REDIS_REFERENCE=/path/to/redis-server go test -v ./internal/server -run '^TestPersistenceRedisBidirectional$'
go test ./...
go test -race ./...
go vet ./...
go build -o ./bin/peridot-server ./cmd/server
```

The differential suite requires exactly Redis 8.10.2, starts isolated reference
processes, and transfers complete RDB or manifest-referenced file sets without
flattening them. With the Xsync backend and both base formats it verifies
Redis → Peridot → Redis → Peridot, including binary/empty data, Redis integer and
LZF encodings, absolute expiry, deletion, post-base mutations, rewrite, and
restart. Unit tests cover missing/corrupt files, checksum and LZF validation,
legacy hybrid migration, ownership, rewrite cancellation, writes during a
checkpoint, snapshot scheduling, failure recovery, and shutdown waiting.

Failure regression tests against Redis 8.10.2 cover foreground SAVE versus
BGSAVE write protection, SET error precedence, and AOF rewrite after an RDB
failure with AOF logging enabled or disabled. Rewrite preparation failures return
Redis's recoverable command error, preserve pipelined follow-up commands in
RESP2/RESP3, and allow retry after the filesystem problem is corrected. RDB write
protection does not prevent an independent AOF rewrite or clear when it succeeds.

References:

- [Official persistence guide](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis 8.10.2 AOF manifest and rewrite](https://github.com/redis/redis/blob/8.10.2/src/aof.c)
- [Redis 8.10.2 RDB constants](https://github.com/redis/redis/blob/8.10.2/src/rdb.h)
- [Redis 8.10.2 RDB loader and save commands](https://github.com/redis/redis/blob/8.10.2/src/rdb.c)
- [Redis 8.10.2 CRC](https://github.com/redis/redis/blob/8.10.2/src/crc64.c)
