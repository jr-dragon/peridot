# Persistence implementation plan

## Phase 1: Single-file AOF and rewrite

Implementation is present. The first version synchronizes every effective
mutation and uses a blocking consistency gate during manual rewrite; background
sync, automatic rewrite thresholds, RDB, and multi-part files remain later work.

1. Add strict AOF decoding and binary-safe command encoding using existing RESP
   facilities where their contracts fit file parsing. Support current string
   state, absolute expiration, deletion, database zero, timestamp annotations,
   and complete transaction groups. Reject unsupported formats and commands.
2. Keep Xsync and DLHT as independent memory backends. Add AOF persistence with
   exclusive file ownership and failure poisoning, and a database coordinator
   that orders synchronous append before memory publication. Replay preserves
   historical state before evaluating final expiration.
3. Add a consistent storage snapshot boundary and single-file rewrite with
   temporary-file sync, atomic replacement, directory sync, and cleanup.
4. Add opt-in executable configuration, startup loading before listeners,
   `BGREWRITEAOF`, and persistence closure after connection workers finish.
   Preserve existing in-memory operation when persistence is disabled.
5. Add behavior tests and Redis 8.10.2 interoperability tests. Cover original and
   rewritten output in Redis, Redis-generated command streams in Peridot, binary
   payloads, TTL, no-op conditional SET, truncated/corrupt input, concurrency,
   write/sync failure, and restart. Document unsupported cases explicitly.
6. Regenerate DI output and run focused tests, `go test ./...`, `go vet ./...`,
   `go test -race ./...`, the server build, and `git diff --check`.

Acceptance: Peridot produces and reads Redis-compatible single-file AOF for its
implemented string state, including rewrite. RDB preambles and multi-part AOF are
not accepted in this phase. Redis fixtures must come from Redis 8.10.2 with
`aof-use-rdb-preamble no`; Redis may upgrade imported legacy output into its own
multi-part layout during verification.

Validated with Go 1.27.3 and Redis 8.10.2 on macOS:

- `REDIS_REFERENCE=/opt/homebrew/bin/redis-server go test ./...`
- `REDIS_REFERENCE=/opt/homebrew/bin/redis-server go test -race ./...`
- `go vet ./...`
- `go build -o ./bin/peridot-server ./cmd/server`
- `git diff --check`

The Go build cache was placed in a writable temporary directory. Tests requiring
local TCP/Unix sockets ran with local-listener access. Both original and rewritten
AOF passed interoperability in both directions. Fault tests cover failed/short
appends, failed sync, failed rename, and failure after replacement. These tests do
not establish power-loss guarantees for every filesystem or storage device.

## Phase 2: RDB checkpoint codec

Implement version-aware RDB reading/writing for supported types, checksums,
length/integer/compressed encodings, and expiration metadata. Verify Redis-produced
input and Redis-readable output, including compact encodings. Fail explicitly on
unsupported types. Add HLL/GEO compatibility when their commands are implemented.
No external codec or compression dependency is approved by this plan.

## Phase 3: Multi-part persistence and concurrent checkpoints

Introduce RDB base plus incremental logs and a manifest with crash-safe switching,
log boundaries, garbage collection, and consistent backup. Replace blocking
snapshot/rewrite with a measured concurrency design. Add restart fault injection
at publication boundaries and validate Redis-compatible file sets.

## Phase 4: Expanded semantics and performance

Add typed collection effects, multi-key atomic operations, transaction persistence,
and any implemented sub-key expiration. Consider group commit and `everysec`
only with explicit acknowledgment and failure policies. Measure p50/p99, sync
latency, write amplification, checkpoint pause and peak RAM, and recovery time
using large collections, hot keys, and TTL-heavy workloads before changing engines.
