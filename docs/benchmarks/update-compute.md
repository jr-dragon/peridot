# Non-AOF atomic updates

Measured on 2026-10-23 against baseline commit `92e29f9`, after the read-fast-path changes.

## Change and correctness

- Isolate the shared database gate from hot read fields with the existing golang.org/x/sys/cpu CacheLinePad. Its architecture-dependent size prevents the former stripe array removal from introducing false sharing.
- Remove the database's 256 key stripes. The direct memory path delegates to Xsync Update under the shared database gate.
- Coordinated non-AOF updates use raw backend Compute. It invokes its callback exactly once with the physical entry, including expired entries, and explicitly cancels, replaces or deletes the entry.
- Expired Get/Read rechecks use the same Compute primitive. Cancellation is checked again after bucket acquisition.
- TTL-index updates occur inside the computation before physical publication. The shared database gate spans the entire operation, including completion after Compute returns, so checkpoints, expiration samples, multi-key deletion and Close cannot observe incomplete bookkeeping.
- Concrete Xsync dispatch avoids callback-state escape allocations. Backend wrappers retain a fallback through the interface. All measured update cases retain 48 B/op and one allocation per operation.
- AOF-enabled and unknown persistence keep the exclusive database mutation gate and original Load/Append/Put path. The ordered Update body, expireKey and direct Xsync.Update are unchanged. Compute is not used to hold a bucket mutex across AOF disk writes or fsync.

Validation passed with Go 1.27.3: full `go test ./...` and `go test -race ./...` with `REDIS_REFERENCE=/opt/homebrew/bin/redis-server`, `go vet ./...`, the server build and `git diff --check`.

Regressions cover raw atomic increments during growth, exactly-once callbacks, expired physical values, cancellation/no-op/delete behavior, cancellation after bucket waits, expired replacements, TTL-index ordering when a completed Compute has not yet returned to DB, removal of redundant direct-path stripes, dirty counts and append failures. Existing snapshot and AOF ordering tests pass. New Redis 8.10.2 concurrent conditional-write tests run 16 contenders with RESP2 and RESP3 in memory, active-expiration, RDB and AOF modes. They verify exact success/null replies, one winner for expired-key NX and IFEQ/KEEPTTL, zero winners for live-key NX, the winner's binary-safe value, expiration, and a successful follow-up PING. Existing conditional SET/keyspace differential tests and bidirectional RDB/AOF/rewrite interoperability tests also pass. This validates the implemented command scope, not all Redis commands.

## redis-benchmark comparison

Apple M2 Pro, macOS arm64, Go 1.27.3, Xsync v4.5.0, Redis server and redis-benchmark 8.10.2. Peridot uses GOMAXPROCS=10; Redis uses one I/O thread. All three isolated localhost TCP servers use RESP2, keepalive, `save ""` and `appendonly no`, with separate temporary directories and dynamic ports. Peridot retains its configured RDB engine and active-expiration coordinator.

Each workload starts with 65,536 identical keys and explicit 128-byte values. Uniform SET overwrites all keys; hot SET targets one existing key; TTL SET and hot TTL SET use the same absolute PXAT deadline (4102444800000) for every key and overwrite; GET is the unchanged live-read control. Hot TTL SET targets one key and was measured in an additional isolated batch after the four primary workloads, to investigate its microbenchmark regression. Every key's value and TTL category are checked before and after each workload. Redis-benchmark custom commands use an explicit payload, avoiding assumptions about generated SET data.

128 clients, four benchmark threads, pipelines 1 and 16. Pipeline 1 uses one million requests per measured run; pipeline 16 uses eight million. Each combination has a 20,000-request warmup. Three measured trials rotate server order Before/After/Redis, After/Redis/Before and Redis/Before/After. The microbenchmarks finish before the network tests start; no test/build/profile workload overlaps the measurements.

Results are three-run medians. Threaded redis-benchmark uses a 250 ms completion timer; small changes near one timer tick are not precise evidence of improvement. Pipeline latency measures batch round trips rather than isolated command execution.

| Pipeline | Workload | Before req/s | After req/s | Change | Redis req/s | After / Redis |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | set-uniform | 142,755 | 142,755 | +0.00% | 137,874 | 1.035x |
| 1 | set-hot | 142,755 | 142,755 | +0.00% | 142,796 | 1.000x |
| 1 | set-ttl | 142,755 | 142,755 | +0.00% | 133,298 | 1.071x |
| 1 | set-ttl-hot | 142,735 | 142,735 | +0.00% | 137,874 | 1.035x |
| 1 | get-control | 147,995 | 142,735 | -3.55% | 142,796 | 1.000x |
| 16 | set-uniform | 1,522,070 | 1,596,806 | +4.91% | 969,344 | 1.647x |
| 16 | set-hot | 666,500 | 727,140 | +9.10% | 1,102,992 | 0.659x |
| 16 | set-ttl | 1,279,795 | 1,279,590 | -0.02% | 820,092 | 1.560x |
| 16 | set-ttl-hot | 592,505 | 603,682 | +1.89% | 913,868 | 0.661x |
| 16 | get-control | 1,996,008 | 1,996,008 | +0.00% | 1,184,834 | 1.685x |

| Pipeline | Workload | Before p99 ms | After p99 ms | Redis p99 ms | Before max ms | After max ms | Redis max ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | set-uniform | 1.319 | 1.375 | 1.255 | 11.999 | 9.087 | 3.839 |
| 1 | set-hot | 1.407 | 1.359 | 1.215 | 11.983 | 16.975 | 5.279 |
| 1 | set-ttl | 1.375 | 1.351 | 1.287 | 13.759 | 11.383 | 3.839 |
| 1 | set-ttl-hot | 1.367 | 1.367 | 1.239 | 9.543 | 11.535 | 3.935 |
| 1 | get-control | 1.375 | 1.343 | 1.207 | 10.871 | 9.055 | 1.919 |
| 16 | set-uniform | 5.551 | 5.351 | 2.647 | 22.751 | 17.487 | 4.159 |
| 16 | set-hot | 11.143 | 9.663 | 2.367 | 25.967 | 24.287 | 5.839 |
| 16 | set-ttl | 5.607 | 5.823 | 3.127 | 13.231 | 23.375 | 5.783 |
| 16 | set-ttl-hot | 12.191 | 11.743 | 2.775 | 26.111 | 30.335 | 5.975 |
| 16 | get-control | 2.783 | 2.815 | 2.231 | 8.175 | 17.567 | 6.191 |

Maxima above are medians of the three run maxima. They are separate from p99, not substitutes for it. The CSV format does not expose p99.9; that percentile was not measured.

| Pipeline | Workload | Before req/s range | After req/s range | Redis req/s range |
| ---: | --- | --- | --- | --- |
| 1 | set-uniform | 142,735–148,038 | 142,735–142,776 | 137,874–137,893 |
| 1 | set-hot | 142,755–148,038 | 142,735–142,755 | 137,874–142,816 |
| 1 | set-ttl | 137,741–142,776 | 142,735–142,755 | 133,298–133,298 |
| 1 | set-ttl-hot | 142,735–142,755 | 142,369–148,060 | 137,874–142,816 |
| 1 | get-control | 147,995–148,017 | 142,735–147,995 | 142,796–142,796 |
| 16 | set-uniform | 1,519,180–1,522,070 | 1,595,851–1,597,125 | 969,344–969,462 |
| 16 | set-hot | 666,445–666,556 | 727,140–727,140 | 1,102,992–1,102,992 |
| 16 | set-ttl | 1,279,590–1,279,795 | 1,279,590–1,279,795 | 799,680–820,176 |
| 16 | set-ttl-hot | 592,461–603,682 | 603,682–603,682 | 888,297–913,972 |
| 16 | get-control | 1,995,510–1,996,008 | 1,996,008–1,997,004 | 1,184,659–1,184,834 |

Peridot's unsupported CONFIG GET produces a nonfatal redis-benchmark preflight warning. The harness accepts only that exact warning and fails on other diagnostics, command errors or malformed output. No AOF/RDB files were created and all benchmark-owned server processes were stopped.

Pipeline-16 uniform SET shows 4.91% higher measured throughput and hot SET 9.10%.
Their p99 batch latency decreases from 5.551 to 5.351 ms and 11.143 to 9.663 ms.
Uniform TTL SET throughput is unchanged, while its p99 and maximum latency rise.
Hot TTL SET shows a small 1.89% throughput increase and lower p99, but a higher
maximum. These results do not establish a general latency improvement. Redis has
lower p99 and maximum latency for every measured pipeline-16 workload, and higher
hot-key write throughput. Peridot's higher uniform-key throughput than Redis
already exists in the baseline and is workload-specific.

All pipeline-1 SET medians are unchanged. Pipeline-1 GET has a 3.55% lower median,
with overlapping ranges and a difference near one completion tick. Pipeline-16
GET throughput is unchanged. Small changes, especially the 1.89% hot TTL result,
should be read with the benchmark's timer granularity and three-run ranges.

## Go microbenchmarks

The same update benchmark file runs against the clean baseline archive and the changed code. Three trials alternate version order, using 400 ms per case at GOMAXPROCS 1 and 10. The direct case has no persistence or active-expiration coordination; coordinated and TTL cases include the real active-expiration worker with nil persistence. They do not include the RDB engine's status/append checks or network/protocol costs. All cases use 65,536 prepopulated keys, immutable 128-byte values, and independent per-worker key generators. TTL cases maintain the expiration index; hot cases target one key. ns/op is aggregate elapsed time divided by completed operations.

| Case | Before ns/op | After ns/op | Time change | Before B/op | After B/op | Before / after allocs |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `BenchmarkDatabaseUpdate/coordinated/hot=false` | 168.80 | 161.40 | -4.38% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/coordinated/hot=false-10` | 157.60 | 159.20 | +1.02% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/coordinated/hot=true` | 103.50 | 99.51 | -3.86% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/coordinated/hot=true-10` | 295.70 | 284.70 | -3.72% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/direct/hot=false` | 147.80 | 156.60 | +5.95% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/direct/hot=false-10` | 184.70 | 171.50 | -7.15% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/direct/hot=true` | 105.50 | 99.59 | -5.60% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/direct/hot=true-10` | 280.30 | 285.30 | +1.78% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/ttl/hot=false` | 213.10 | 199.00 | -6.62% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/ttl/hot=false-10` | 255.90 | 254.60 | -0.51% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/ttl/hot=true` | 115.50 | 111.90 | -3.12% | 48 | 48 | 1 / 1 |
| `BenchmarkDatabaseUpdate/ttl/hot=true-10` | 258.50 | 315.10 | +21.90% | 48 | 48 | 1 / 1 |

At ten processors, direct uniform update time falls 7.15%, coordinated uniform
updates are effectively unchanged, and coordinated hot updates improve 3.72%.
The hot TTL microbenchmark regresses 21.90%; the additional TCP hot TTL batch
does not reproduce that throughput regression. The one-processor direct uniform
case also regresses 5.95%. These are different workloads, not evidence that either
measurement is invalid. Removing the database stripes does not eliminate xsync
bucket locks or TTL-index contention, and is not a universal performance win.

## Layout diagnostic

An initial implementation removed the stripes without isolating the gate from
hot database fields. A separate three-trial, 600 ms, GOMAXPROCS=10 diagnostic
changed only the spacing after the shared gate. Median ns/op:

| Case | Original baseline | Unpadded Compute | Gate isolated |
| --- | ---: | ---: | ---: |
| Coordinated uniform Update | 160.5 | 252.0 | 159.1 |
| Uniform database Get | 140.2 | 164.6 | 139.7 |

This layout-only control supports false sharing as the cause of the initial
regression. The final implementation uses CacheLinePad from the already required
official golang.org/x/sys v0.48.0 module; no module version or dependency is added.
Its requirement moves from indirect to direct. The final network and microbenchmark
tables above were rerun with this isolation in place. The unpadded pilot/network
results and layout diagnostic remain in the parent run directory and are excluded
from the final comparison. The initial interface-closure allocation regression
was also corrected before the final measurements.

## Reproduction and evidence

The complete local run bundle is `/private/tmp/peridot-update-compute-k5eNDy/final`. It includes the clean baseline archive, identical benchmark harnesses, both server binaries and compiled benchmark binaries, version/binary hashes, commands, raw CSV and stderr, dataset checks, raw microbenchmark results and process cleanup status.

The network command template is:

```sh
redis-benchmark -h 127.0.0.1 -p PORT -n REQUESTS -c 128 -P PIPELINE -r KEYSPACE --threads 4 --seed SEED --csv SET bench:__rand_int__ VALUE
# KEYSPACE=65536 for uniform workloads; 1 for hot SET.
# TTL SET appends: PXAT 4102444800000. GET omits VALUE.
```

The per-version microbenchmark command is:

```sh
GOCACHE=/private/tmp/peridot-go-cache go test ./internal/database -run '^$' -bench '^BenchmarkDatabaseUpdate$' -benchmem -benchtime=400ms -count=1 -cpu=1,10
```

The run driver uses compiled test binaries with the equivalent testing flags to keep compilation outside measurements. Diagnostic pilot results are retained separately and excluded from all reported medians.

Pinned references: [Xsync v4.5.0 Compute implementation](https://github.com/puzpuzpuz/xsync/blob/v4.5.0/map.go), [Redis 8.10.2 SET semantics](https://github.com/redis/redis/blob/8.10.2/src/t_string.c), [Redis 8.10.2 benchmark timing and command generation](https://github.com/redis/redis/blob/8.10.2/src/redis-benchmark.c).
