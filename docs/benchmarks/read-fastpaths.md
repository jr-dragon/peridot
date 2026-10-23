# Read fast paths and Xsync API optimizations

Measured on 2026-10-23 against baseline commit `eee1b30`.

## Change and validation

- Live `DB.Get` and `DB.Read` use the shared database gate without a key stripe. Expired observations release it and reload under the original mutation coordination.
- Live `Xsync.Read` uses concurrent `Load`; expired observations use the existing `Update`/`Compute` slow path after releasing its shared gate.
- `Snapshot` preallocates the result map and uses `RangeRelaxed` while its owner excludes mutations. Public raw `Range` keeps its previous semantics.
- Direct `Xsync.DeleteKeys` uses `LoadAndDelete`. Durable database deletion retains resolve -> append -> publish ordering.
- `DB.Update` and `Xsync.Update` are unchanged, including their locking.

Validation passed: the full Go suite and race detector with `REDIS_REFERENCE=/opt/homebrew/bin/redis-server`, `go vet ./...`, the server build and `git diff --check`. Added regressions cover reads during unpublished updates, expired-observation replacement, cancellation on lock upgrade, shared ordered reads, close/deletion boundaries, snapshot growth/deletion, and distinct live deletion counts. New exact-wire Redis 8.10.2 tests cover RESP2/RESP3 across direct Xsync, active-expiration database, RDB and AOF modes. Existing bidirectional RDB/AOF interoperability tests also passed.

## redis-benchmark comparison

Apple M2 Pro; macOS arm64; Go 1.27.3; Xsync v4.5.0; Redis and redis-benchmark 8.10.2. Peridot uses GOMAXPROCS=10; Redis uses one I/O thread. All three servers use localhost TCP, RESP2, keepalive, `save ""` and `appendonly no`. Peridot retains its configured RDB engine and active-expiration coordinator. Servers have separate temporary data directories and dynamically allocated ports.

Each server starts with 65,536 known keys and identical 128-byte values. All keys are read and verified both before and after measurement. GET hits live values; TTL reads persistent keys and returns -1; SET overwrites existing keys with the same explicit payload. Custom benchmark commands avoid relying on redis-benchmark's generated SET payload. No RDB/AOF data files were produced.

128 clients, four benchmark threads, pipeline 1 and 16. One million requests per command at pipeline 1 and eight million at pipeline 16. Each server/configuration has a 20,000-request warmup. Three measured trials rotate server order: Before/After/Redis, After/Redis/Before, Redis/Before/After. Results below are medians. Native threaded redis-benchmark completion uses a 250ms timer; differences near one completion tick should not be interpreted as precise small gains. Pipeline latency is the benchmark pipeline/batch latency, not isolated command service time.

| Pipeline | Command | Before req/s | After req/s | Change | Redis req/s | After / Redis |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | GET | 148,038 | 142,755 | -3.57% | 137,874 | 1.035x |
| 1 | TTL | 142,755 | 142,714 | -0.03% | 142,796 | 0.999x |
| 1 | SET | 142,755 | 142,755 | +0.00% | 137,874 | 1.035x |
| 16 | GET | 1,878,816 | 1,879,258 | +0.02% | 1,184,834 | 1.586x |
| 16 | TTL | 1,996,008 | 1,996,506 | +0.02% | 1,279,590 | 1.560x |
| 16 | SET | 1,517,163 | 1,520,912 | +0.25% | 969,462 | 1.569x |

| Pipeline | Command | Before p99 ms | After p99 ms | Redis p99 ms | Before req/s range | After req/s range |
| ---: | --- | ---: | ---: | ---: | --- | --- |
| 1 | GET | 1.287 | 1.279 | 1.255 | 142,776–148,082 | 142,735–148,038 |
| 1 | TTL | 1.287 | 1.255 | 1.239 | 142,714–148,060 | 137,495–148,038 |
| 1 | SET | 1.447 | 1.439 | 1.279 | 142,755–142,776 | 142,735–142,755 |
| 16 | GET | 3.543 | 3.311 | 2.263 | 1,873,098–1,996,008 | 1,876,173–1,997,004 |
| 16 | TTL | 3.079 | 3.159 | 2.215 | 1,879,258–1,997,004 | 1,879,258–1,996,506 |
| 16 | SET | 5.919 | 5.895 | 2.695 | 1,448,750–1,520,334 | 1,519,468–1,521,202 |

Peridot does not implement CONFIG GET; redis-benchmark emits a nonfatal CONFIG preflight warning. The harness accepts exactly that diagnostic and fails on any other error or malformed result. All benchmark-owned server processes were stopped.

These runs do not establish an end-to-end throughput improvement. Pipeline-16
GET and TTL throughput are essentially unchanged; GET p99 decreases from 3.543ms
to 3.311ms, while TTL p99 increases from 3.079ms to 3.159ms. Pipeline-1 GET has a
3.57% lower median, with overlapping before/after ranges and a difference near
one benchmark completion tick. SET is the unchanged-write control, not evidence
of a write-path optimization. Peridot's higher pipeline throughput than Redis
in this workload predates this change and accompanies higher p99 batch latency.

## Go microbenchmarks

The same new benchmark files run against a clean archive of `eee1b30` and the modified tree. Packages run serially (`-p 1`) after the network benchmark finishes, so the two workloads do not overlap. Three trials alternate before/after order. Each case runs for 300ms at GOMAXPROCS 1 and 10. Cases without a numeric suffix use one processor; `-10` uses ten.

Database cases include the real active-expiration worker and shared coordination, with nil persistence and live non-TTL values. Snapshot copies 65,536 physical entries. DeleteKeys cases measure successful deletion plus identical raw refills in every iteration; these are cycle times, not isolated DEL service times. Fixture creation is outside measurement.

| Case | Before ns/op | After ns/op | Time change | Before B/op | After B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkXsyncRead/hot=false` | 96.03 | 65.64 | -31.65% | 0 | 0 |
| `BenchmarkXsyncRead/hot=false-10` | 150.50 | 151.00 | +0.33% | 0 | 0 |
| `BenchmarkXsyncRead/hot=true` | 69.07 | 52.67 | -23.74% | 0 | 0 |
| `BenchmarkXsyncRead/hot=true-10` | 231.90 | 151.60 | -34.63% | 0 | 0 |
| `BenchmarkXsyncSnapshot` | 6,943,490.00 | 2,838,068.00 | -59.13% | 12,587,499 | 6,296,752 |
| `BenchmarkXsyncSnapshot-10` | 6,824,715.00 | 2,835,155.00 | -58.46% | 12,587,512 | 6,296,755 |
| `BenchmarkXsyncDeleteKeys/keys=1` | 97.68 | 94.01 | -3.76% | 48 | 48 |
| `BenchmarkXsyncDeleteKeys/keys=1-10` | 100.30 | 92.26 | -8.02% | 48 | 48 |
| `BenchmarkXsyncDeleteKeys/keys=32` | 1,965.00 | 1,757.00 | -10.59% | 1,536 | 1,536 |
| `BenchmarkXsyncDeleteKeys/keys=32-10` | 1,992.00 | 1,703.00 | -14.51% | 1,536 | 1,536 |
| `BenchmarkXsyncDeleteKeys/keys=1024` | 67,401.00 | 58,774.00 | -12.80% | 49,152 | 49,152 |
| `BenchmarkXsyncDeleteKeys/keys=1024-10` | 70,736.00 | 59,259.00 | -16.23% | 49,152 | 49,152 |
| `BenchmarkDatabaseRead/Get/hot=false` | 75.67 | 65.82 | -13.02% | 0 | 0 |
| `BenchmarkDatabaseRead/Get/hot=false-10` | 149.40 | 139.40 | -6.69% | 0 | 0 |
| `BenchmarkDatabaseRead/Get/hot=true` | 60.44 | 52.81 | -12.62% | 0 | 0 |
| `BenchmarkDatabaseRead/Get/hot=true-10` | 228.50 | 140.60 | -38.47% | 0 | 0 |
| `BenchmarkDatabaseRead/Read/hot=false` | 83.61 | 92.00 | +10.03% | 16 | 16 |
| `BenchmarkDatabaseRead/Read/hot=false-10` | 152.30 | 142.60 | -6.37% | 16 | 16 |
| `BenchmarkDatabaseRead/Read/hot=true` | 72.59 | 67.23 | -7.38% | 16 | 16 |
| `BenchmarkDatabaseRead/Read/hot=true-10` | 232.20 | 134.50 | -42.08% | 16 | 16 |

At ten processors, hot-key database GET/Read cycle time decreases by 38.47% and
42.08%. Snapshot time decreases by 58.46% and its allocation volume approximately
halves. The 32-key deletion/refill cycle decreases by 14.51%. These local gains
do not translate into the same proportional TCP throughput improvement. The
one-processor uniform database Read case regresses by 10.03%; ten-processor
uniform Xsync Read is essentially unchanged. Removing a lock is not a claim of
improvement for every workload.

## Reproduction and evidence

The run bundle contains both server binaries, the clean baseline archive, raw CSV results, benchmark commands, binary SHA-256 hashes, environment metadata, dataset verification logs, process cleanup status, the network harness (`run.py`) and the serial Go benchmark driver (`micro.py`):

`/private/tmp/peridot-read-fastpath-P8VXJC`

The network command template is:

```sh
redis-benchmark -h 127.0.0.1 -p PORT -n REQUESTS -c 128 -P PIPELINE -r 65536 --threads 4 --seed SEED --csv GET bench:__rand_int__
# TTL uses the same key argument. SET adds an explicit 128-byte value.
```

The per-version Go benchmark command is:

```sh
GOCACHE=/private/tmp/peridot-go-cache go test -p 1 ./internal/storage ./internal/database -run '^$' -bench '^(BenchmarkXsync(Read|Snapshot|DeleteKeys)|BenchmarkDatabaseRead)$' -benchmem -benchtime=300ms -count=1 -cpu=1,10
```

Pinned references: [Xsync v4.5.0 traversal and map APIs](https://github.com/puzpuzpuz/xsync/blob/v4.5.0/map.go), [Redis 8.10.2 benchmark timing and command generation](https://github.com/redis/redis/blob/8.10.2/src/redis-benchmark.c).
