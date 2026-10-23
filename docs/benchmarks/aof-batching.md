# AOF batching benchmark

This records the first optimization stage. See the [pipeline-16 follow-up](aof-pipeline16.md)
for the subsequent queue implementation, side-effect audit, and fresh measurements.

The default `everysec` policy improves pipeline-16 SET throughput from 142,694
to 430,293 req/s (3.02x), increasing its share of the contemporaneous Redis rate
from 20.8% to 58.2%. The Redis/Peridot ratio falls from 4.81x to 1.72x.
`always` improves from 375 to 10,793 req/s (28.75x), although Redis remains about
13.05x faster with pipeline 16. Pipeline-1 `always` reaches 8,576 req/s versus
Redis's 9,737 req/s.

With `everysec`, Peridot retains 31.5% of its AOF-off pipeline-16 SET throughput,
compared with 10.8% before. AOF-off SET changes by +3.0%; GET controls change
by +0.1% to +1.4%. These small control differences are not claimed as improvements.

Median P99 improves from 17.391 to 3.127 ms for `everysec`, and from 2277.375 to
92.735 ms for `always`. The worst `no` observation increases from 26.767 to
63.487 ms despite its improved median P99. The source of this isolated spike
was not identified; it is retained in the raw results and is an unresolved tail
latency limitation. No claim is made that all maximum latencies improved.

## Scope and method

This report compares the working-tree change with Peridot revision
`0ed0e176ead0cee82349f17a641fba76dd8803dd` and Redis **8.10.2**. The baseline
binary was built before the Go changes. Both binaries use Go **1.27.3**.

Measurements ran on an Apple M2 Pro with 10 CPUs and 32 GiB RAM, macOS 26.6.2,
using the local data volume and TCP loopback. Peridot uses the default
`GOMAXPROCS`; Redis reports `io-threads 1`. This compares complete servers with
their defaults, and does not establish equal CPU consumption.

Each stage contains three rounds, alternating server order. Only one server and
one client workload run at a time. Every server/policy/round starts with a fresh
process and directory. Both servers use `save ""`; Redis automatic AOF rewrite
is disabled. Policies are AOF off and AOF on with `everysec`, `no`, or `always`.

The client is Redis 8.10.2 `redis-benchmark`, using RESP2, one client thread,
50 connections, 10,000 random keys, seed 42, and a fixed 32-byte value. SET and
GET run separately with pipeline depths 1 and 16. Each ordinary measurement has
1,000,000 requests after 100,000 warmup requests. `always` uses 20,000 measured
requests after 2,000 warmup requests because the original implementation fsyncs
each mutation. The exact deterministic dataset is populated and all 10,000 keys
are checked before and after timing. AOF manifests and nonempty increments are
checked after shutdown. The workload primarily measures overwrites, not map growth.

Unit tests, interoperability tests, race detection and profiling run outside the
reported timing windows. One original baseline `always`/P1/SET sample overlapped
brief focused validation; it was discarded and replaced by an isolated rerun.
[The exclusion record](aof-batching-20261023/excluded.json) contains both samples
and the replacement's provenance. Incomplete intermediate builds are excluded.

Redis was measured again after optimization. Its throughput also changed between
stages, so the tables include the reference from each stage and the corresponding
Peridot/Redis ratios. Three-run medians describe this machine and workload;
they are not confidence intervals or general production guarantees.

## Findings and implementation

AOF enabled global database mutation ordering, bypassing the existing concurrent
Compute path. Each mutation also built intermediate RESP values and issued its
own Write; `always` added a Sync per mutation. A separate diagnostic snapshot
found 48 connection goroutines in `DB.lockAccess`. These costs explain the large
loss relative to AOF-off SET throughput.

The implementation now uses the backend's atomic Compute callback for AOF as
well as memory and RDB operations. Same-key mutations remain ordered, and append
completes before the replacement is published. Different keys commute. The shared
database gate spans the whole callback; snapshots, multi-key DEL, expiration
samples and close still take the exclusive gate. Unknown persistence engines
retain the conservative global ordering fallback.

Appenders copy RESP bytes into reusable buffers. One caller writes while others
queue the next batch, with no batching timer. Each caller waits until its own
record is written; `always` additionally waits for its batch's successful Sync.
Short writes and sync failures poison subsequent appends and wake waiting callers.
Rotation and close wait for the active writer before replacing its descriptor;
background `everysec` fsync can still overlap appends. Completed buffers larger
than 64 KiB are discarded.

Engine status checks release the engine gate before waiting for the AOF gate.
This removes a second lock convoy observed during diagnostic profiling. The
AOF/RDB error precedence and configured synchronization policies are retained.

Redis 8.10.2 also groups AOF writes before returning to its event loop; see the
[pinned AOF source](https://github.com/redis/redis/blob/8.10.2/src/aof.c#L1268-L1285).
Peridot retains its existing append-before-memory-publication failure behavior.

## Results

Throughput is requests per second. Each value is a median of three measured runs.

### SET, pipeline 1

| Policy | Peridot before | Redis before | Peridot after | Redis after | Peridot speedup | Before / Redis | After / Redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| off | 99,206 | 109,409 | 99,820 | 123,609 | 1.01x | 90.7% | 80.8% |
| everysec | 75,324 | 109,757 | 100,492 | 119,646 | 1.33x | 68.6% | 84.0% |
| no | 75,290 | 110,387 | 101,647 | 119,389 | 1.35x | 68.2% | 85.1% |
| always | 380 | 8,696 | 8,576 | 9,737 | 22.58x | 4.4% | 88.1% |

### SET, pipeline 16

| Policy | Peridot before | Redis before | Peridot after | Redis after | Peridot speedup | Before / Redis | After / Redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| off | 1,324,503 | 1,027,749 | 1,364,256 | 1,129,944 | 1.03x | 128.9% | 120.7% |
| everysec | 142,694 | 686,342 | 430,293 | 739,645 | 3.02x | 20.8% | 58.2% |
| no | 143,906 | 683,060 | 431,779 | 739,645 | 3.00x | 21.1% | 58.4% |
| always | 375 | 138,889 | 10,793 | 140,845 | 28.75x | 0.3% | 7.7% |

### SET latency, pipeline 16

All figures are milliseconds. P99 is the median reported P99 across three runs; maximum is the largest observation across those runs. These are different statistics.

| Policy | P99 before | P99 after | Maximum before | Maximum after |
| --- | ---: | ---: | ---: | ---: |
| off | 1.895 | 1.911 | 5.287 | 4.351 |
| everysec | 17.391 | 3.127 | 39.967 | 13.895 |
| no | 17.103 | 3.111 | 26.767 | 63.487 |
| always | 2277.375 | 92.735 | 2359.295 | 117.951 |

### Retained throughput with AOF

SET pipeline-16 throughput divided by the same binary's AOF-off throughput:

| Policy | Before | After |
| --- | ---: | ---: |
| everysec | 10.8% | 31.5% |
| no | 10.9% | 31.6% |
| always | 0.028% | 0.791% |

### GET control

| Policy | Pipeline | Peridot before | Peridot after | Change |
| --- | ---: | ---: | ---: | ---: |
| off | 1 | 98,941 | 99,295 | +0.4% |
| off | 16 | 1,366,120 | 1,371,742 | +0.4% |
| everysec | 1 | 99,226 | 99,731 | +0.5% |
| everysec | 16 | 1,355,014 | 1,360,544 | +0.4% |
| no | 1 | 99,512 | 99,592 | +0.1% |
| no | 16 | 1,362,398 | 1,381,216 | +1.4% |

## Validation

The final Go implementation passed:

```sh
REDIS_REFERENCE=/opt/homebrew/bin/redis-server PERIDOT_REDIS_SERVER=/opt/homebrew/bin/redis-server go test ./...
REDIS_REFERENCE=/opt/homebrew/bin/redis-server PERIDOT_REDIS_SERVER=/opt/homebrew/bin/redis-server go test -race ./...
go vet ./...
go build -o /tmp/peridot-aof-20261023/peridot-after ./cmd/server
git diff --check
```

Focused regressions cover batching 16 waiting records into one Write and, for
`always`, one Sync; exact binary-safe replay and absolute TTLs; acknowledgment
and memory publication after Sync; short-write and sync-failure propagation to
all waiting callers; live reads during pending sync; and descriptor lifetime
across Write, rewrite and close. Concurrent same-key updates, independent-key
updates, TTL changes, multi-key deletion, rewrite and restart are checked together.

Redis 8.10.2 reads Peridot's concurrent append output, including a rewrite,
binary keys/values and TTLs, under all three sync policies. The full suite also
checks existing Redis-to-Peridot and Peridot-to-Redis RDB and multipart AOF paths,
RESP2/RESP3 errors, expiration and pipeline reuse. No dependency or generator
inputs changed.

## Remaining limits

- `always` with pipeline 16 still trails Redis substantially. Each Peridot
  connection waits for the current command's durable publication before running
  its next command. Redis can accumulate more pipelined commands before its
  event-loop AOF flush. This change groups concurrent connections, while retaining
  the current execution and failure boundaries.
- Same-key and same-bucket operations remain serialized. Single-client, hot-key,
  insertion-heavy and large-value workloads were not benchmarked here.
- The `always` GET samples contain only 20,000 requests and can be very short or
  noisy. Their raw data is retained; the GET control table uses the longer runs.
- This change retains the existing compatibility limits documented in
  [the persistence design](../design/persistence.md#compatibility-limits),
  including fail-closed AOF errors and missing automatic rewrite thresholds.
  Peridot's unsupported `CONFIG GET` causes a benchmark metadata warning; the
  timed SET/GET operations and complete dataset checks succeed.

## Reproduce and inspect

Build the baseline revision in a separate checkout and the current source with
Go 1.27.3, then run from the repository root with fresh output directories:

```sh
python3 scripts/benchmark_aof.py --candidate /path/to/peridot-before --label peridot-before --output /tmp/aof-before
python3 scripts/benchmark_aof.py --candidate /path/to/peridot-after --label peridot-after --output /tmp/aof-after
```

The runner checks the reference version, records commands/configuration,
verifies data, and emits per-run results and medians. Redis and benchmark paths
can be supplied with `--redis-server` and `--benchmark`.

Repository review artifacts in [aof-batching-20261023/](aof-batching-20261023/):

- `before.csv` and `after.csv`: all 96 measured results per stage, with the
  contaminated baseline sample replaced.
- `summary.json`: medians and worst observed maximum latencies.
- `before-metadata.json` and `after-metadata.json`: hardware, versions,
  configuration parameters and executable SHA-256 hashes.
- `before-profile.txt` and `after-profile.txt`: separate diagnostic CPU profiles.
- `excluded.json`: the excluded sample and isolated replacement provenance.

Full configurations, client output, server logs, profiles, datasets, test logs
and binaries are retained locally at `/private/tmp/peridot-aof-20261023/`.
