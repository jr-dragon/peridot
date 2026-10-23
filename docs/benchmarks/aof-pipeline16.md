# AOF pipeline-16 follow-up

The final UMPSCQueue implementation raises pipeline-16 `everysec` SET from
417,014 to 689,655 req/s (65.4% faster). Its share of
the contemporaneous Redis rate rises from 60.1% to
100.4%; the Redis/Peridot ratio falls from 1.67x to
1.00x. AOF-on throughput retains 50.8% of AOF-off
SET throughput, versus 31.4% before this follow-up.

The `always` median changes by -0.7% and still trails Redis substantially;
no improvement is claimed for that policy. Median P99 improves for `everysec`
and `no`, while the worst `no` sample increases from 17.999 to 19.983 ms and
`always` from 107.903 to 114.431 ms. These observations do not establish a
consistent tail regression, but they prevent a claim that all maxima improved.
AOF-off and GET control differences are retained without attributing them to
this queue change.

## Results

Throughput values are medians of three runs, in requests per second.

### SET, pipeline 16

| Policy | Peridot before | Redis before | Peridot after | Redis after | Change | Before / Redis | After / Redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| off | 1,329,787 | 1,028,807 | 1,356,852 | 1,011,122 | +2.0% | 129.3% | 134.2% |
| everysec | 417,014 | 694,444 | 689,655 | 686,813 | +65.4% | 60.1% | 100.4% |
| no | 420,345 | 661,376 | 682,594 | 686,813 | +62.4% | 63.6% | 99.4% |
| always | 9,629 | 121,212 | 9,565 | 139,860 | -0.7% | 7.9% | 6.8% |

### SET latency, pipeline 16

Milliseconds. P99 is the median reported P99; maximum is the single largest
observation across all three rounds, not the median of run maxima.

| Policy | P99 before | P99 after | Worst maximum before | Worst maximum after |
| --- | ---: | ---: | ---: | ---: |
| off | 2.087 | 2.063 | 5.471 | 6.847 |
| everysec | 3.287 | 1.983 | 14.583 | 11.695 |
| no | 3.199 | 2.071 | 17.999 | 19.983 |
| always | 99.071 | 99.711 | 107.903 | 114.431 |

### Controls

Pipeline 1 is freshly compared for AOF off and `everysec`; pipeline-16 GET controls
use the longer ordinary runs. Small differences are retained without treating
them as reliable improvements.

| Policy | Pipeline | Operation | Peridot before | Peridot after | Change |
| --- | ---: | --- | ---: | ---: | ---: |
| off | 1 | SET | 99,980 | 100,040 | +0.1% |
| everysec | 1 | SET | 100,060 | 100,371 | +0.3% |
| off | 1 | GET | 99,354 | 99,731 | +0.4% |
| everysec | 1 | GET | 99,552 | 99,049 | -0.5% |
| off | 16 | GET | 1,338,688 | 1,355,014 | +1.2% |
| everysec | 16 | GET | 1,297,017 | 1,355,014 | +4.5% |
| no | 16 | GET | 1,356,852 | 1,381,216 | +1.8% |

### Evidence and reproduction

[Raw results and metadata](aof-pipeline16-20261023/) include the baseline,
intermediate handoff, UMPSC pilot, pipeline-1 baseline controls, and final stage.
Metadata records binary hashes, versions, hardware, configuration and timing.
The initial pilot and intermediate stages are not pooled into the final medians.
Benchmark stdout, stderr, commands and effective configuration are retained in
per-stage evidence JSON files.

```sh
python3 scripts/benchmark_aof.py --output /tmp/peridot-pipeline16-before --candidate /tmp/peridot-before --label peridot-before --pipelines 16
python3 scripts/benchmark_aof.py --output /tmp/peridot-pipeline1-before --candidate /tmp/peridot-before --label peridot-before --pipelines 1 --policies off everysec
python3 scripts/benchmark_aof.py --output /tmp/peridot-pipeline16-after --candidate /tmp/peridot-after --label peridot-after --pipelines 1 16
```

## Scope and method

This is a follow-up to [the initial AOF batching change](aof-batching.md). The
baseline is that change's completed binary, not the original globally ordered
implementation. Its SHA-256 matches the first report's optimized binary. Redis
is **8.10.2** and Go is **1.27.3**; hardware and client configuration match the
first report: Apple M2 Pro, 50 connections, 10,000 random keys, seed 42, fixed
32-byte values, one RESP2 client thread, fresh processes and data directories,
three rounds with alternating server order, and sequential server workloads.
Peridot uses the default `GOMAXPROCS`; Redis uses `io-threads 1`. This compares
complete servers with their defaults and does not establish equal CPU cost.

Ordinary runs use 1,000,000 measured requests after 100,000 warmup requests;
`always` uses 20,000 after 2,000. Every dataset is checked before and after timing,
and manifests and nonempty increments are checked after shutdown. Full tests,
race checks, builds and profiles run outside timing windows. Redis is remeasured
at each stage. Ratios use each stage's contemporaneous Redis median. Results
describe this workload and host; three medians are not confidence intervals.

## Implementation and queue choice

The first batching version woke all completed callers through a condition
variable; they then competed for the same AOF mutex. Moving batch completion to
immutable per-batch results and handing I/O to one waiting leader improved the
pipeline-16 `everysec` SET median from 417,014 to 593,120 req/s. That intermediate
stage's raw measurements are retained separately. The handoff prototype also
reached 12,531 req/s under `always`, higher than the final queue's 9,565 req/s.
The final implementation prioritizes the default `everysec` and `no` gains while
keeping one append ordering implementation; improving `always` remains open.

The final version uses the already pinned **xsync v4.5.0 UMPSCQueue** with one
consumer. Its [source](https://github.com/puzpuzpuz/xsync/blob/v4.5.0/umpscqueue.go)
supports concurrent producers and a single consumer, matching connection callers
and one AOF writer. The [SPSCQueue contract](https://github.com/puzpuzpuz/xsync/blob/v4.5.0/spscqueue.go)
requires a single producer, so it cannot directly receive all connection writes.
No dependency versions or generator inputs changed.

Producers enqueue resolved immutable mutations, leaving encoding and Write to
the consumer. Each caller waits on a pooled request's completion, then publishes
its memory mutation while still holding the backend bucket lock. The writer
never enters the backend. This preserves same-key ordering without a bucket-lock
cycle. A caller receives its own completed result even if a later batch fails.

The consumer drains existing reservations into batches of up to 256 requests or
approximately 256 KiB. A single larger record remains intact. There is no batching
timer, and no success before Write; `always` also waits for Sync. Each synchronous
in-flight persistence call contributes at most one request. Close rejects new
submissions, queues a sentinel after all accepted producers, and joins the writer
before closing the descriptor. Rotation remains protected by the database gate.

This adds one consumer goroutine and queue memory. On this platform the initial
two 4,096-entry segments contain approximately 192 KiB of pointer and WaitGroup
slots, before request, pool, buffer and goroutine overhead; later segment reuse
can retain more memory. Requests clear key/value references before completion so
old queue slots cannot pin large payloads. Buffers over 64 KiB are discarded.
This is a known resource tradeoff, not a claim of zero memory overhead.

## Diagnostic profile

Separate five-second CPU profiles show cumulative `Mutex.lockSlow` samples falling
from 18.86% to 2.59%, consistent with removing the append completion convoy and
encoding under the producer mutex. Raw syscalls dominate the final profile.
These are saturated, fixed-duration profiles with different amounts of work;
total CPU samples increase from 22.27 to 28.18 seconds, so they do not establish
equal total CPU consumption or a measured CPU cost per request. The text profiles
are retained with the raw results; full CPU, allocation, heap, and goroutine
captures remain under `/private/tmp/peridot-pipeline16-20261023/`.

## Side-effect audit and validation

The first-stage implementation passed an added race-enabled growth/deletion
stress test before this follow-up. The final queue version passed full tests,
full race detection, vet, and build, with Redis 8.10.2 available for interoperability:

```sh
REDIS_REFERENCE=/opt/homebrew/bin/redis-server PERIDOT_REDIS_SERVER=/opt/homebrew/bin/redis-server go test ./...
REDIS_REFERENCE=/opt/homebrew/bin/redis-server PERIDOT_REDIS_SERVER=/opt/homebrew/bin/redis-server go test -race ./...
go vet ./...
go build -o /tmp/peridot-pipeline16-20261023/peridot-after ./cmd/server
```

Focused coverage includes exact binary-safe replay and absolute TTLs; concurrent
same-key increments and independent-key updates; map growth and shrink; values
over 64 KiB; multi-key DEL; concurrent rewrite, close and restart; memory
publication only after Write/Sync; short writes and failed syncs; completed-batch
results across a later failure; and draining 16 or 272 queued calls during close
while rejecting late submissions. Both Redis-produced input and Redis-readable
output remain covered by the full suite. No correctness, protocol, or lifecycle
regression was found in these checks; this is not proof against every workload
or storage failure.

## Remaining limits

`always` still waits for each command's durable publication on each connection
before processing the next pipelined command. The queue groups concurrent calls;
it does not change that execution boundary, and Redis retains a large pipeline-16
advantage under this policy. Same-key and same-bucket contention remain. These
throughput measurements primarily overwrite a fixed keyspace; insertion-heavy,
hot-key, large-value throughput, and memory at production scale were not measured.
The first report's isolated 63.487 ms `no` maximum remains a historical
observation; these runs do not establish its cause or prove such spikes cannot
recur. The `always` GET samples are short and should not be treated as a precise
read performance comparison. Existing persistence compatibility limits remain listed
in [the design](../design/persistence.md#compatibility-limits).
