# Active expiration

The application enables database-owned active expiration after recovery. `hz`
sets the baseline cycle frequency (default 10); `dynamic-hz yes` enables adaptive
scheduling (default yes). Redis boolean configuration uses `yes`/`no`, not the
literal word `enabled`. As in Redis 8.10.2, `hz` accepts nonnegative signed-32-bit
integers and clamps the baseline to 1 through 500. CLI directives override files.

Embedded callers enable the worker with `database.WithActiveExpiration`; a bare
`database.New` retains passive-only behavior. `DB.Close` signals and joins the
worker before final snapshots or persistence closure. The application closes the
database after draining request handlers.

## Sampling and deletion

The database maintains a dense slice and key-to-position map containing only
keys with expiration timestamps. Recovery seeds the index once. Mutations,
passive expiration, and active expiration update it under the database gate;
removal swaps the last element into the removed slot. Index maintenance is O(1),
with O(N) additional memory for N TTL keys. No per-cycle full-keyspace scan or
snapshot is needed. Both Xsync and DLHT backends use the same coordinator.
With active expiration enabled, memory-only mutations also use the database's
exclusive gate so index and value publication remain atomic.

Each cycle samples up to 20 distinct keys uniformly using a partial Fisher-Yates
shuffle. It rechecks current expiration under the gate, attempts any required AOF
DEL before removing the key, and increments the snapshot dirty counter. A known
AOF persistence failure does not block expiration: the key and index entry are
removed while user writes remain protected. Persisted expiration timestamps
still prevent recovery from making expired keys visible. Other persistence
errors leave the key intact and end the cycle; the worker logs the failure and
can retry on later ticks. Keys expire strictly after their
Unix millisecond deadline, matching the existing passive path.

A deletion ratio strictly greater than 25% triggers another sample in the same
cycle. Exactly 25%, a lower ratio, no TTL keys, shutdown, a busy database gate,
or an exhausted 25ms budget stops the cycle. The worker uses `TryLock` rather
than waiting behind writers/checkpoints, and releases the gate between samples.
The monotonic time budget includes sampling and deletion and is checked before
each key and before each durable deletion. A started synchronous append/fsync,
runtime scheduling pause, or backend operation cannot be preempted: 25ms is a
cooperative work budget, not a hard real-time bound on an OS operation. No new
work starts after budget exhaustion; maintaining durable publication order
takes precedence over abandoning an in-flight append.

## Adaptive frequency

After each complete sample, let N be the remaining number of TTL keys and D the
absolute distance in milliseconds between the sample's median expiration
timestamp and the current time. The even-sized median averages the two middle
timestamps. Samples include both live and expired keys, before deletion.

```text
population = N / (N + 1000)
proximity  = 1000 / (1000 + D)
frequency  = hz * (0.5 + 1.5 * population * proximity)
frequency  = clamp(frequency, hz / 2, hz * 2)
```

The population term grows smoothly with the number of TTL keys, while proximity
decreases smoothly as the median moves away from now. The scales make 1,000 keys
and one second the respective half-saturation points. At `hz=10`, 1,000 keys
with a median one second away produce 8.75 cycles/second; the same population
with a median at now produces 12.5. Empty databases use half the baseline, and
large populations near expiration approach twice the baseline. Absolute distance
also lowers frequency for a far-overdue median, as requested; a high expired
ratio still causes repeated sampling within each cycle.

The first cycle uses the baseline; each later timer uses the last complete
sample's result. Fixed mode always uses the baseline. Frequency is floating point
so `hz=1` can reach 0.5 cycles/second without rounding outside the bounds. Timers
subtract elapsed cycle time from the next interval. Slow cycles do not overlap
or queue catch-up runs, and yield for at least 1ms. Consequently actual throughput
may fall below the target frequency under contention or slow I/O.

This adaptive algorithm follows Peridot's requested TTL population/median policy,
not Redis's client-count-based dynamic-hz algorithm. Configuration semantics are
checked by the opt-in Redis 8.10.2 differential tests; the scheduling policy is
covered by deterministic `testing/synctest` tests.

Reference: [Redis 8.10.2 configuration source](https://github.com/redis/redis/blob/8.10.2/src/config.c).
