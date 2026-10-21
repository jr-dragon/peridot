# Startup configuration

Peridot accepts Redis configuration syntax and applies only the settings its
current implementation uses. The parser lives in `internal/data/config.go`;
`data.LoadConfig(os.Args[1:], os.Stdin)` supplies `data.Config` to `NewApp`.

```sh
go run ./cmd/server ./redis.conf --port 6380 --pprof-addr localhost:6061
go run ./cmd/server --bind 127.0.0.1 -::1 --port 6380
go run ./cmd/server --appendonly yes --appendfsync everysec --dir /existing/data --appendfilename peridot.aof
```

| Directive | Default | Behavior |
| --- | --- | --- |
| `bind` | `* -::*` | One or more TCP addresses; optional addresses have a leading `-`. |
| `port` | `6379` | TCP port, from 0 through 65535. |
| `hz` | `10` | Active expiration cycles per second; nonnegative int32 values are clamped to 1 through 500. Each cycle has a 25ms cooperative work budget. |
| `dynamic-hz` | `yes` | Adapt expiration frequency to TTL key count and sampled median deadline distance, between half and twice `hz`; `no` uses fixed `hz`. |
| `appendonly` | `no` | Enable multipart AOF persistence. |
| `appendfsync` | `everysec` | `always` syncs each mutation; `everysec` syncs in a background goroutine every second; `no` leaves normal flushing to the OS. |
| `appendfilename` | `appendonly.aof` | A filename without a directory component. |
| `appenddirname` | `appendonlydir` | Directory beneath `dir` containing the AOF manifest and segments. |
| `aof-use-rdb-preamble` | `yes` | Write RDB bases; `no` writes plain command bases. Both formats can be read. |
| `dbfilename` | `dump.rdb` | Standalone RDB snapshot filename. |
| `save` | `3600 1 300 100 60 10000` | Snapshot after elapsed seconds and changed-key counts; `save ""` disables automatic/shutdown snapshots. |
| `stop-writes-on-bgsave-error` | `yes` | Block mutations and PING after a background snapshot failure when save rules are enabled; a failed foreground SAVE leaves this status unchanged. |
| `dir` | Process working directory | Base directory for persistence and subsequent relative paths. |
| `include` | None | Read another file, or matching files in sorted order. |

Configuration is applied in order: optional file, optional stdin, then CLI
directives. Repeated scalar settings use the last value. The first explicit `save`
replaces defaults; later `save` rules accumulate, and `save ""` clears them. Pass `-` first or last to read
stdin. CLI arguments use Redis's `--directive value` syntax, including multi-value
`--bind` and a whole directive in one argument, such as `"--port 6380"`.
The old `-addr` and `-aof` flags are replaced by the directives above.

Files support blank lines, whole-line `#` comments, single and double quotes,
and Redis byte escapes. Inline `#` text is an argument, not a comment. Include
paths resolve against the working directory at that point in parsing, not the
including file's directory. A `dir` directive changes this logical directory
without changing the Go process's working directory. Include globs without
matches are ignored; missing literal paths fail. Include nesting is limited to
128 levels to terminate cycles. Parse errors identify the source and line.

`data.Config.Extra` stores Peridot-only settings. Currently it contains
`PprofAddress`, defaulting to `localhost:6060`. Set it with `--pprof-addr`.
The `pprof-addr` directive is rejected in files, included files, and stdin.

## Compatibility boundary

See [active expiration](expiration.md) for sampling, the dynamic frequency
formula, and the non-preemptible I/O limitation of its 25ms work budget. The
adaptive policy is Peridot-specific; it does not reproduce Redis's client-based
dynamic frequency adjustment.

Unsupported directives are ignored, including module loading, authentication,
protected mode, replication, automatic AOF rewrite thresholds, and RDB output
compression/checksum tuning.
Successfully loading a Redis configuration does not enable those features.
The AOF manifest is `dir/appenddirname/appendfilename.manifest`. The legacy path
`dir/appendfilename` is imported only when no manifest exists. `appendonly no`
disables ongoing AOF logging and recovery. Manual `BGREWRITEAOF` still exports a
base snapshot and manifest without enabling logging or creating an increment;
`appendfsync no` keeps AOF writes enabled without per-write or periodic fsync.
The default `everysec` policy acknowledges writes before their background sync.
Shutdown waits for background sync and performs a final sync before closing.
Creation and rewrite still sync their files and directory entries in every policy.
Reading configuration alone does not open persistence. Application startup loads
AOF when enabled, otherwise `dir/dbfilename`, even with `save ""`. Active save rules
enable periodic snapshots and a final snapshot of changed data on shutdown. See
[persistence design](persistence.md) for recovery precedence and compatibility limits.

Multiple TCP listeners share one RESP server, handler, storage instance, and
client ID sequence. An optional bind skips an unavailable local address but does
not hide an occupied port. Unsupported address families are skipped. All opened
listeners close if another required bind fails. Existing graceful drain and
forced close behavior covers every listener.

`port 0` and an empty `bind` disable TCP. Since Unix sockets and TLS listeners are
not implemented, application startup rejects these configurations before opening
persistence, rather than allocating an ephemeral TCP port.

## Reference and verification

The syntax follows the [Redis configuration documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/config/)
and Redis 8.10.2's `src/config.c`, `src/server.c`, and `src/sds.c`.
The optional differential test requires Redis 8.10.2 and compares effective
configuration through `CONFIG GET` over a temporary Unix socket:

```sh
PERIDOT_REDIS_SERVER="$(command -v redis-server)" go test ./internal/data -run TestRedisConfigCompatibility -v
```

The official-sample test reads unmodified Redis `redis.conf` files from 8.0, 8.2,
8.4, 8.6, 8.8, 8.10, and 8.10.2, plus `redis-full.conf` from 8.0 through 8.8.
Place each release's samples under `<root>/<version>/`, then run:

```sh
PERIDOT_REDIS_CONFIG_DIR=/path/to/samples go test ./internal/data -run TestOfficialRedisConfigs -v
```

Download samples from `https://raw.githubusercontent.com/redis/redis/<version>/<filename>`.
Redis 8.10 consolidates the samples into `redis.conf`. Both integration tests
are opt-in and perform no network downloads; regular parser tests require no
Redis installation or external fixtures.
