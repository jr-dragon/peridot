# Agent Instructions

## Project scope

- Peridot is a Go service targeting protocol and input/output compatibility with **Redis 8.10.2**.
- To external callers, Peridot must behave like a standalone Redis server. Redis Sentinel and Redis Cluster compatibility are out of scope.
- Compatibility covers observable command semantics, RESP framing and reply types, error codes and messages, binary-safe arguments, pipelining, and connection behavior. Support RESP2 and RESP3 as required by the implemented features.
- Internal data structures, memory layout, storage engines, and persistence mechanisms may differ from Redis. Do not reproduce Redis internals unless needed for observable compatibility.
- Support reading Redis-compatible `redis.conf` configuration and reading and writing Redis-compatible AOF and RDB files. These are interoperability requirements; Peridot may use a different internal configuration model or persistence format, with import/export adapters at the boundary.
- Treat these requirements as the project target, not a claim that every feature is already implemented. Report unsupported features and compatibility gaps explicitly.
- Use Redis 8.10.2 as the reference when resolving compatibility questions. Check the matching official documentation, source, or observable behavior rather than silently adopting another version's behavior.

## Language and tools

- Write code, comments, documentation, commit messages, and branch names in English unless explicitly instructed otherwise. Preserve proper nouns, identifiers, and commands in their original form.

## Go and dependencies

- Use **Go 1.27.3**, matching `go.mod`. Do not change the required Go version without explicit instruction.
- Project-owned code must not use CGO: do not add `import "C"`, C/C++ bindings, or CGO-dependent implementations to this repository's code.
- External libraries may use CGO internally. Do not impose a blanket `CGO_ENABLED=0` requirement that would prohibit otherwise approved dependencies.
- Prefer the Go standard library and official Go packages. Packages under `golang.org/x/` count as official libraries and do not require third-party approval.
- Obtain the user's **explicit approval before introducing or starting to use any non-official library**. This applies to runtime dependencies, test libraries, development tools, code generators, and non-official libraries introduced transitively.
- Before requesting approval, explain the library's purpose, why existing code or official packages are insufficient, and relevant transitive dependencies or CGO requirements. Do not install, import, vendor, or generate code that depends on an unapproved library while awaiting approval.
- Existing dependencies are not blanket approval for additional libraries. Do not replace or add dependencies merely for convenience.
- Current dependencies and tools include Kessoku for dependency injection, `golang.org/x/sync` for concurrency utilities, and moq for generated mocks. Check `go.mod` and `go.sum` for the current versions.

## Repository layout

- `cmd/server/`: executable entry point, application lifecycle, signal handling, pprof HTTP server, and dependency injection wiring.
- `internal/server/`: TCP connection lifecycle, request dispatch, middleware, and error classification.
- `internal/service/cmd/`: Redis command implementations and client-visible command errors. Register commands through `NewCommands`.
- `internal/liberr/`: structured error logging helpers.
- `pkg/resp/`: RESP value types, encoding/decoding, command parsing, and protocol errors. Keep application command behavior out of this package.
- Tests live beside the implementation in `*_test.go` files.
- `justfile`: existing run, build, generation, and test recipes. Build output goes to `bin/`.
- `README.md`: change only when explicitly requested by the user.
- Check for more specific `AGENTS.md` instructions before editing a subdirectory.

## Implementation conventions

- Follow existing Go style and keep changes focused on the requested behavior. Format handwritten Go code with `gofmt`.
- Keep wire parsing, transport lifecycle, and command execution separate using the existing package boundaries.
- Command instances are shared across connections and must support concurrent execution. `Exec` receives the command name at `args[0]`; arguments are borrowed until it returns, so copy the argument slice before retaining it.
- Preserve ASCII case-insensitive command lookup and binary-safe payload handling. Avoid Unicode normalization of command bytes.
- Preserve error identity with wrapping and use `errors.Is`/`errors.As`. Client-visible command errors must match `cmd.ErrCommand`; internal error details must not be exposed in replies.
- Keep recoverable command errors, fatal protocol errors, clean EOF between commands, and internal failures distinct. Preserve their corresponding reply and connection behavior.
- Keep graceful shutdown and immediate close distinct. Cancellation must not prematurely interrupt connections that are being drained; the application enforces its shutdown timeout and closes remaining connections when needed.
- Use `log/slog`, typed attributes such as `slog.String`, and `internal/liberr.Attr` for errors, following existing logging patterns. Do not log raw command payloads or treat normal EOF and intentional shutdown as errors.
- Edit generator inputs rather than files marked `Code generated ... DO NOT EDIT`. Current generated files include `cmd/server/kessoku_band.go` and `internal/server/handler_mock.go`.
- For dependency injection or Kessoku generation changes, read `.codex/skills/kessoku-di/SKILL.md` and regenerate the affected output.

## Development and validation

Run commands from the repository root:

| Task | Command |
| --- | --- |
| Run the server | `just run` or `go run ./cmd/server` |
| Build the server | `just build` or `go build -o ./bin/peridot-server ./cmd/server` |
| Regenerate code | `just generate` or `go generate ./...` |
| Run tests | `just test` or `go test -v ./...` |
| Static checks | `go vet ./...` |
| Check concurrency changes | `go test -race ./...` |

- Add or update behavior tests for code changes. Use the standard `testing` package and existing helpers unless a third-party testing library has explicit approval.
- For command and protocol changes, assert exact wire replies where deterministic, including CRLF framing, reply type, error text, binary data, malformed input, and pipelined follow-up requests as relevant.
- When implementing or changing compatibility behavior, compare equivalent inputs against Redis 8.10.2 with matching configuration where practical. Record the reference version and any deviations; do not claim differential verification if it was not performed.
- For configuration and persistence interoperability, test Redis-produced input and Redis-readable output as applicable. A Peridot-only round trip does not establish Redis format compatibility.
- Test connection lifecycle and concurrency changes for draining, forced closure, cancellation, and error propagation as relevant. Use deadlines and deterministic synchronization rather than arbitrary sleeps where possible.
- Run relevant package tests during development, then `go test ./...`, `go vet ./...`, and the build for Go changes. Run the race detector for concurrency changes and regenerate code when generator inputs change.
- For documentation-only changes, check accuracy against the repository and run `git diff --check`; Go tests are unnecessary unless the documented workflow changes require verification.
- Report the checks performed and any environment limitations or unverified behavior. Do not weaken assertions to hide a failure.

## Branches and commits

- When asked to create a branch, follow Conventional Branch with `<purpose>/<description>`, using purpose prefixes such as `feat/`, `fix/`, `bugfix/`, `hotfix/`, `release/`, or `chore/`.
- Use concise lowercase English descriptions with letters, numbers, and hyphens; dots are allowed for release versions. Avoid spaces, underscores, and consecutive, leading, or trailing hyphens or dots.
- Never use `feature/` or agent prefixes such as `ai/`, `codex/`, `claude/`, `copilot/`, or `cursor/`.
- When asked to commit, follow Conventional Commits 1.0.0: `<type>(optional scope): <description>`. Use `feat` for features, `fix` for fixes, and appropriate types such as `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `perf`, or `style` for other changes. Never use `feature`.
- Mark breaking changes with `!` or a `BREAKING CHANGE:` footer as required.
- Include the exact line `AI Generated: <model name>` in the commit footer, separated from preceding content by a blank line. Replace the placeholder with the actual model name used for the work. Preserve `AI Generated:` exactly as a project-specific exception to footer-token whitespace rules.
