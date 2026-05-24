# bmcpoll - Bounded Concurrent BMC Poller

A small Go CLI that polls a configurable list of HTTP endpoints with
bounded concurrency, per-call timeout, retry-with-backoff, and graceful
shutdown.

Each target is hit with a plain `GET <url>`; any 2xx is treated as success.

## Build & run

```bash
go build -o poller ./cmd/poller

# Targets file: one URL per line. '#' lines are skipped.
# Optional 2-column form "<id>  <url>" lets you label targets.
cat > targets.txt <<'EOF'
bmc-a   https://httpbin.org/status/200
bmc-b   https://httpbin.org/delay/0
bmc-c   https://httpbin.org/delay/5
bmc-d   https://httpbin.org/status/500
EOF

./poller \
  --targets=targets.txt \
  --concurrency=50 \
  --timeout=3s \
  --max-retries=3 \
  --backoff-base=200ms \
  --backoff-max=5s
```

Sample output (stderr):

```
2026/05/25 12:30:01 polling 4 targets with concurrency=50 timeout=3s max-retries=3
completed in 8.2s - 2 success, 1 timeout, 1 http_error
```

Exit code is `0` if every target succeeded, `1` otherwise.

Add `--json` to also dump a structured per-target report to stdout.

`./poller --targets=-` reads URLs from stdin.

`Ctrl-C` / `SIGTERM` triggers graceful shutdown: the dispatcher stops feeding
new targets, in-flight calls drain (bounded by `--timeout`), the partial
summary prints, and the process exits.

## Flags

| Flag              | Default | Description                                          |
| ----------------- | ------- | ---------------------------------------------------- |
| `--targets`       | -       | path to URL file (one per line), or `-` for stdin    |
| `--concurrency`   | `50`    | max in-flight requests                               |
| `--timeout`       | `3s`    | per-call timeout                                     |
| `--max-retries`   | `3`     | retries per target on retryable failure              |
| `--backoff-base`  | `200ms` | initial retry backoff                                |
| `--backoff-max`   | `5s`    | max retry backoff                                    |
| `--total-timeout` | `5m`    | overall run timeout (0 disables)                     |
| `--json`          | `false` | also emit a per-target JSON report on stdout         |

Retry classification: `2xx` → success; `4xx` → non-retryable; `5xx` and
network errors → retry up to `--max-retries`. Backoff is exponential with
jitter and is interrupted by context cancellation.

## Tests

```bash
go test -race -v ./internal/poller/...
```

- `TestRun_BoundedConcurrency` - 500 targets, `Concurrency=10`; the mock
  BMC tracks max in-flight via atomic CAS and the test asserts it never
  exceeds the cap.
- `TestRun_HungEndpointDoesNotBlock` - mix of `/fast` and `/hang` targets,
  100 ms per-call timeout; the run must finish in under 1 s with hung
  targets classified as `timeout`.
- `TestRun_GracefulShutdownNoLeak` - kicks off 100 slow targets, cancels
  the parent ctx mid-run, asserts `Run` returns within 2 s and goroutine
  count returns to baseline.
- `TestConfigValidate` - config sanity checks.

## Design notes

- **Bounded concurrency** via a `chan struct{}` semaphore of capacity
  `Concurrency`. The dispatcher acquires a slot before spawning a
  goroutine per target; the worker releases it on exit.
- **Cancellation is purely ctx-driven.** `http.Client.Timeout` is left
  unset; every request is scoped by `context.WithTimeout(parent,
  PerCallTimeout)`. A hung server, a SIGTERM, and the total-run timeout
  all flow through the same cancellation path.
- **Per-attempt `defer cancel()`** lives inside the retry loop, not at
  function scope, so cancel funcs don't accumulate across retries.
- **Results channel is buffered to `len(targets)`** so workers never
  block on send. This avoids the deadlock where the aggregator waits on
  the WaitGroup while workers wait on an unbuffered channel.
- **Backoff sleep is ctx-aware**: `select { case <-time.After(d): case
  <-ctx.Done(): }` - never blocks past cancellation. Jitter is
  `rand.Int63n(base)` to break up synchronized retry storms.
- **Errors** are typed values in `errors/error_codes.go` with `Create*`
  factories that return a `*PollError` wrapping the underlying cause and
  implementing `Unwrap`.

## Out of scope

- Real Redfish parsing - `GET` and 2xx-is-success only.
- Metrics export.
- Per-host circuit breaker.
- Persistence / resumable runs - runs are in-memory.
- Distributed work partitioning across collector replicas.
- Structured logging.
- IPMI fallback.

## Layout

```
bmcpoll/
├── cmd/poller/main.go         # CLI: flag parsing, signal handling, summary print
├── errors/                    # typed errors + Create* factories
│   ├── error_codes.go
│   ├── error.go
│   └── error_utility.go
└── internal/
    ├── poller/
    │   ├── poller.go          # Poller struct, Run(ctx, targets) Summary
    │   ├── retry.go           # doWithRetry, backoffFor, httpAttempt
    │   ├── result.go          # Target, Result, Summary, status constants
    │   └── poller_test.go     # bounded / hung / shutdown / validate tests
    └── mock/server.go         # httptest fake BMC for tests
```
