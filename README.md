> [!CAUTION]
> Ntry is under active development and mostly consists of AI slop code. 
> Expect breaking changes and occasional proof that generating code is easier than maintaining it. 👍

# Ntry

Ntry is a local Sentry-compatible, Python ddtrace-compatible, and OTLP/HTTP
sink for errors, logs, traces, and metrics. A single daemon owns Fjall hot
storage, Zstandard Parquet archives, and the query API used by the web, CLI,
and MCP front ends.

## Start

```sh
curl -fsSL https://raw.githubusercontent.com/duckyou/ntry/main/install.sh | bash
ntry serve --sentry-bind 127.0.0.1:8911 --ddtrace-bind 127.0.0.1:8112 --otlp-bind 127.0.0.1:8918
ntry project add demo
```

Open `http://127.0.0.1:8910` for the web interface.

Use the printed DSN with `sentry-sdk==2.68.1`:

```python
import sentry_sdk

sentry_sdk.init(dsn="http://PROJECT_KEY@127.0.0.1:8911/PROJECT_ID", enable_logs=True)
```

The listeners use these conventional ports:

| Listener | Configuration | Default |
|---|---|---|
| Web UI, Ntry API, MCP | `--bind` / `NTRY_BIND` | `127.0.0.1:8910` |
| Sentry | `--sentry-bind` / `NTRY_SENTRY_BIND` | disabled (use `127.0.0.1:8911`) |
| Python ddtrace | `--ddtrace-bind` / `NTRY_DDTRACE_BIND` | disabled (use `127.0.0.1:8112`) |
| OTLP/HTTP | `--otlp-bind` / `NTRY_OTLP_BIND` | disabled (use `127.0.0.1:8918`) |

Ingestion listeners are separately opt-in through their bind options. A
non-loopback bind requires `--allow-remote`; v1 has no TLS.
Project management is limited to loopback clients unless `--allow-remote` is
set. Browser MCP requests must have an Origin that matches the Host header.
Use `ntry serve --verbose` to print structured server logs. `RUST_LOG` can set
a custom tracing filter.

Ntry stores data in `~/.config/ntry` by default. Override it with `--data-dir`
or `NTRY_DATA_DIR`.

## Python ddtrace

The ddtrace listener accepts MessagePack v0.4 and v0.5 trace payloads. Run
`ntry project add` or `ntry project list` to get the authenticated agent URL,
then configure Python before importing `ddtrace`:

```sh
export DD_TRACE_AGENT_URL=http://127.0.0.1:8112/PROJECT_ID/PROJECT_KEY/
```

The project key is part of the URL because native Datadog trace requests do
not include an application authentication header. Error spans also create
Ntry Error records and issues.

## OTLP/HTTP

The OTLP listener accepts binary protobuf requests, optionally gzip-compressed,
at exactly these routes:

```text
POST /{project_id}/v1/traces
POST /{project_id}/v1/logs
POST /{project_id}/v1/metrics
Content-Type: application/x-protobuf
Content-Encoding: gzip (optional)
Authorization: Bearer PROJECT_KEY
```

`ntry project add` and `ntry project list` print the project-specific endpoint
and authorization header when OTLP is enabled. Configure a standard OpenTelemetry
exporter with the trailing slash on the endpoint so it appends `v1/traces`,
`v1/logs`, or `v1/metrics` correctly:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:8918/PROJECT_ID/
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer PROJECT_KEY"
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

## Query

```sh
ntry project list             # IDs, names, and DSNs
ntry issues 'level:error' --project demo --since 24h
ntry errors 'error.type:ValueError' --project demo --json
ntry logs 'message:*timeout' --project demo
ntry traces 'trace_id:TRACE_ID' --project demo
ntry metrics requests --aggregate per_second --group-by route --project demo
ntry get RECORD_ID --project demo --json
ntry status
ntry mcp             # MCP over stdio
```

List commands return newest-first cursor pages with `--json`. Pass
`--cursor VALUE` to continue. Times accept relative `s`, `m`, `h`, `d`, or `w`
durations and RFC 3339 UTC timestamps.

## Storage

Safe durability is the default and syncs accepted batches before HTTP success.
`ntry serve --durability fast` can lose admitted data if the process or machine
crashes; normal shutdown still flushes it.

Records stay in Fjall for 24 hours by default, then move to date-partitioned
Zstandard Parquet. Configure this with `NTRY_HOT_WINDOW` and
`NTRY_ARCHIVE_INTERVAL`. The logical storage target defaults to 10 GiB and can
be changed with `NTRY_STORAGE_TARGET_BYTES`. Ntry warns at 80 percent, then
evicts oldest archive partitions, hot records, and finally issue aggregates.

Unsupported envelope item types are discarded and counted. Supported JSON
items are limited to 1 MiB; decompressed envelopes are limited to 20 MiB. A
full ingest queue returns `429` with `Retry-After: 1`.

The storage schema has no backward compatibility or migration support. After a
schema change, stop Ntry and reset `NTRY_DATA_DIR` (or `~/.config/ntry`) before
restarting.

Complex OTLP histograms, exponential histograms, and summaries are stored
losslessly and can be searched and inspected, but `ntry metrics` cannot
aggregate them yet.

## MCP

The daemon serves authenticated Streamable HTTP at `http://127.0.0.1:8910/mcp`.
`ntry mcp` provides the same `search`, `get`, `query_metrics`, and `status`
tools over stdio. MCP has no write or project-management tools.

## Compatibility Check

Build the release binary, then run the daemon with Sentry and ddtrace ingestion
enabled. The smoke script defaults to `NTRY_URL=http://127.0.0.1:8910` and
discovers both advertised endpoints. It does not require an OTLP Python
package.

```sh
cargo build --release
target/release/ntry serve --sentry-bind 127.0.0.1:8911 --ddtrace-bind 127.0.0.1:8112
```

In another terminal, using the same `NTRY_URL` and `NTRY_DATA_DIR`:

```sh
uv run scripts/smoke.py
npm --prefix web ci && npm --prefix web run build
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```
