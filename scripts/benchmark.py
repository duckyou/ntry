#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["opentelemetry-proto"]
# ///

import argparse
import concurrent.futures
import http.client
import json
import os
import statistics
import threading
import time
import urllib.parse
from collections import Counter

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, ResourceLogs, ScopeLogs


def main():
    parser = argparse.ArgumentParser(description="Benchmark Ntry Sentry and OTLP ingestion")
    parser.add_argument("-n", "--requests", type=int, default=1_000)
    parser.add_argument("-c", "--concurrency", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("requests, concurrency, and timeout must be greater than zero")
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if not sentry_dsn:
        parser.error("SENTRY_DSN is required")
    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not otlp_endpoint:
        parser.error("OTEL_EXPORTER_OTLP_ENDPOINT is required")
    otlp_header = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "")
    if otlp_header.startswith("Authorization: "):
        authorization = otlp_header.split(": ", 1)[1]
    else:
        headers = dict(
            urllib.parse.unquote(part).split("=", 1)
            for part in otlp_header.split(",")
            if "=" in part
        )
        authorization = headers.get("Authorization")
    if not authorization:
        parser.error("OTEL_EXPORTER_OTLP_HEADERS must include Authorization")

    dsn = urllib.parse.urlsplit(sentry_dsn)
    project_id = dsn.path.strip("/")
    if dsn.scheme != "http" or not dsn.hostname or not dsn.username or not project_id:
        parser.error("DSN must look like http://PROJECT_KEY@HOST/PROJECT_ID")
    otlp = urllib.parse.urlsplit(otlp_endpoint)
    if otlp.scheme != "http" or not otlp.hostname or not otlp.path.strip("/"):
        parser.error("OTLP endpoint must look like http://HOST/PROJECT_ID/")

    run_id = time.time_ns()
    sentry_payload = json.dumps(
        {
            "version": 2,
            "items": [
                {
                    "timestamp": run_id / 1_000_000_000,
                    "level": "info",
                    "body": "ntry benchmark",
                    "attributes": {},
                    "trace_id": "0" * 32,
                }
            ],
        },
        separators=(",", ":"),
    ).encode()
    sentry_item_header = json.dumps(
        {
            "type": "log",
            "content_type": "application/vnd.sentry.items.log+json",
            "item_count": 1,
            "length": len(sentry_payload),
        },
        separators=(",", ":"),
    ).encode()

    def sentry_body(sequence):
        envelope_header = json.dumps(
            {"dsn": sentry_dsn, "event_id": f"{run_id + sequence:032x}"},
            separators=(",", ":"),
        ).encode()
        return b"\n".join([envelope_header, sentry_item_header, sentry_payload])

    def otlp_body(sequence):
        return ExportLogsServiceRequest(
            resource_logs=[
                ResourceLogs(
                    scope_logs=[
                        ScopeLogs(
                            log_records=[
                                LogRecord(
                                    time_unix_nano=run_id + sequence,
                                    severity_text="INFO",
                                    body=AnyValue(string_value="ntry benchmark"),
                                )
                            ]
                        )
                    ]
                )
            ]
        ).SerializeToString()

    sentry_warmup = json.dumps(
        {"dsn": sentry_dsn}, separators=(",", ":")
    )
    targets = [
        (
            "Sentry",
            dsn.hostname,
            dsn.port or 80,
            f"/api/{urllib.parse.quote(project_id, safe='')}/envelope/",
            sentry_body,
            sentry_warmup.encode(),
            {"Content-Type": "application/x-sentry-envelope"},
        ),
        (
            "OTLP",
            otlp.hostname,
            otlp.port or 80,
            f"{otlp.path.rstrip('/')}/v1/logs",
            otlp_body,
            b"",
            {
                "Content-Type": "application/x-protobuf",
                "Authorization": authorization,
            },
        ),
    ]

    any_failed = False
    for name, host, port, path, make_body, warmup_body, headers in targets:
        local = threading.local()

        def send(body):
            connection = getattr(local, "connection", None)
            if connection is None:
                connection = http.client.HTTPConnection(
                    host, port, timeout=args.timeout
                )
                local.connection = connection
            started = time.perf_counter()
            try:
                connection.request("POST", path, body, headers)
                response = connection.getresponse()
                response.read()
                result = response.status
            except Exception as error:
                connection.close()
                local.connection = None
                result = type(error).__name__
            return (time.perf_counter() - started) * 1_000, result

        with concurrent.futures.ThreadPoolExecutor(args.concurrency) as workers:
            list(workers.map(send, [warmup_body] * args.concurrency))
            bodies = [make_body(sequence) for sequence in range(args.requests)]
            started = time.perf_counter()
            results = list(workers.map(send, bodies))
            elapsed = time.perf_counter() - started

        statuses = Counter(status for _, status in results)
        latencies = sorted(
            latency
            for latency, status in results
            if isinstance(status, int) and 200 <= status < 300
        )
        failed = args.requests - len(latencies)
        any_failed |= bool(failed)

        print(f"{name}:")
        print(
            f"requests:    {args.requests} "
            f"({len(latencies)} successful, {failed} failed)"
        )
        print(f"concurrency: {args.concurrency}")
        print(f"elapsed:     {elapsed:.3f} s")
        print(f"throughput:  {len(latencies) / elapsed:.1f} requests/s")
        print(
            "responses:   "
            + ", ".join(f"{key}={value}" for key, value in statuses.items())
        )
        if latencies:
            percentile = lambda value: latencies[
                round((len(latencies) - 1) * value)
            ]
            print(
                "latency:     "
                f"min {latencies[0]:.2f} ms, "
                f"mean {statistics.fmean(latencies):.2f} ms, "
                f"p50 {percentile(0.50):.2f} ms, "
                f"p95 {percentile(0.95):.2f} ms, "
                f"p99 {percentile(0.99):.2f} ms, "
                f"max {latencies[-1]:.2f} ms"
            )
        print()
    raise SystemExit(any_failed)


if __name__ == "__main__":
    main()
