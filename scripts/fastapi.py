#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "fastapi",
#   "opentelemetry-exporter-otlp-proto-http",
#   "opentelemetry-instrumentation-fastapi",
#   "opentelemetry-sdk",
#   "sentry-sdk==2.68.1",
#   "uvicorn",
# ]
# ///

import argparse
import logging
import os
import sys
from urllib.parse import quote

# This file shadows the fastapi package unless its directory leaves sys.path.
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir in sys.path:
    sys.path.remove(script_dir)

import sentry_sdk
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration


app = FastAPI(title="Ntry telemetry test server")
logger = logging.getLogger("ntry.fastapi")
meter = otel_metrics.get_meter("ntry.fastapi")
otel_request_counter = meter.create_counter("fastapi.requests")
otel_worker_gauge = meter.create_gauge("fastapi.workers")
otel_duration = meter.create_histogram("fastapi.request.duration", unit="ms")


@app.get("/", response_class=HTMLResponse)
def index():
    return """<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ntry telemetry trigger</title>
<style>
  :root { color-scheme: dark; font: 16px/1.5 ui-monospace, monospace; }
  body { max-width: 760px; margin: 0 auto; padding: 3rem 1rem; background: #10130f; color: #e7eadf; }
  header { border-bottom: 1px solid #4a5143; margin-bottom: 1.5rem; }
  h1 { margin: 0; color: #d8ff72; font-size: clamp(2rem, 7vw, 4rem); letter-spacing: -.08em; }
  header p { color: #a8b09d; }
  main { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: .75rem; }
  button { min-height: 6rem; padding: 1rem; border: 1px solid #4a5143; border-radius: 0; background: #191e17; color: inherit; font: inherit; text-align: left; cursor: pointer; }
  button:hover, button:focus-visible { border-color: #d8ff72; outline: none; background: #22291e; }
  button:disabled { opacity: .55; cursor: wait; }
  button strong, button small { display: block; }
  button strong { color: #d8ff72; margin-bottom: .35rem; }
  button small { color: #a8b09d; }
  pre { min-height: 4rem; margin-top: 1.5rem; padding: 1rem; overflow: auto; border-left: 3px solid #d8ff72; background: #080a07; white-space: pre-wrap; }
</style>
<header>
  <h1>NTRY</h1>
  <p>Trigger local Sentry and OpenTelemetry signals.</p>
</header>
<main>
  <button type="button" data-path="/error"><strong>Unhandled error</strong><small>Sentry error + failed OTLP span</small></button>
  <button type="button" data-path="/handled"><strong>Handled error</strong><small>Sentry error + OTLP span event</small></button>
  <button type="button" data-path="/message"><strong>Warning message</strong><small>Sentry message + OTLP request span</small></button>
  <button type="button" data-path="/log"><strong>Structured log</strong><small>Sentry log + OTLP log</small></button>
  <button type="button" data-path="/metrics"><strong>Metrics</strong><small>Counter, gauge, and duration</small></button>
  <button type="button" data-path="/flush" data-method="POST"><strong>Flush</strong><small>Send all queued telemetry</small></button>
</main>
<pre id="output" aria-live="polite">Ready.</pre>
<script>
  const output = document.querySelector("#output");
  document.querySelectorAll("button[data-path]").forEach((button) => {
    button.addEventListener("click", async () => {
      button.disabled = true;
      output.textContent = `Calling ${button.dataset.path}...`;
      try {
        const response = await fetch(button.dataset.path, {method: button.dataset.method || "GET"});
        output.textContent = `${response.status} ${response.statusText}\n${await response.text()}`;
      } catch (error) {
        output.textContent = error.message;
      } finally {
        button.disabled = false;
      }
    });
  });
</script>
</html>"""


@app.get("/error")
def unhandled_error():
    sentry_sdk.set_tag("feature", "unhandled-error")
    sentry_sdk.set_user({"id": "test-user"})
    sentry_sdk.set_context("ntry_test", {"endpoint": "/error", "safe": True})
    sentry_sdk.add_breadcrumb(category="test", message="about to fail", level="info")
    raise RuntimeError("Ntry FastAPI unhandled error")


@app.get("/handled")
def handled_error():
    try:
        raise ValueError("Ntry FastAPI handled error")
    except ValueError as error:
        trace.get_current_span().record_exception(error)
        event_id = sentry_sdk.capture_exception(error)
    return {"event_id": str(event_id)}


@app.get("/message")
def message():
    return {"event_id": str(sentry_sdk.capture_message("Ntry FastAPI warning", "warning"))}


@app.get("/log")
def log():
    logger.info("Ntry FastAPI structured log", extra={"feature": "sdk-log"})
    return {"logged": True}


@app.get("/metrics")
def metrics(gauge: float = 3.5, duration_ms: float = 125.0):
    attributes = {"endpoint": "/metrics"}
    sentry_sdk.metrics.count("fastapi.requests", 1, attributes=attributes)
    sentry_sdk.metrics.gauge("fastapi.workers", gauge, attributes=attributes)
    sentry_sdk.metrics.distribution(
        "fastapi.request.duration",
        duration_ms,
        unit="millisecond",
        attributes=attributes,
    )
    otel_request_counter.add(1, attributes)
    otel_worker_gauge.set(gauge, attributes)
    otel_duration.record(duration_ms, attributes)
    return {"counter": 1, "gauge": gauge, "duration_ms": duration_ms}


@app.post("/flush")
def flush():
    otel_flushed = [
        provider.force_flush(5000) for provider in app.state.otel_providers
    ]
    return {
        "sentry": sentry_sdk.flush(timeout=5),
        "opentelemetry": all(otel_flushed),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Run a FastAPI server that reports to Ntry through Sentry and OTLP"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    dsn = os.environ.get("SENTRY_DSN")
    if not dsn:
        parser.error("SENTRY_DSN is required")
    otel_headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "")
    if otel_headers.startswith("Authorization: "):
        name, value = otel_headers.split(": ", 1)
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"{name}={quote(value)}"
    elif otel_headers.startswith("Authorization=") and " " in otel_headers:
        name, value = otel_headers.split("=", 1)
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"{name}={quote(value)}"
    resource = Resource.create({"service.name": "ntry-fastapi"})
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
    )
    otel_metrics.set_meter_provider(meter_provider)
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    logger.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))
    app.state.otel_providers = tracer_provider, meter_provider, logger_provider
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)
    sentry_sdk.init(
        dsn=dsn,
        environment="development",
        release="ntry-fastapi@1.0",
        enable_logs=True,
        integrations=[
            FastApiIntegration(),
            LoggingIntegration(event_level=logging.ERROR, sentry_logs_level=logging.INFO),
        ],
    )
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
