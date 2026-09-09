# /// script
# requires-python = ">=3.10"
# dependencies = ["ddtrace==4.14.0", "sentry-sdk==2.68.1"]
# ///

import json
import logging
import os
import subprocess
import uuid

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NTRY = os.environ.get("NTRY_BIN", os.path.join(ROOT, "target", "release", "ntry"))
URL = os.environ.get("NTRY_URL", "http://127.0.0.1:8910")


def ntry(*args):
    return subprocess.run(
        [NTRY, "--url", URL, *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def query(command, *args):
    result = json.loads(ntry("--json", command, *args))
    return result.get("items", result)


def main():
    ntry("--json", "status")
    project_name = f"smoke-{uuid.uuid4().hex[:8]}"
    created = False
    try:
        project = json.loads(ntry("--json", "project", "add", project_name))
        created = True
        if not project["dsn"]:
            raise RuntimeError(
                "Sentry listener disabled; start ntry with "
                "--sentry-bind 127.0.0.1:8911"
            )
        if not project["ddtrace_agent_url"]:
            raise RuntimeError(
                "DDTrace listener disabled; start ntry with "
                "--ddtrace-bind 127.0.0.1:8112"
            )
        os.environ["DD_TRACE_AGENT_URL"] = project["ddtrace_agent_url"]
        os.environ["DD_INSTRUMENTATION_TELEMETRY_ENABLED"] = "false"
        os.environ["DD_REMOTE_CONFIGURATION_ENABLED"] = "false"
        from ddtrace import tracer

        try:
            with tracer.trace("ntry.smoke", service="ntry-smoke", resource="smoke"):
                raise RuntimeError("ntry ddtrace smoke error")
        except RuntimeError:
            pass
        tracer.shutdown()

        logging.basicConfig(level=logging.INFO)
        sentry_sdk.init(
            dsn=project["dsn"],
            enable_logs=True,
            integrations=[
                LoggingIntegration(event_level=None, sentry_logs_level=logging.INFO)
            ],
        )
        try:
            raise ValueError("ntry smoke error")
        except ValueError as error:
            sentry_sdk.capture_exception(error)
        logging.getLogger("ntry.smoke").info("ntry smoke log")
        sentry_sdk.metrics.count("ntry.smoke.count", 2)
        sentry_sdk.metrics.gauge("ntry.smoke.gauge", 3.5)
        sentry_sdk.metrics.distribution(
            "ntry.smoke.distribution", 4.5, unit="millisecond"
        )
        sentry_sdk.flush(timeout=5)

        errors = query("errors", "--project", project_name)
        assert errors
        assert any(error["source"] == "ddtrace" for error in errors)
        traces = query("traces", "--project", project_name)
        assert any(trace["source"] == "ddtrace" for trace in traces)
        assert query("logs", "--project", project_name)
        assert query("issues", "--project", project_name)
        assert json.loads(
            ntry(
                "--json",
                "get",
                errors[0]["id"],
                "--project",
                project_name,
            )
        )["id"] == errors[0]["id"]
        for name, aggregate in [
            ("ntry.smoke.count", "sum"),
            ("ntry.smoke.gauge", "avg"),
            ("ntry.smoke.distribution", "p50"),
        ]:
            assert query(
                "metrics",
                name,
                "--aggregate",
                aggregate,
                "--project",
                project_name,
            )["series"]

        assert ntry("errors", "--project", project_name)
        assert ntry("logs", "--project", project_name)
        assert ntry("issues", "--project", project_name)
        assert ntry("get", errors[0]["id"], "--project", project_name)
        assert ntry(
            "metrics",
            "ntry.smoke.count",
            "--aggregate",
            "sum",
            "--project",
            project_name,
        )
        print("Ntry running-server smoke check passed")
    finally:
        if created:
            ntry("project", "remove", project_name)


if __name__ == "__main__":
    main()
