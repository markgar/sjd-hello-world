#!/usr/bin/env python3
"""Fabric Spark / Livy helper — run, monitor, and fetch logs for Spark Job Definitions.

Usage:
    # Run a Spark Job Definition and wait for it to finish
    python fabric_livy.py run

    # Check the latest (or specific) run status
    python fabric_livy.py status
    python fabric_livy.py status --run-id <uuid>

    # Fetch Livy session details for the latest run
    python fabric_livy.py livy

    # Tail driver stdout/stderr for a Spark application
    python fabric_livy.py logs
    python fabric_livy.py logs --app-id <application_id>

    # List recent runs
    python fabric_livy.py runs

Environment variables (required):
    WS_ID   — Fabric workspace ID
    SJD_ID  — Spark Job Definition item ID
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_RESOURCE = "https://analysis.windows.net/powerbi/api"


def _env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: environment variable {name} is not set", file=sys.stderr)
        sys.exit(1)
    return val


# ---------------------------------------------------------------------------
# Auth — delegates to `az account get-access-token`
# ---------------------------------------------------------------------------

_token_cache: dict[str, str] = {}


def get_token(resource: str = FABRIC_RESOURCE) -> str:
    if resource not in _token_cache:
        result = subprocess.run(  # noqa: S603
            ["az", "account", "get-access-token", "--resource", resource, "--query", "accessToken", "-o", "tsv"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
        )
        _token_cache[resource] = result.stdout.strip()
    return _token_cache[resource]


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Low-level HTTP helpers (stdlib only — no requests dependency)
# ---------------------------------------------------------------------------


def _api(method: str, path: str, *, body: dict | None = None, expect: set[int] | None = None) -> dict | None:
    """Call the Fabric REST API. Returns parsed JSON (or None for 202/204)."""
    url = f"{FABRIC_API}{path}"
    data = json.dumps(body).encode() if body else None
    headers = _headers()
    if data is None and method in ("POST", "PUT", "PATCH"):
        headers["Content-Length"] = "0"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)  # noqa: S310
    expect = expect or {200, 201, 202, 204}

    try:
        with urllib.request.urlopen(req) as resp:  # noqa: S310
            code = resp.status
            raw = resp.read()
    except urllib.error.HTTPError as e:
        code = e.code
        raw = e.read()
        if code not in expect:
            print(f"HTTP {code}: {raw.decode(errors='replace')}", file=sys.stderr)
            sys.exit(1)

    if not raw:
        return None
    return json.loads(raw)


# ---------------------------------------------------------------------------
# API wrappers
# ---------------------------------------------------------------------------


def _ws() -> str:
    return _env("WS_ID")


def _sjd() -> str:
    return _env("SJD_ID")


def submit_run() -> str:
    """Trigger a Spark Job run. Returns the job instance ID from the Location header."""
    url = f"{FABRIC_API}/workspaces/{_ws()}/items/{_sjd()}/jobs/instances?jobType=sparkjob"
    headers = _headers()
    headers["Content-Length"] = "0"
    req = urllib.request.Request(url, headers=headers, method="POST")  # noqa: S310
    try:
        with urllib.request.urlopen(req) as resp:  # noqa: S310
            location = resp.headers.get("Location", "")
            # Location header contains the run instance URL — extract the ID
            run_id = location.rstrip("/").rsplit("/", 1)[-1] if location else ""
            return run_id
    except urllib.error.HTTPError as e:
        if e.code == 202:  # noqa: PLR2004
            location = e.headers.get("Location", "")
            run_id = location.rstrip("/").rsplit("/", 1)[-1] if location else ""
            return run_id
        print(f"HTTP {e.code}: {e.read().decode(errors='replace')}", file=sys.stderr)
        sys.exit(1)


def list_runs(limit: int = 5) -> list[dict]:
    data = _api("GET", f"/workspaces/{_ws()}/items/{_sjd()}/jobs/instances?limit={limit}")
    return data.get("value", []) if data else []


def get_run(run_id: str) -> dict:
    data = _api("GET", f"/workspaces/{_ws()}/items/{_sjd()}/jobs/instances/{run_id}")
    return data or {}


def get_latest_run() -> dict:
    runs = list_runs(limit=1)
    if not runs:
        print("No runs found.", file=sys.stderr)
        sys.exit(1)
    return runs[0]


def list_livy_sessions() -> list[dict]:
    data = _api("GET", f"/workspaces/{_ws()}/sparkJobDefinitions/{_sjd()}/livySessions")
    return data.get("value", []) if data else []


def get_livy_for_run(run_id: str) -> dict | None:
    """Find the Livy session that matches a job instance ID."""
    for session in list_livy_sessions():
        if session.get("jobInstanceId") == run_id:
            return session
    return None


def get_spark_app_log(app_id: str, log_type: str = "stdout") -> str | None:
    """Fetch driver stdout or stderr for a Spark application.

    Tries multiple known Fabric monitoring API patterns.
    """
    paths = [
        # Pattern 1: workspace-scoped monitoring API
        f"/workspaces/{_ws()}/spark/applications/{app_id}/attempts/1/driverlog/{log_type}",
        # Pattern 2: without attempt
        f"/workspaces/{_ws()}/spark/applications/{app_id}/driverlog/{log_type}",
    ]
    for path in paths:
        url = f"{FABRIC_API}{path}"
        headers = _headers()
        req = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310
        try:
            with urllib.request.urlopen(req) as resp:  # noqa: S310
                return resp.read().decode(errors="replace")
        except urllib.error.HTTPError:
            continue
    return None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_STATUS_ICONS = {
    "NotStarted": "⏳",
    "InProgress": "🔄",
    "Completed": "✅",
    "Failed": "❌",
    "Cancelled": "🚫",
    "Deduped": "♻️",
}


def _fmt_status(status: str) -> str:
    return f"{_STATUS_ICONS.get(status, '❓')} {status}"


def _print_run(run: dict, *, verbose: bool = False) -> None:
    print(f"  Run ID:   {run.get('id', '?')}")
    print(f"  Status:   {_fmt_status(run.get('status', '?'))}")
    print(f"  Started:  {run.get('startTimeUtc', '-')}")
    print(f"  Ended:    {run.get('endTimeUtc', '-')}")
    failure = run.get("failureReason")
    if failure:
        print(f"  Error:    {failure.get('errorCode', '?')}")
        print(f"  Message:  {failure.get('message', '?')}")
    if verbose:
        print(f"  Raw:      {json.dumps(run, indent=2)}")


def _print_livy(session: dict) -> None:
    print(f"  Livy ID:     {session.get('livyId', '?')}")
    print(f"  Spark App:   {session.get('sparkApplicationId', '?')}")
    print(f"  State:       {session.get('state', '?')}")
    print(f"  Submitted:   {session.get('submittedDateTime', '-')}")
    print(f"  Started:     {session.get('startDateTime', '-')}")
    print(f"  Ended:       {session.get('endDateTime', '-')}")
    q = session.get("queuedDuration", {})
    r = session.get("runningDuration", {})
    t = session.get("totalDuration", {})
    if q:
        print(f"  Queued:      {q.get('value', '?')} {q.get('timeUnit', 's')}")
    if r:
        print(f"  Running:     {r.get('value', '?')} {r.get('timeUnit', 's')}")
    if t:
        print(f"  Total:       {t.get('value', '?')} {t.get('timeUnit', 's')}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_run(args: argparse.Namespace) -> None:
    """Submit a run and optionally wait for completion."""
    print(f"Submitting Spark job for SJD {_sjd()}...")
    run_id = submit_run()
    if run_id:
        print(f"Run submitted — ID: {run_id}")
    else:
        print("Run submitted (no run ID in response). Fetching latest...")
        time.sleep(2)
        run = get_latest_run()
        run_id = run["id"]
        print(f"Latest run ID: {run_id}")

    if not args.wait:
        return

    print("Waiting for completion...", end="", flush=True)
    terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
    while True:
        time.sleep(args.interval)
        run = get_run(run_id) if run_id else get_latest_run()
        status = run.get("status", "?")
        print(f"\r  Status: {_fmt_status(status):40s}", end="", flush=True)
        if status in terminal_states:
            print()
            break

    _print_run(run)

    # Auto-fetch Livy session details on failure
    if run.get("status") == "Failed":
        _show_failure_details(run.get("id", run_id))


def _show_failure_details(run_id: str) -> None:
    """After a failure, show Livy + log details automatically."""
    print("\n--- Livy session ---")
    session = get_livy_for_run(run_id)
    if session:
        _print_livy(session)
        app_id = session.get("sparkApplicationId")
        if app_id:
            print(f"\n--- Driver stdout ({app_id}) ---")
            stdout = get_spark_app_log(app_id, "stdout")
            if stdout:
                # Show last 60 lines
                lines = stdout.strip().splitlines()
                if len(lines) > 60:  # noqa: PLR2004
                    print(f"  ... ({len(lines) - 60} lines truncated)")
                for line in lines[-60:]:
                    print(f"  {line}")
            else:
                print("  (not available via API)")

            print(f"\n--- Driver stderr ({app_id}) ---")
            stderr = get_spark_app_log(app_id, "stderr")
            if stderr:
                lines = stderr.strip().splitlines()
                if len(lines) > 60:  # noqa: PLR2004
                    print(f"  ... ({len(lines) - 60} lines truncated)")
                for line in lines[-60:]:
                    print(f"  {line}")
            else:
                print("  (not available via API)")
    else:
        print("  No Livy session found for this run.")


def cmd_status(args: argparse.Namespace) -> None:
    """Show status of a specific or the latest run."""
    if args.run_id:
        run = get_run(args.run_id)
    else:
        run = get_latest_run()
    _print_run(run, verbose=args.verbose)


def cmd_runs(args: argparse.Namespace) -> None:
    """List recent runs."""
    runs = list_runs(limit=args.limit)
    if not runs:
        print("No runs found.")
        return
    for i, run in enumerate(runs):
        print(f"\n[{i + 1}]")
        _print_run(run)


def cmd_livy(args: argparse.Namespace) -> None:
    """Show Livy session for the latest (or specified) run."""
    if args.run_id:
        run_id = args.run_id
    else:
        run = get_latest_run()
        run_id = run["id"]
        print(f"Latest run: {run_id}  ({_fmt_status(run.get('status', '?'))})")

    session = get_livy_for_run(run_id)
    if session:
        _print_livy(session)
    else:
        print("No Livy session found for this run.")
        print("Available Livy sessions:")
        for s in list_livy_sessions()[:5]:
            print(f"  {s.get('livyId')}  job={s.get('jobInstanceId')}  state={s.get('state')}")


def cmd_logs(args: argparse.Namespace) -> None:
    """Fetch driver logs for a Spark application."""
    app_id = args.app_id
    if not app_id:
        # Discover from latest run → Livy session
        run = get_latest_run()
        session = get_livy_for_run(run["id"])
        if not session:
            print("No Livy session found for latest run.", file=sys.stderr)
            sys.exit(1)
        app_id = session.get("sparkApplicationId")
        if not app_id:
            print("No Spark application ID in Livy session.", file=sys.stderr)
            sys.exit(1)
        print(f"Spark app: {app_id}  (from run {run['id']})")

    for log_type in ("stdout", "stderr"):
        print(f"\n--- {log_type} ---")
        content = get_spark_app_log(app_id, log_type)
        if content:
            lines = content.strip().splitlines()
            tail = args.tail
            if tail and len(lines) > tail:
                print(f"  ... ({len(lines) - tail} lines skipped)")
                lines = lines[-tail:]
            for line in lines:
                print(f"  {line}")
        else:
            print("  (not available via API — logs may have expired or the app died before writing output)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fabric Spark / Livy helper — run, monitor, and fetch logs for Spark Job Definitions.",
        epilog="Requires WS_ID and SJD_ID environment variables.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Submit a Spark job run")
    p_run.add_argument("--no-wait", dest="wait", action="store_false", default=True, help="Don't wait for completion")
    p_run.add_argument("--interval", type=int, default=15, help="Poll interval in seconds (default: 15)")
    p_run.set_defaults(func=cmd_run)

    # status
    p_status = sub.add_parser("status", help="Show run status")
    p_status.add_argument("--run-id", help="Specific run ID (default: latest)")
    p_status.add_argument("-v", "--verbose", action="store_true", help="Show raw JSON")
    p_status.set_defaults(func=cmd_status)

    # runs
    p_runs = sub.add_parser("runs", help="List recent runs")
    p_runs.add_argument("-n", "--limit", type=int, default=5, help="Number of runs to show")
    p_runs.set_defaults(func=cmd_runs)

    # livy
    p_livy = sub.add_parser("livy", help="Show Livy session details")
    p_livy.add_argument("--run-id", help="Specific run ID (default: latest)")
    p_livy.set_defaults(func=cmd_livy)

    # logs
    p_logs = sub.add_parser("logs", help="Fetch driver logs")
    p_logs.add_argument("--app-id", help="Spark application ID (default: auto-detect from latest run)")
    p_logs.add_argument("--tail", type=int, default=100, help="Show last N lines (default: 100)")
    p_logs.set_defaults(func=cmd_logs)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
