"""Shared helpers for the failure drills.

Measurements are taken against Postgres directly rather than through the replay API.
The API is served by pods that a drill may itself be killing, and a few seconds of
"the API was unreachable" would otherwise be recorded as ingest downtime.
"""

from __future__ import annotations

import subprocess
import time
from datetime import datetime

NS = "mdp"
CLUSTER = "mdp"


def sh(*args: str, check: bool = True) -> str:
    result = subprocess.run(args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def kubectl(*args: str, check: bool = True) -> str:
    return sh("kubectl", "-n", NS, *args, check=check)


def newest_tick() -> datetime | None:
    """When the most recent row was written, by Postgres' clock.

    Server-side insert time, not the exchange timestamp: both the baseline and the
    post-failure reading come from the same clock, so the measurement has no skew and
    a trade that was already in flight cannot be mistaken for a recovery.
    """
    out = sh(
        "kubectl", "-n", NS, "exec", "postgres-0", "--", "psql", "-U", "mdp", "-d", "market",
        "-tAc", "SELECT COALESCE(MAX(inserted_at)::text, '') FROM ticks",
        check=False,
    )
    line = out.splitlines()[-1].strip() if out else ""
    try:
        return datetime.fromisoformat(line) if line else None
    except ValueError:
        return None


def psql(query: str) -> str:
    out = sh(
        "kubectl", "-n", NS, "exec", "postgres-0", "--", "psql", "-U", "mdp", "-d", "market",
        "-tAc", query, check=False,
    )
    return out.splitlines()[-1].strip() if out else ""


def db_now() -> datetime:
    """Postgres' clock. Every drill timestamp comes from here so nothing has to skew."""
    return datetime.fromisoformat(psql("SELECT now()::text"))


def longest_write_gap(since: datetime) -> float | None:
    """The longest interval with no rows written, from `since` to now.

    This is the honest answer to "how long was ingest down". Watching for the first
    write after the failure is not: a batch that was already in flight lands
    immediately afterwards and the outage looks like nothing happened. Measuring the
    gap after the fact cannot be fooled that way.
    """
    value = psql(
        "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM gap)), 0) FROM ("
        "  SELECT inserted_at - LAG(inserted_at) OVER (ORDER BY inserted_at) AS gap"
        f"  FROM ticks WHERE inserted_at > '{since.isoformat()}'"
        ") g"
    )
    try:
        return float(value)
    except ValueError:
        return None


def ready_replicas(deployment: str) -> tuple[int, int]:
    ready = kubectl("get", "deploy", deployment, "-o", "jsonpath={.status.readyReplicas}", check=False) or "0"
    desired = kubectl("get", "deploy", deployment, "-o", "jsonpath={.spec.replicas}", check=False) or "0"
    return int(ready or 0), int(desired or 0)


def node_for_role(role: str) -> str:
    return sh("kubectl", "get", "nodes", "-l", f"mdp.io/role={role}", "-o", "jsonpath={.items[0].metadata.name}")


def pods_on(node: str, app: str) -> list[str]:
    out = kubectl(
        "get", "pods", "-l", f"app={app}", "--field-selector", f"spec.nodeName={node}",
        "-o", "jsonpath={range .items[*]}{.metadata.name} {end}",
    )
    return out.split()


def wait_until(predicate, timeout: float, interval: float = 0.5) -> float | None:
    """Seconds until predicate() is true, or None on timeout."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if predicate():
            return time.monotonic() - started
        time.sleep(interval)
    return None


def require_healthy() -> datetime:
    ready, desired = ready_replicas("ingest")
    if ready != desired or desired == 0:
        raise SystemExit(f"ingest is {ready}/{desired} ready; bring the platform up before drilling")
    newest = newest_tick()
    if newest is None:
        raise SystemExit("no trades in storage yet; wait for ingest to warm up")
    return newest


def report(title: str, rows: list[tuple[str, str]]) -> None:
    width = max(len(k) for k, _ in rows)
    print(f"\n{title}")
    print("-" * (width + 24))
    for key, value in rows:
        print(f"  {key.ljust(width)}   {value}")
    print()
