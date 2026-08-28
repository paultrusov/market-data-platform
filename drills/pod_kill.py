#!/usr/bin/env python3
"""Pod-kill drill: delete an ingest pod under live load and measure the recovery.

The cheaper cousin of the node-kill drill, and the one that runs in CI: no node is
harmed, so it works on a single-node cluster and finishes in seconds.
"""

from __future__ import annotations

import time

from drill import db_now, kubectl, longest_write_gap, ready_replicas, report, require_healthy, wait_until

TIMEOUT = 120.0


def main() -> None:
    require_healthy()
    victim = kubectl("get", "pods", "-l", "app=ingest", "-o", "jsonpath={.items[0].metadata.name}")
    _, desired = ready_replicas("ingest")
    print(f"deleting pod {victim}")

    mark = db_now()
    started = time.monotonic()
    kubectl("delete", "pod", victim, "--wait=false")

    # Wait for the loss to register before timing the recovery: immediately after the
    # delete the doomed pod is still counted as ready, and "already recovered" is the
    # wrong answer.
    detected = wait_until(lambda: ready_replicas("ingest")[0] < desired, TIMEOUT, interval=0.5)
    recovered = (
        wait_until(lambda: ready_replicas("ingest") == (desired, desired), TIMEOUT, interval=0.5)
        if detected is not None
        else None
    )

    write_gap = longest_write_gap(mark)

    report(
        "pod-kill drill",
        [
            ("pod deleted", victim),
            ("longest write gap", f"{write_gap:.2f}s" if write_gap is not None else "unreadable"),
            ("loss detected after", f"{detected:.1f}s" if detected is not None else "never"),
            ("degraded until full", f"{recovered:.1f}s" if recovered is not None else "n/a"),
            ("total to full strength", f"{detected + recovered:.1f}s" if detected is not None and recovered is not None else "n/a"),
            ("total drill time", f"{time.monotonic() - started:.1f}s"),
        ],
    )


if __name__ == "__main__":
    main()
