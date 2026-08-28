#!/usr/bin/env python3
"""Node-kill drill: stop a worker node under live load and measure the recovery.

Two different numbers matter and they are usually confused with each other:

  * how long the platform stopped ingesting data
  * how long it ran degraded, below its configured replica count

The first is what a consumer of the data notices, and it is small because a second
ingest replica on the surviving node keeps writing. The second is what an operator
notices, and it is bounded by how quickly Kubernetes will evict pods from a node it
has decided is gone -- the reason `tolerationSeconds` is set to 10 instead of the
default 300.

The data node is never the target: Postgres' volume is local to it, so killing it
tests nothing except that local storage is local.
"""

from __future__ import annotations

import time

from drill import db_now, kubectl, longest_write_gap, node_for_role, pods_on, ready_replicas, report, require_healthy, sh, wait_until

TIMEOUT = 240.0


def ensure_spread(target: str, attempts: int = 5) -> list[str]:
    """Put the fleet in the state this drill is meant to test: some ingest on the
    target node, and some not.

    The spread constraint is ScheduleAnyway, and the data node already carries
    Postgres and Prometheus, so the scheduler will happily stack both ingest replicas
    on the emptier node. Either stacking is a different experiment -- all replicas on
    the target measures a cold restart, none on it measures nothing at all -- and both
    have been mistaken for the intended result, so the drill insists on the
    arrangement it is describing.

    Deleting one pod from the crowded node converges faster than restarting the
    deployment: the surviving pod makes that node the skewed one, so the replacement
    goes elsewhere.
    """
    for _ in range(attempts):
        here = pods_on(target, "ingest")
        _, desired = ready_replicas("ingest")
        if here and len(here) < desired:
            return here
        crowded = here if here else pods_on(other_worker(target), "ingest")
        if not crowded:
            raise SystemExit("no ingest pods found at all")
        print(f"ingest is {len(here)}/{desired} on {target}; rescheduling {crowded[0]}")
        kubectl("delete", "pod", crowded[0], "--wait=false")
        kubectl("rollout", "status", "deployment/ingest", "--timeout=180s")
    raise SystemExit("could not get a split placement across nodes; refusing to report a drill that measures something else")


def other_worker(target: str) -> str:
    names = sh("kubectl", "get", "nodes", "-l", "!node-role.kubernetes.io/control-plane",
               "-o", "jsonpath={range .items[*]}{.metadata.name} {end}").split()
    return next(n for n in names if n != target)


def main() -> None:
    baseline = require_healthy()
    target = node_for_role("compute")
    container = target  # kind names the node container after the node
    on_target = ensure_spread(target)
    victims = on_target + pods_on(target, "replay")
    ready_before, desired = ready_replicas("ingest")
    print(f"target node   : {target}")
    print(f"pods on it    : {', '.join(victims) or 'none'}")
    print(f"ingest before : {ready_before}/{desired} ready, {len(on_target)} replica(s) on the target")

    mark = db_now()
    print(f"\nstopping {container} ...")
    killed_at = time.monotonic()
    sh("docker", "stop", container)

    # 2. Two separate waits, and the order matters. Asking "is it back to full
    #    strength?" straight after the kill answers "yes" -- not because it recovered
    #    but because the control plane has not noticed the node is gone yet, and the
    #    dead pods still count as ready. So: first wait for the loss to register, then
    #    time the recovery from there.
    detected = wait_until(lambda: ready_replicas("ingest")[0] < desired, TIMEOUT, interval=0.5)
    recovered = (
        wait_until(lambda: ready_replicas("ingest") == (desired, desired), TIMEOUT, interval=0.5)
        if detected is not None
        else None
    )
    elapsed_total = time.monotonic() - killed_at
    # Measured after the fact, over the window the failure happened in.
    write_gap = longest_write_gap(mark)

    print(f"\nrestarting {container} ...")
    sh("docker", "start", container)
    sh("kubectl", "wait", "--for=condition=Ready", f"node/{target}", "--timeout=180s", check=False)
    kubectl("rollout", "status", "deployment/ingest", "--timeout=180s", check=False)

    report(
        "node-kill drill",
        [
            ("node stopped", target),
            ("pods lost", str(len(victims))),
            ("longest write gap", f"{write_gap:.2f}s" if write_gap is not None else "unreadable"),
            ("loss detected after", f"{detected:.1f}s" if detected is not None else "never (pods stayed ready)"),
            ("degraded until full", f"{recovered:.1f}s" if recovered is not None else "n/a"),
            ("total to full strength", f"{detected + recovered:.1f}s" if detected is not None and recovered is not None else "n/a"),
            ("total drill time", f"{elapsed_total:.1f}s"),
            ("measurement interval", "0.5s"),
        ],
    )


if __name__ == "__main__":
    main()
