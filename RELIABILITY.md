# Reliability

Every number here was measured on the cluster this repo builds, with the live
Coinbase feed running. Nothing is estimated.

**Environment.** kind v0.33 / Kubernetes v1.37, three nodes (one control plane, two
workers) on Docker Desktop, Apple Silicon. Ingest at two replicas against the public
Coinbase feed, three symbols. Drill scripts are in `drills/`.

## Steady state

| | |
|---|---|
| writes to storage | 27.7 rows/sec |
| ingest lag behind the exchange clock | < 0.1s |
| duplicate writes collapsed by the primary key | 2,692 |

That last row is the design working. Both ingest replicas consume the same feed and
write the same trades; the `(symbol, seq)` primary key discards the second copy. It
is the reason losing a replica costs nothing.

## Pod-kill drill

`make drill-pod` — delete one of two ingest pods under live load.

| run | longest write gap | back to 2/2 ready |
|---|---|---|
| 1 | 0.94s | 4.1s |
| 2 | 0.50s | 5.4s |
| 3 | 0.27s | 4.9s |

## Node-kill drill

`make drill-node` — `docker stop` the compute worker under live load, then bring it
back. The data node is never the target: the Postgres volume is local to it, so
killing it would only demonstrate that local storage is local.

| run | longest write gap | loss detected | degraded | back to full strength |
|---|---|---|---|---|
| 1 | 0.75s | 3.4s | 13.8s | 17.2s |
| 2 | 0.76s | 7.9s | 15.6s | 23.5s |
| 3 | 0.76s | 7.9s | 13.8s | 21.6s |

**Ingest never stopped for as long as a second**, because the replica on the surviving
node kept writing. Full replica count came back in 17-24s.

Those two numbers answer different questions and it is worth keeping them apart. The
write gap is what a consumer of the data sees. Time to full strength is what an
operator sees, and it is dominated by how long Kubernetes waits before it will act on
a node it cannot reach — 10s of that is `tolerationSeconds`, deliberately lowered from
the default of 300, and a few more seconds is `node-monitor-grace-period`, lowered from
40s in `kind-config.yaml`. On defaults the same drill takes over five minutes, and the
recovery is not the pod starting, it is Kubernetes deciding to start it.

## Alerting

`make up` brings up Prometheus with four rules. Scaling ingest to zero fires two of
them within a minute:

```
IngestStalled        firing
IngestReplicaDown    firing
IngestLagHigh        inactive
ReplayErrors         inactive
```

Scaling back to two clears both.

The first version of those rules did not fire at all. `sum(rate(mdp_ticks_ingested_total[1m])) == 0`
looks like it says "no data is arriving", but when the last ingest pod goes away the
series goes with it, `rate()` has nothing to evaluate, and the comparison matches
nothing. The alert was silent in exactly the case it existed for. Both rules now carry
an `or absent(...)` arm, which is what makes the table above possible.

## What is not covered

- Postgres is a single replica with a node-local volume. Losing the data node loses the
  database until that node comes back; there is no replication and no backup.
- No load test of the replay tier under concurrency, so its resource limits are
  reasoned about rather than measured.
- Drills are run by hand (and one of them in CI). They are not continuous.
