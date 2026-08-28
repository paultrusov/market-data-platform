# market-data platform

A market-data ingest and replay platform that runs as a small production-shaped
distributed system: two Dockerized Python services on a three-node Kubernetes cluster,
deployed by Terraform, monitored by Prometheus, and tested by killing things while it
is running.

```bash
make up          # cluster, images, deploy, wait  (~3 minutes from nothing)
make status      # what is running, and how much data is in it
make drill-node  # stop a worker node under live load and measure the recovery
make down
```

Measured recovery numbers are in [RELIABILITY.md](RELIABILITY.md).

## What it does

```
  Coinbase public feed                      ┌──────────────┐
           │                                │  Prometheus  │──── alerts
           ▼                                └──────┬───────┘
  ┌──────────────────┐                             │ scrapes both tiers
  │  ingest  x2      │──── writes ──┐              │   by DNS, per pod
  │  (python)        │              ▼              │
  └──────────────────┘        ┌───────────┐        │
                              │ postgres  │        │
  ┌──────────────────┐        │ (pinned   │◀───────┘
  │  replay  x2      │◀─reads─│  to the   │
  │  (fastapi)       │        │ data node)│
  └──────────────────┘        └───────────┘
           │
   NodePort 30080
```

- **ingest** streams live trades from Coinbase's public websocket feed, normalizes
  them, and writes them to Postgres in batches.
- **replay** serves stored windows back over HTTP as newline-delimited JSON, at a
  configurable speed multiplier, so a consumer can re-run a past session.
- **Prometheus** scrapes both tiers and evaluates four alert rules.

```bash
curl 'http://localhost:30080/stats'
curl 'http://localhost:30080/replay?symbol=BTC-USD&minutes=5&speed=60'
open http://localhost:30090        # prometheus
```

## Two ingest replicas, both writing

The obvious design is one writer, because two writers means duplicate rows. This runs
two, and lets the `(symbol, seq)` primary key throw the duplicates away —
`ON CONFLICT DO NOTHING`, where `seq` is the exchange's own trade id, so both replicas
agree on what a given trade is.

That one decision is what the reliability numbers rest on. Losing a replica, or the
node under it, does not interrupt the data: the other replica is already writing the
same rows. The alternative — a single writer with leader election — is more moving
parts and a worse failure mode, because a lost leader is downtime until a new one is
elected. In steady state the drills logged 2,692 collapsed duplicate writes, which is
the mechanism visibly doing its job.

## Design notes

**Recovery speed is a configuration decision, not a property.** A pod on an unreachable
node is evicted only after `tolerationSeconds`, which defaults to 300. On defaults, the
node-kill drill takes over five minutes and almost none of it is the pod starting up.
Set to 10, with `node-monitor-grace-period` lowered from 40s to 10s in the kind config,
the same drill finishes in about 20 seconds. The cost is reacting to a network blip as
though it were a dead node, which is cheap here precisely because the writes are
idempotent.

**Liveness and readiness are not the same probe.** `/healthz` reports only that the
process is alive and never touches Postgres — restarting a pod cannot fix a database
that is down, and a liveness probe wired to a dependency turns one outage into a
crash-loop. `/readyz` is where the dependency belongs, plus a staleness check, so a
replica that has silently stopped producing is taken out of the health picture instead
of sitting there looking fine.

**The probe server is threaded.** Readiness touches the database and can block for
seconds. On a single-threaded server that queues the liveness probe behind it, and the
pod gets killed for a database problem.

**Storage is pinned to a node, and the drills know it.** Postgres' volume is provisioned
by kind's local-path storage class, so the data lives on one node's disk and cannot
follow the pod. The node is labelled `mdp.io/role=data`, Postgres has a matching
`nodeSelector`, and the drills only ever kill the `compute` node. Discovering that
during a drill instead of designing for it is how a demo becomes a debugging session.

**Failed writes go back in the buffer.** A batch that Postgres rejects is pushed back to
the front of the queue and retried with backoff, up to a bounded buffer, with overflow
counted in `mdp_ticks_dropped_total`. The first version discarded the batch, which
meant a database restart silently lost trades — the exact failure the idempotent-write
design exists to prevent.

**Alerts must survive the thing they watch.** See RELIABILITY.md: `rate(x) == 0` cannot
fire once `x` stops existing.

## Layout

| path | |
|---|---|
| `ingest/` | feed consumer, batched writer, metrics, health |
| `replay/` | FastAPI replay and stats API |
| `infra/` | Terraform: namespace, Postgres, both tiers, Prometheus, services |
| `kind-config.yaml` | the three-node cluster, node labels, control-plane tuning |
| `drills/` | node-kill and pod-kill drills, with the measurement code |
| `tests/` | unit tests for feed parsing and the dedupe invariant |
| `.github/workflows/ci.yml` | tests, image builds, then a real deploy to a kind cluster and a drill |

## CI/CD

`ci.yml` runs unit tests, `terraform fmt`/`validate`, and both image builds, then
brings up a real three-node kind cluster, deploys with the same Terraform, smoke-tests
the replay API, checks that Prometheus has every target up, and runs the pod-kill
drill. It uses the synthetic feed so a slow public exchange endpoint cannot turn the
pipeline red.

## Requirements

Docker, `kind`, `kubectl`, `terraform`, Python 3.13. `make up` handles the rest.
