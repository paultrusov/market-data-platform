"""Replay API: serve stored market data back to a consumer at a chosen speed.

The point of a replay tier is that a strategy or a dashboard can re-run a past
session without touching the live feed. `speed` is a multiplier on wall clock:
speed=60 replays an hour in a minute, speed=0 streams as fast as the client reads.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import psycopg
import psycopg_pool
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

PG_DSN = os.environ["PG_DSN"]
MAX_ROWS = int(os.environ.get("MAX_ROWS", "200000"))

REQUESTS = Counter("mdp_replay_requests_total", "Replay requests", ["endpoint"])
ERRORS = Counter("mdp_replay_errors_total", "Replay requests that failed", ["endpoint"])
ROWS = Counter("mdp_replay_rows_streamed_total", "Ticks streamed to consumers")
LATENCY = Histogram(
    "mdp_replay_request_seconds",
    "Time to first byte of a replay response",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

pool: psycopg_pool.AsyncConnectionPool | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool
    # A pool, not a connection per request: Postgres backends are expensive and the
    # replay tier is the part that scales out under load.
    pool = psycopg_pool.AsyncConnectionPool(PG_DSN, min_size=1, max_size=8, open=False)
    # Deliberately not waiting for the first connection. Blocking startup on Postgres
    # means the process is unresponsive while the database is coming up, the liveness
    # probe kills it, and the pod crash-loops for a dependency that is merely slow.
    # Readiness is where "the database is not there yet" belongs.
    await pool.open(wait=False)
    yield
    await pool.close()


app = FastAPI(title="market-data replay", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> Response:
    return Response("ok\n", media_type="text/plain")


@app.get("/readyz")
async def readyz() -> Response:
    try:
        async with pool.connection(timeout=2) as conn:
            await conn.execute("SELECT 1")
    except (psycopg.Error, psycopg_pool.PoolTimeout, AttributeError):
        return Response("not ready\n", status_code=503, media_type="text/plain")
    return Response("ready\n", media_type="text/plain")


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/stats")
async def stats() -> dict:
    REQUESTS.labels("stats").inc()
    try:
        async with pool.connection(timeout=5) as conn:
            rows = await (await conn.execute(
                "SELECT symbol, COUNT(*) AS n, MIN(ts) AS first, MAX(ts) AS last FROM ticks GROUP BY symbol ORDER BY symbol"
            )).fetchall()
            total = await (await conn.execute("SELECT COUNT(*) FROM ticks")).fetchone()
    except (psycopg.Error, psycopg_pool.PoolTimeout) as err:
        ERRORS.labels("stats").inc()
        raise HTTPException(status_code=503, detail=str(err)) from err
    return {
        "total": total[0],
        "symbols": [
            {"symbol": s, "ticks": n, "first": first.isoformat(), "last": last.isoformat()}
            for s, n, first, last in rows
        ],
    }


@app.get("/replay")
async def replay(
    symbol: str = Query(..., description="e.g. BTC-USD"),
    minutes: int = Query(5, ge=1, le=1440, description="window ending now, unless start/end given"),
    start: datetime | None = None,
    end: datetime | None = None,
    speed: float = Query(60.0, ge=0.0, description="wall-clock multiplier; 0 = as fast as possible"),
) -> StreamingResponse:
    REQUESTS.labels("replay").inc()
    started = time.perf_counter()
    if end is None:
        end = datetime.now(timezone.utc)
    if start is None:
        start = end - timedelta(minutes=minutes)

    async def stream():
        try:
            async with pool.connection(timeout=5) as conn:
                cur = await conn.execute(
                    "SELECT symbol, seq, price, size, ts FROM ticks"
                    " WHERE symbol = %s AND ts >= %s AND ts <= %s ORDER BY ts, seq LIMIT %s",
                    (symbol, start, end, MAX_ROWS),
                )
                previous: datetime | None = None
                async for sym, seq, price, size, ts in cur:
                    if speed > 0 and previous is not None:
                        gap = (ts - previous).total_seconds() / speed
                        if gap > 0:
                            await asyncio.sleep(min(gap, 5.0))
                    previous = ts
                    ROWS.inc()
                    yield json.dumps({
                        "symbol": sym, "seq": seq, "price": float(price),
                        "size": float(size), "ts": ts.isoformat(),
                    }) + "\n"
        except (psycopg.Error, psycopg_pool.PoolTimeout) as err:
            ERRORS.labels("replay").inc()
            yield json.dumps({"error": str(err)}) + "\n"

    LATENCY.observe(time.perf_counter() - started)
    return StreamingResponse(stream(), media_type="application/x-ndjson")
