"""Market-data ingest: public trade feed -> Postgres, with metrics and health.

Two replicas run at once. They both consume the same public feed and both write
every trade, keyed by the exchange's own trade id, and the unique constraint on
(symbol, seq) turns the duplicate write into a no-op. That is what makes the
service survivable: losing a replica -- or the node under it -- costs nothing,
because the other one is already writing the same rows. The alternative, a single
writer with leader election, is more moving parts for a worse failure mode.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
import websocket
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

LOG = logging.getLogger("ingest")

SOURCE = os.environ.get("SOURCE", "coinbase")
SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD").split(",") if s.strip()]
PG_DSN = os.environ["PG_DSN"]
HTTP_PORT = int(os.environ.get("HTTP_PORT", "9100"))
BATCH_MAX = int(os.environ.get("BATCH_MAX", "50"))
BATCH_MS = int(os.environ.get("BATCH_MS", "250"))
SYNTHETIC_HZ = float(os.environ.get("SYNTHETIC_HZ", "20"))
READY_STALE_S = float(os.environ.get("READY_STALE_S", "20"))
MAX_BUFFER = int(os.environ.get("MAX_BUFFER", "20000"))
WS_URL = "wss://ws-feed.exchange.coinbase.com"

TICKS = Counter("mdp_ticks_ingested_total", "Trades written to storage", ["symbol"])
DUPES = Counter("mdp_ticks_duplicate_total", "Trades already written by the other replica")
RECONNECTS = Counter("mdp_ws_reconnects_total", "Feed reconnections")
DB_ERRORS = Counter("mdp_db_errors_total", "Failed database writes")
DROPPED = Counter("mdp_ticks_dropped_total", "Trades dropped because the retry buffer was full")
LAG = Gauge("mdp_ingest_lag_seconds", "Now minus the exchange timestamp of the last trade")
LAST_TICK = Gauge("mdp_last_tick_timestamp_seconds", "Unix time of the last write")

# Both replicas run this on startup. Concurrent CREATE TABLE IF NOT EXISTS is not
# safe in Postgres -- the two transactions race in the system catalog and one of them
# fails on a unique violation in pg_type -- so an advisory lock serializes them.
SCHEMA = """
SELECT pg_advisory_lock(hashtext('mdp.schema'));
CREATE TABLE IF NOT EXISTS ticks (
    symbol TEXT        NOT NULL,
    seq    BIGINT      NOT NULL,
    price  NUMERIC     NOT NULL,
    size   NUMERIC     NOT NULL,
    ts     TIMESTAMPTZ NOT NULL,
    -- Written by Postgres, not by the service. The drills measure recovery as "when
    -- did a new row land", and `ts` cannot answer that: it is the exchange's clock,
    -- so a trade that was already in flight when a node died looks like a recovery.
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, seq)
);
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS ticks_symbol_ts ON ticks (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS ticks_inserted_at ON ticks (inserted_at DESC);
SELECT pg_advisory_unlock(hashtext('mdp.schema'));
"""


@dataclass(frozen=True)
class Tick:
    symbol: str
    seq: int
    price: float
    size: float
    ts: datetime


def parse_coinbase(raw: str) -> Tick | None:
    """Normalize one feed message. Returns None for anything that is not a trade."""
    msg = json.loads(raw)
    if msg.get("type") not in ("match", "last_match"):
        return None
    try:
        return Tick(
            symbol=msg["product_id"],
            seq=int(msg["trade_id"]),
            price=float(msg["price"]),
            size=float(msg["size"]),
            ts=datetime.fromisoformat(msg["time"].replace("Z", "+00:00")),
        )
    except (KeyError, ValueError) as err:
        LOG.warning("unparseable trade: %s", err)
        return None


def synthetic_ticks() -> "list[Tick]":
    """Deterministic stand-in for the live feed.

    Both replicas must produce identical rows for the same instant, or dedupe by
    (symbol, seq) would not collapse them -- so the sequence number is derived from
    the clock, not from a per-process counter.
    """
    now = time.time()
    seq = int(now * SYNTHETIC_HZ)
    ts = datetime.fromtimestamp(seq / SYNTHETIC_HZ, tz=timezone.utc)
    out = []
    for i, symbol in enumerate(SYMBOLS):
        base = 100.0 * (i + 1)
        out.append(Tick(symbol, seq, base + (seq % 997) / 100.0, 0.01 + (seq % 13) / 100.0, ts))
    return out


class Writer:
    """Batched writer. Reconnects to Postgres rather than dying when it goes away."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.conn: psycopg.Connection | None = None
        self.buffer: list[Tick] = []
        self.last_flush = time.time()
        self.last_write = 0.0
        self.retry_after = 0.0
        self.backoff = 0.25
        self.lock = threading.Lock()

    def connect(self) -> psycopg.Connection:
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5)
            with self.conn.cursor() as cur:
                cur.execute(SCHEMA)
        return self.conn

    def add(self, tick: Tick) -> None:
        with self.lock:
            self.buffer.append(tick)
            due = len(self.buffer) >= BATCH_MAX or (time.time() - self.last_flush) * 1000 >= BATCH_MS
        if due:
            self.flush()

    def flush(self) -> None:
        # Postgres being unreachable is a normal event (a restart, a rescheduled pod).
        # Retrying it four times a second turns a blip into a log flood and a busy
        # loop, so failures back off.
        if time.time() < self.retry_after:
            return
        with self.lock:
            batch, self.buffer = self.buffer, []
            self.last_flush = time.time()
        if not batch:
            return
        try:
            conn = self.connect()
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO ticks (symbol, seq, price, size, ts) VALUES (%s, %s, %s, %s, %s)"
                    " ON CONFLICT (symbol, seq) DO NOTHING",
                    [(t.symbol, t.seq, t.price, t.size, t.ts) for t in batch],
                )
                written = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(batch)
        except psycopg.Error as err:
            DB_ERRORS.inc()
            # Put the batch back. Discarding it would mean a database restart silently
            # loses trades -- the failure the whole idempotent-write design exists to
            # avoid. The buffer is bounded, and overflow is counted rather than hidden.
            with self.lock:
                self.buffer[:0] = batch
                overflow = len(self.buffer) - MAX_BUFFER
                if overflow > 0:
                    del self.buffer[:overflow]
                    DROPPED.inc(overflow)
            LOG.error("write failed, retrying in %.2fs: %s", self.backoff, err)
            self.retry_after = time.time() + self.backoff
            self.backoff = min(self.backoff * 2, 5.0)
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            return

        self.backoff = 0.25
        DUPES.inc(max(0, len(batch) - written))
        for tick in batch:
            TICKS.labels(tick.symbol).inc()
        newest = max(batch, key=lambda t: t.ts)
        LAG.set(max(0.0, time.time() - newest.ts.timestamp()))
        self.last_write = time.time()
        LAST_TICK.set(self.last_write)

    def healthy(self) -> bool:
        try:
            conn = self.connect()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except psycopg.Error:
            return False


def make_http_handler(writer: Writer):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.startswith("/metrics"):
                body = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            elif self.path.startswith("/healthz"):
                # Liveness: the process is up. Restarting it would not help a
                # database that is down, so this must not depend on Postgres.
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
            elif self.path.startswith("/readyz"):
                # Readiness: can we write, and have we written recently? A replica
                # that has silently stopped producing is removed from the fleet's
                # health picture instead of sitting there looking fine.
                started = writer.last_write == 0.0
                fresh = time.time() - writer.last_write < READY_STALE_S
                ok = writer.healthy() and (fresh or started)
                body = b"ready\n" if ok else b"not ready\n"
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "text/plain")
            else:
                body = b"not found\n"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            try:
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # kubelet closes probe connections as soon as it has the status line.
                # Not an error, and not worth a stack trace every few seconds.
                pass

        def log_message(self, *_args) -> None:
            pass

    return Handler


def flush_loop(writer: Writer, stop: threading.Event) -> None:
    """A partial batch must not sit in memory waiting for traffic that may not come."""
    while not stop.is_set():
        stop.wait(BATCH_MS / 1000)
        writer.flush()


def run_coinbase(writer: Writer, stop: threading.Event) -> None:
    backoff = 1.0
    while not stop.is_set():
        try:
            ws = websocket.create_connection(WS_URL, timeout=30)
            ws.send(json.dumps({"type": "subscribe", "product_ids": SYMBOLS, "channels": ["matches"]}))
            LOG.info("subscribed to %s", SYMBOLS)
            backoff = 1.0
            while not stop.is_set():
                tick = parse_coinbase(ws.recv())
                if tick is not None:
                    writer.add(tick)
        except Exception as err:  # noqa: BLE001 -- any failure here means reconnect
            RECONNECTS.inc()
            LOG.warning("feed dropped (%s); reconnecting in %.0fs", err, backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, 30.0)


def run_synthetic(writer: Writer, stop: threading.Event) -> None:
    last_seq = -1
    while not stop.is_set():
        for tick in synthetic_ticks():
            if tick.seq != last_seq:
                writer.add(tick)
        last_seq = int(time.time() * SYNTHETIC_HZ)
        stop.wait(1.0 / SYNTHETIC_HZ)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    writer = Writer(PG_DSN)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    # Threaded: readiness touches the database and can block for seconds. On a
    # single-threaded server that would queue the liveness probe behind it, and the
    # pod would be restarted for a database problem that restarting cannot fix.
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), make_http_handler(writer))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=flush_loop, args=(writer, stop), daemon=True).start()
    LOG.info("ingest up: source=%s symbols=%s port=%d", SOURCE, SYMBOLS, HTTP_PORT)

    if SOURCE == "synthetic":
        run_synthetic(writer, stop)
    else:
        run_coinbase(writer, stop)

    writer.flush()
    server.shutdown()


if __name__ == "__main__":
    main()
