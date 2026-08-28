import json
from datetime import datetime, timezone

import ingest


def match(**overrides) -> str:
    payload = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 812345,
        "price": "64210.55",
        "size": "0.0031",
        "time": "2026-08-28T14:03:11.123456Z",
    }
    payload.update(overrides)
    return json.dumps(payload)


def test_parses_a_trade():
    tick = ingest.parse_coinbase(match())
    assert tick is not None
    assert tick.symbol == "BTC-USD"
    assert tick.seq == 812345
    assert tick.price == 64210.55
    assert tick.ts == datetime(2026, 8, 28, 14, 3, 11, 123456, tzinfo=timezone.utc)


def test_ignores_non_trade_messages():
    for kind in ("subscriptions", "heartbeat", "ticker", "error"):
        assert ingest.parse_coinbase(json.dumps({"type": kind})) is None


def test_malformed_trade_is_dropped_not_raised():
    # A feed that starts sending garbage must not take the ingest loop down with it.
    assert ingest.parse_coinbase(match(price="not-a-number")) is None
    assert ingest.parse_coinbase(json.dumps({"type": "match"})) is None


def test_both_replicas_generate_identical_synthetic_ticks(monkeypatch):
    # Dedupe by (symbol, seq) only collapses the two replicas' writes if they agree on
    # seq for the same instant. This is the property that lets both of them write.
    monkeypatch.setattr(ingest.time, "time", lambda: 1_772_000_000.037)
    first = ingest.synthetic_ticks()
    monkeypatch.setattr(ingest.time, "time", lambda: 1_772_000_000.041)
    second = ingest.synthetic_ticks()
    assert first == second
    assert len({t.symbol for t in first}) == len(ingest.SYMBOLS)


def test_synthetic_sequence_advances_with_the_clock(monkeypatch):
    monkeypatch.setattr(ingest.time, "time", lambda: 1_772_000_000.0)
    before = ingest.synthetic_ticks()[0].seq
    monkeypatch.setattr(ingest.time, "time", lambda: 1_772_000_001.0)
    after = ingest.synthetic_ticks()[0].seq
    assert after > before
