from decimal import Decimal
import json
import time

import pytest

from test_complement import pair
from polymarket_lab.models import now_ms
from polymarket_lab.opportunities import ComplementScanner
from polymarket_lab.storage import Storage


@pytest.fixture
def tracker(tmp_path):
    store = Storage(tmp_path / "research.sqlite3")
    lines = []
    scanner = ComplementScanner(store, output=lines.append)
    yield scanner, store, lines
    store.close()


def test_episode_lifecycle_exact_sqlite_and_best_values(pair, tracker):
    market, books = pair
    scanner, store, lines = tracker
    scanner.scan(market, books, timestamp=1000, mono_ns=10_000_000_000)
    scanner.scan(market, books, timestamp=1100, mono_ns=10_100_000_000)  # duplicate
    assert len(lines) == 1
    for book in books.values():
        book.asks[next(iter(book.asks))] = Decimal(200)
        book.revision += 1
    scanner.scan(market, books, timestamp=1500, mono_ns=10_500_000_000)
    episode = next(iter(scanner.open.values()))
    assert episode["observation_count"] == 2 and episode["duration_ms"] == 500
    assert episode["best_gross_edge_seen"] == 10 and episode["best_quantity_seen"] == 200
    books["100"].asks = {Decimal(".6"): Decimal(200)}
    books["100"].revision += 1
    scanner.scan(market, books, timestamp=1600, mono_ns=10_600_000_000)
    assert not scanner.open and "CLOSE" in lines[-1]
    raw = store.db.execute("SELECT record_json FROM complement_opportunities").fetchone()[0]
    saved = json.loads(raw)
    assert saved["gross_cost"] == "190.00" and saved["gross_edge"] == "10.00"
    assert saved["last_seen_at"] == 1500 and saved["closed_at"] == 1600
    assert saved["duration_ms"] == 500 and saved["close_reason"] == "NO_VALID_POSITIVE_EDGE"
    assert not saved["censored"]
    assert saved["survival_status"] == "NOT_MEASURED"
    assert saved["yes_consumed_depth"] == [{"price": "0.45", "size": "200"}]
    assert store.db.execute("SELECT count(*) FROM complement_observations").fetchone()[0] == 2
    books["100"].asks = {Decimal(".45"): Decimal(200)}
    books["100"].revision += 1
    scanner.scan(market, books, timestamp=1700, mono_ns=10_700_000_000)
    assert scanner.episodes == 2 and len(scanner.open) == 1
    assert scanner.summary()["max_gross_edge"] == 10
    assert scanner.summary()["fees_unknown_count"] == 2
    assert scanner.summary()["median_duration_ms"] == 500


def test_reconnect_closes_and_never_creates_phantom(pair, tracker):
    market, books = pair
    scanner, store, _ = tracker
    scanner.scan(market, books, mono_ns=10_000_000_000)
    for book in books.values():
        book.invalidate("disconnected")
    scanner.scan(market, books, available=False, mono_ns=10_100_000_000)
    assert not scanner.open
    scanner.scan(market, books, mono_ns=10_200_000_000)
    assert scanner.episodes == 1
    raw = json.loads(store.db.execute("SELECT record_json FROM complement_opportunities").fetchone()[0])
    assert raw["censored"] and raw["close_reason"] == "NOT_OBSERVABLE"


def test_time_alone_closes_stale_episode(pair, tracker):
    market, books = pair
    scanner, _, _ = tracker
    scanner.scan(market, books, mono_ns=10_000_000_000)
    for book in books.values():
        book._monotonic_update = time.monotonic()-31
    scanner.expire({market.market_id: market}, books)
    assert not scanner.open


def test_unknown_minimum_and_fee_updates_keep_one_episode(pair, tracker):
    market, books = pair
    scanner, _, lines = tracker
    scanner.scan(market, books, mono_ns=10_000_000_000)
    market.fee_metadata = {"feesEnabled": False}
    scanner.scan(market, books, mono_ns=10_100_000_000)
    assert scanner.episodes == 1 and len(lines) == 2
    assert next(iter(scanner.open.values()))["best_net_edge_seen"] == 5
    assert scanner.summary()["fees_known_count"] == scanner.summary()["fees_unknown_count"] == 1


def test_restart_process_censors_old_episode_without_extending_duration(pair, tmp_path):
    market, books = pair
    path = tmp_path / "durable.sqlite3"
    store = Storage(path)
    tracker = ComplementScanner(store, output=lambda _: None)
    tracker.scan(market, books, timestamp=1234, mono_ns=10_000_000_000)
    store.close()
    store = Storage(path)
    try:
        replacement = ComplementScanner(store, output=lambda _: None)
        raw = json.loads(store.db.execute("SELECT record_json FROM complement_opportunities").fetchone()[0])
        assert raw["last_seen_at"] == 1234 and raw["duration_ms"] == 0
        assert raw["censored"] and raw["close_reason"] == "PROCESS_INTERRUPTED"
        assert not replacement.open
    finally:
        store.close()


def test_all_positive_research_data_survives_phase1_purge(pair, tracker):
    market, books = pair
    scanner, store, _ = tracker
    scanner.scan(market, books, mono_ns=10_000_000_000)
    scanner.close_all("SESSION_STOPPED")
    store.prune(now_ms()+60*86400000)
    assert store.db.execute("SELECT count(*) FROM complement_opportunities").fetchone()[0] == 1
    assert store.db.execute("SELECT count(*) FROM complement_observations").fetchone()[0] == 1


def test_schema_upgrade_keeps_phase1_registry(tmp_path, pair):
    import sqlite3
    path = tmp_path / "old.sqlite3"
    db = sqlite3.connect(path)
    db.execute("PRAGMA user_version=1")
    db.close()
    store = Storage(path)
    try:
        store.registry([pair[0]], complete=True)
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert store.db.execute("SELECT count(*) FROM markets").fetchone()[0] == 1
    finally:
        store.close()
