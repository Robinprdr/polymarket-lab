import asyncio
from copy import deepcopy
from decimal import Decimal
import pytest
from polymarket_lab.models import OrderBook
from polymarket_lab.monitor import Monitor, parser, validate


class API:
    requests = 0
    retries = 0
    def __init__(self, payload):
        self.payload = payload
    async def book(self, token):
        return deepcopy(self.payload)
    async def close(self):
        pass


@pytest.fixture
def monitor(tmp_path, market, book, book_raw):
    args = parser().parse_args(["--database", str(tmp_path / "db.sqlite3")])
    result = Monitor(args, api=API(book_raw))
    result.markets[market.market_id] = market
    result.store.registry([market], complete=True)
    result.books[book.token_id] = book
    yield result
    result.store.close()


def test_reconciliation_match_does_not_refresh_age(monitor):
    book = monitor.books["100"]
    before = book.last_local_update
    assert asyncio.run(monitor.reconcile_one("100"))["status"] == "matched"
    assert book.last_local_update == before


def test_reconciliation_divergence_visible_and_invalidates(monitor):
    monitor.api.payload["bids"][0]["size"] = "100"
    assert asyncio.run(monitor.reconcile_one("100"))["status"] == "divergence_resubscribe"
    assert monitor.feed.restart.is_set()
    assert not monitor.books["100"].valid
    assert monitor.counters["divergences"] == 1
    assert monitor.store.db.execute("SELECT count(*) FROM book_snapshots").fetchone()[0] == 2


def test_reconciliation_never_overwrites_concurrent_update(monitor):
    async def book(token):
        monitor.books[token].changes([{"asset_id":token,"side":"BUY","price":"0.4","size":"777"}], "1001")
        return monitor.api.payload
    monitor.api.book = book
    assert asyncio.run(monitor.reconcile_one("100"))["status"] == "inconclusive_concurrent_update"
    assert monitor.books["100"].bids[Decimal("0.4")] == 777


def test_reconciliation_older_rest(monitor):
    monitor.api.payload["timestamp"] = "999"
    assert asyncio.run(monitor.reconcile_one("100"))["status"] == "inconclusive_older_rest"
    assert not monitor.feed.restart.is_set()


def test_health_reports_incomplete_stale(monitor):
    metrics = monitor.health()
    assert metrics["tokens_discovered"] == 2
    assert metrics["tokens_followed"] == 1
    assert metrics["tokens_stale_or_unready"] == 1
    assert metrics["coverage_complete"] is False
    assert metrics["dropped_messages"] is None


def test_discovery_pagination_and_cap(monitor, market_raw):
    calls = []
    async def page(cursor=None):
        calls.append(cursor)
        if cursor is None:
            return [market_raw], "second"
        second = dict(market_raw, id="8", clobTokenIds='["300","400"]')
        return [second], None
    async def bootstrap():
        pass
    monitor.api.market_page = page
    monitor.bootstrap = bootstrap
    monitor.args.max_markets = 1
    asyncio.run(monitor.discover())
    assert calls == [None, "second"]
    assert monitor.discovery_complete and len(monitor.markets) == 2
    assert len(monitor.books) == 2
    assert monitor.health()["coverage_complete"] is False


def test_repeated_cursor_is_incomplete(monitor, market_raw):
    async def page(cursor=None):
        return [market_raw], "same"
    async def bootstrap():
        pass
    monitor.api.market_page = page
    monitor.bootstrap = bootstrap
    asyncio.run(monitor.discover())
    assert not monitor.discovery_complete
    assert monitor.discovery_status == "error"


@pytest.mark.parametrize("option,value", [("--duration","nan"),("--health-seconds","0"),
                                         ("--max-markets","101"),("--discovery-pages","-1")])
def test_config_validation(option, value):
    with pytest.raises(ValueError):
        validate(parser().parse_args([option,value]))


def test_empty_complete_discovery_unsubscribes_old_markets(monitor):
    async def page(cursor=None):
        return [], None
    async def bootstrap():
        pass
    monitor.api.market_page = page
    monitor.bootstrap = bootstrap
    asyncio.run(monitor.discover())
    assert monitor.discovery_complete and monitor.markets == {} and monitor.books == {}
    assert monitor.store.db.execute("SELECT in_latest_discovery FROM markets").fetchone()[0] == 0


def test_partial_discovery_is_explicit(monitor, market_raw):
    monitor.args.discovery_pages = 1
    async def page(cursor=None):
        return [market_raw], "more"
    async def bootstrap():
        pass
    monitor.api.market_page = page
    monitor.bootstrap = bootstrap
    asyncio.run(monitor.discover())
    assert monitor.discovery_status == "capped" and not monitor.discovery_complete
