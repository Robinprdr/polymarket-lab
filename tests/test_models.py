from copy import deepcopy
from decimal import Decimal
import pytest
from polymarket_lab.models import parse_market, number, OrderBook


def test_metadata_and_reversed_yes_no(market):
    assert market.yes_no["YES"].token_id == "100"
    assert market.yes_no["NO"].token_id == "200"
    assert market.event_id == "9"
    assert market.tick_size == Decimal("0.01")
    assert market.fee_metadata == {"feesEnabled": True}
    assert market.resolution_metadata["description"] == "Synthetic rules only"
    assert market.retrieved_at == 1234 and market.followable


def test_non_yes_no_not_inferred(market_raw):
    market_raw["outcomes"] = ["Team A", "Team B"]
    assert parse_market(market_raw).yes_no == {}


@pytest.mark.parametrize("field,value", [
    ("outcomes", ["Yes"]), ("outcomes", ["yes", "YES"]),
    ("clobTokenIds", ["100", "100"]), ("clobTokenIds", [None, "200"]),
    ("active", "true"), ("outcomes", ["", "No"]),
])
def test_bad_metadata(market_raw, field, value):
    market_raw[field] = value
    with pytest.raises(ValueError):
        parse_market(market_raw)


def test_snapshot_sorting_exactness(book):
    assert [x["price"] for x in book.depth("bids")] == ["0.40", "0.10"]
    assert [x["price"] for x in book.depth("asks")] == ["0.60", "0.90"]
    assert book.bids[Decimal("0.4")] == Decimal("12.34567890123456789")
    assert number("0.1") + number("0.2") == number("0.3")


@pytest.mark.parametrize("value", [0.1, "NaN", "Infinity", "-1", True])
def test_reject_bad_numbers(value):
    with pytest.raises(ValueError):
        number(value)


def test_age_monotonic_and_staleness(book):
    assert book.book_age_ms(10.5) == 500
    assert not book.stale(1000, 10.5)
    assert book.stale(1000, 12)
    book.invalidate("gap")
    assert book.stale(1000, 10.5)
    assert OrderBook("1", "2", "Yes").book_age_ms() is None


def test_absolute_updates_removal_and_atomic_reject(book):
    book.changes([{"asset_id":"100","side":"BUY","price":"0.4","size":"2"},
                  {"asset_id":"100","side":"SELL","price":"0.6","size":"0"}], "1001")
    assert book.bids[Decimal("0.4")] == 2
    assert Decimal("0.6") not in book.asks
    saved = deepcopy(book)
    with pytest.raises(ValueError):
        book.changes([{"asset_id":"100","side":"BUY","price":"0.4","size":"3"},
                      {"asset_id":"100","side":"WRONG","price":"0.5","size":"3"}], "1002")
    assert book.bids == saved.bids and book.revision == saved.revision


def test_reconnect_requires_new_baseline(book, book_raw):
    book.invalidate("disconnected")
    with pytest.raises(ValueError, match="before WebSocket"):
        book.changes([], "1001")
    book.snapshot(book_raw, source="rest")
    assert not book.ws_ready
    with pytest.raises(ValueError):
        book.changes([], "1001")
    book.snapshot(book_raw, source="websocket")
    book.changes([], "1001")


def test_out_of_order_and_identity(book, book_raw):
    book_raw["timestamp"] = "999"
    with pytest.raises(ValueError, match="Out-of-order"):
        book.snapshot(book_raw)
    book_raw["asset_id"] = "200"
    with pytest.raises(ValueError, match="identity"):
        book.snapshot(book_raw)


def test_invalid_snapshot_does_not_partially_mutate(book, book_raw):
    book_raw["asks"][0]["size"] = "-1"
    previous = deepcopy(book)
    with pytest.raises(ValueError):
        book.snapshot(book_raw)
    assert book == previous


def test_duplicate_zero_levels_rejected(book, book_raw):
    book_raw["bids"] = [{"price":"0.1","size":"0"},{"price":"0.1","size":"1"}]
    with pytest.raises(ValueError, match="duplicate"):
        book.snapshot(book_raw)


def test_future_timestamp_rejected(book, book_raw):
    from polymarket_lab.models import now_ms
    book_raw["timestamp"] = str(now_ms()+60000)
    with pytest.raises(ValueError, match="future"):
        book.snapshot(book_raw)


def test_recent_receive_cannot_hide_old_source_snapshot(book, book_raw):
    book.snapshot(book_raw, source="websocket", received=61000, monotonic=10)
    assert book.book_age_ms(10.5) == 60500
    assert book.stale(30000, 10.5)
