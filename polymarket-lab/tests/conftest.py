import json
from decimal import Decimal
from pathlib import Path
import pytest
from polymarket_lab.models import OrderBook, parse_market


@pytest.fixture
def market_raw():
    return json.loads((Path(__file__).parent / "fixtures/market.json").read_text(), parse_float=Decimal)


@pytest.fixture
def book_raw():
    return json.loads((Path(__file__).parent / "fixtures/book.json").read_text())


@pytest.fixture
def market(market_raw):
    return parse_market(market_raw, received=1234)


@pytest.fixture
def book(book_raw):
    result = OrderBook("100", "7", "Yes", "0xabc")
    result.snapshot(book_raw, source="websocket", received=1000, monotonic=10)
    return result
