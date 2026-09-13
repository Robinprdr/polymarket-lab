import asyncio
import json
from decimal import Decimal
import httpx
import pytest
from polymarket_lab.public_api import PublicAPI, CLOB
from polymarket_lab.storage import Storage
from polymarket_lab.models import now_ms


def test_sqlite_roundtrip_and_retention(tmp_path, market, book):
    path = tmp_path / "test.sqlite3"
    db = Storage(path)
    db.registry([market], complete=True)
    db.registry([market], complete=True)
    db.snapshot(book, full=True)
    db.snapshot(book)
    db.health({"timestamp":now_ms(),"errors":2})
    db.incident("gap", {"token":"100"})
    assert db.db.execute("SELECT count(*) FROM markets").fetchone()[0] == 1
    assert db.db.execute("SELECT outcome FROM tokens WHERE token_id='100'").fetchone()[0] == "Yes"
    assert db.db.execute("SELECT tick_size FROM markets").fetchone()[0] == "0.01"
    payload = json.loads(db.db.execute("SELECT snapshot_json FROM book_snapshots LIMIT 1").fetchone()[0])
    assert payload["bids"][0]["size"] == "12.34567890123456789"
    assert db.size_bytes() > 0
    db.close()
    db = Storage(path)
    assert db.db.execute("SELECT count(*) FROM system_health").fetchone()[0] == 1
    db.prune(now_ms()+2*86400000)
    assert db.db.execute("SELECT count(*) FROM book_snapshots").fetchone()[0] == 1
    db.prune(now_ms()+31*86400000)
    assert db.db.execute("SELECT count(*) FROM book_snapshots").fetchone()[0] == 0
    assert db.db.execute("SELECT count(*) FROM incidents").fetchone()[0] == 0
    assert db.db.execute("SELECT count(*) FROM markets").fetchone()[0] == 1
    db.close()


def test_public_routes_get_only_and_cursor(book_raw, market_raw):
    requests = []
    def handler(request):
        requests.append(request)
        if request.url.path == "/markets/keyset":
            assert request.url.params["after_cursor"] == "cursor"
            return httpx.Response(200, json={"markets":[],"next_cursor":None})
        return httpx.Response(200, json=book_raw)
    async def scenario():
        api = PublicAPI(transport=httpx.MockTransport(handler))
        assert await api.market_page("cursor") == ([], None)
        assert (await api.book("100"))["asset_id"] == "100"
        with pytest.raises(ValueError):
            await api._get(CLOB, "/not-allowed", {})
        with pytest.raises(ValueError):
            await api.book("100&unexpected=true")
        await api.close()
    asyncio.run(scenario())
    assert all(r.method == "GET" and "authorization" not in r.headers for r in requests)


def test_http_decimal_decode_and_no_redirects():
    async def scenario():
        api = PublicAPI(transport=httpx.MockTransport(lambda req:
            httpx.Response(200, text='{"markets":[{"n":0.1234567890123456789}]}')))
        rows, _ = await api.market_page()
        assert rows[0]["n"] == Decimal("0.1234567890123456789")
        await api.close()
        api = PublicAPI(transport=httpx.MockTransport(lambda req:
            httpx.Response(302, headers={"Location":"https://example.org"})))
        with pytest.raises(httpx.HTTPStatusError):
            await api.market_page()
        await api.close()
    asyncio.run(scenario())
