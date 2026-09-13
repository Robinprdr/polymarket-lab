import asyncio
from copy import deepcopy
import json
import time
import pytest
from websockets.asyncio.server import serve
from websockets.asyncio.client import connect as real_connect
import polymarket_lab.feed as feed_module
from polymarket_lab.feed import Feed
from polymarket_lab.models import OrderBook


def test_multiple_updates_are_atomic(book, book_raw):
    second = deepcopy(book)
    second.token_id = "200"
    second.invalidate("gap")
    books = {"100": book, "200": second}
    feed = Feed(books, lambda *a: None)
    with pytest.raises(ValueError):
        feed.handle({"event_type":"price_change","market":"0xabc","timestamp":"1001",
                     "price_changes":[{"asset_id":"100","side":"BUY","price":"0.4","size":"7"},
                                      {"asset_id":"200","side":"SELL","price":"0.5","size":"2"}]})
    assert books["100"].revision == book.revision
    assert books["100"].bids == book.bids


def test_tick_does_not_refresh_book_and_resolved_invalidates(book):
    feed = Feed({"100":book}, lambda *a: None)
    before = book._monotonic_update
    feed.handle({"event_type":"tick_size_change","asset_id":"100","market":"0xabc",
                 "timestamp":"1001","new_tick_size":"0.001"})
    assert book._monotonic_update == before
    feed.handle({"event_type":"market_resolved","market":"0xabc"})
    assert not book.valid and book.invalid_reason == "market_resolved"


def test_real_local_websocket_reconnect_and_rebuild(monkeypatch, book_raw):
    async def scenario():
        books = {"100": OrderBook("100", "7", "Yes", "0xabc")}
        incidents, subscriptions = [], []
        feed = Feed(books, lambda kind, detail: incidents.append(kind))
        finished = asyncio.Event()
        connections = 0
        async def server(ws):
            nonlocal connections
            connections += 1
            subscriptions.append(json.loads(await ws.recv()))
            snapshot = deepcopy(book_raw)
            snapshot["timestamp"] = str(1000 + connections*10)
            snapshot["bids"] = [{"price":"0.4", "size":str(connections)}]
            await ws.send(json.dumps([snapshot]))
            if connections == 1:
                await asyncio.sleep(.05)
                await ws.close()
            else:
                await ws.send(json.dumps({"event_type":"price_change","market":"0xabc","timestamp":"1021",
                    "price_changes":[{"asset_id":"100","side":"BUY","price":"0.4","size":"9"}]}))
                finished.set()
                await ws.wait_closed()
        async with serve(server, "127.0.0.1", 0) as listener:
            port = listener.sockets[0].getsockname()[1]
            def local_connect(uri, **kwargs):
                assert uri == feed_module.WS_MARKET
                return real_connect(f"ws://127.0.0.1:{port}", **kwargs)
            monkeypatch.setattr(feed_module, "connect", local_connect)
            monkeypatch.setattr(feed_module.random, "uniform", lambda a,b: 0)
            task = asyncio.create_task(feed.run())
            try:
                await asyncio.wait_for(finished.wait(), timeout=8)
                for _ in range(100):
                    if feed.counters["price_change_events"]:
                        break
                    await asyncio.sleep(.01)
                assert feed.counters["book_events"] == 2
                assert feed.counters["reconnections"] == 1
                assert str(next(iter(books["100"].bids.values()))) == "9"
                assert books["100"].ws_ready
                assert all(s == {"assets_ids":["100"],"type":"market","initial_dump":True,
                                 "custom_feature_enabled":True} for s in subscriptions)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            assert not feed.connected and not books["100"].valid
            assert "websocket_disconnected" in incidents
    asyncio.run(scenario())


def test_bad_frame_invalidates_and_silence_times_out(book):
    class WS:
        def __init__(self, frame=None):
            self.frame = frame
        async def send(self, message):
            pass
        async def recv(self):
            if self.frame is not None:
                return self.frame
            await asyncio.sleep(10)
    async def scenario():
        feed = Feed({"100":book}, lambda *a: None)
        with pytest.raises(ValueError):
            await feed.session(WS("not json"))
        assert feed.counters["invalid_messages"] == 1
        assert not book.valid
        feed = Feed({"100":book}, lambda *a: None, silence_seconds=.01)
        with pytest.raises(TimeoutError, match="silent"):
            await feed.session(WS())
        assert feed.counters["interruptions"] == 1
    asyncio.run(scenario())


def test_missing_initial_snapshot_timeout(book):
    class WS:
        async def send(self, message):
            pass
        async def recv(self):
            await asyncio.sleep(.02)
            return "PONG"
    async def scenario():
        feed = Feed({"100":book}, lambda *a: None, snapshot_timeout=.01)
        with pytest.raises(TimeoutError, match="Missing initial"):
            await feed.session(WS())
        assert not book.valid
    asyncio.run(scenario())


def test_resolved_book_cannot_be_reactivated(book, book_raw):
    feed = Feed({"100":book}, lambda *a: None)
    feed.handle({"event_type":"market_resolved","market":"0xabc"})
    feed.invalidate("disconnected")
    feed.handle(book_raw)
    assert not book.valid and book.invalid_reason == "market_resolved"


def test_end_to_end_observer_with_local_public_feed(tmp_path, monkeypatch, market_raw, book_raw):
    import httpx
    from polymarket_lab.monitor import Monitor, parser
    from polymarket_lab.public_api import PublicAPI
    from polymarket_lab.models import dumps, now_ms
    async def scenario():
        payloads = {}
        for token in ("100", "200"):
            payloads[token] = dict(book_raw, asset_id=token, timestamp=str(now_ms()))
        def handler(request):
            if request.url.path == "/markets/keyset":
                return httpx.Response(200, text=dumps({"markets":[market_raw],"next_cursor":None}))
            return httpx.Response(200, json=payloads[request.url.params["token_id"]])
        async def server(ws):
            request = json.loads(await ws.recv())
            await ws.send(json.dumps([payloads[t] for t in request["assets_ids"]]))
            await ws.wait_closed()
        async with serve(server, "127.0.0.1", 0) as listener:
            port = listener.sockets[0].getsockname()[1]
            monkeypatch.setattr(feed_module, "connect", lambda uri, **kwargs:
                                real_connect(f"ws://127.0.0.1:{port}", **kwargs))
            args = parser().parse_args(["--database", str(tmp_path / "db.sqlite3"),
                "--health-seconds", "0.05", "--snapshot-seconds", "0.05", "--reconcile-seconds", "0.05"])
            monitor = Monitor(args, api=PublicAPI(transport=httpx.MockTransport(handler)))
            try:
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(monitor.run(), timeout=.35)
                assert monitor.counters["rest_bootstraps"] == 2
                assert monitor.feed.counters["book_events"] == 2
                assert monitor.counters["reconciliation_matches"] >= 2
                assert monitor.health()["markets_discovered"] == 1
                assert monitor.health()["tokens_discovered"] == 2
                health = [json.loads(row[0]) for row in monitor.store.db.execute("SELECT metrics_json FROM system_health")]
                assert any(row["coverage_complete"] for row in health)
                assert health[-1]["stopping"] and not health[-1]["coverage_complete"]
                assert monitor.store.db.execute("SELECT count(*) FROM book_snapshots").fetchone()[0] >= 4
            finally:
                await monitor.api.close()
                monitor.store.close()
    asyncio.run(scenario())
