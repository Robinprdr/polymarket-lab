import asyncio
from copy import deepcopy
import json

import httpx
from websockets.asyncio.server import serve
from websockets.asyncio.client import connect

import polymarket_lab.feed as feed_module
from polymarket_lab.models import dumps, now_ms
from polymarket_lab.monitor import Monitor, parser
from polymarket_lab.public_api import PublicAPI


def test_gamma_rest_ws_opportunity_open_update_close(tmp_path, monkeypatch, market_raw):
    async def scenario():
        requests, outgoing = [], []
        stamp = now_ms()
        raw = deepcopy(market_raw)
        raw["orderMinSize"] = 1
        payloads = {t: dict(event_type="book", asset_id=t, market="0xabc", timestamp=str(stamp),
            bids=[], asks=[dict(price=p, size="100")], min_order_size="1")
            for t, p in (("100", "0.52"), ("200", "0.50"))}
        def http(request):
            requests.append(request)
            if request.url.path == "/markets/keyset":
                return httpx.Response(200, text=dumps({"markets": [raw], "next_cursor": None}))
            return httpx.Response(200, json=payloads[request.url.params["token_id"]])
        done = asyncio.Event()
        async def wait_until(predicate):
            async with asyncio.timeout(3):
                while not predicate():
                    await asyncio.sleep(.005)
        def delta(when, changes):
            return dumps(dict(event_type="price_change", market="0xabc", timestamp=str(when),
                price_changes=[dict(asset_id=t, side="SELL", price=p, size=s) for t,p,s in changes]))
        async def server(ws):
            outgoing.append(json.loads(await ws.recv()))
            await ws.send(dumps(list(payloads.values())))
            await wait_until(lambda: monitor.feed.counters["book_events"] == 2)
            assert monitor.scanner.episodes == 0
            await ws.send(delta(stamp+1, [("100","0.52","0"),("100","0.45","100")]))
            await wait_until(lambda: monitor.scanner.episodes == 1)
            episode = next(iter(monitor.scanner.open.values()))
            assert episode["gross_edge"] == 5 and episode["gross_cost"] == 95
            await ws.send(delta(stamp+2, [("100","0.45","200"),("200","0.50","200")]))
            await wait_until(lambda: next(iter(monitor.scanner.open.values()))["observation_count"] == 2)
            await ws.send(delta(stamp+3, [("100","0.45","0"),("100","0.60","200")]))
            await wait_until(lambda: not monitor.scanner.open)
            done.set()
            await ws.wait_closed()
        async with serve(server, "127.0.0.1", 0) as listener:
            port = listener.sockets[0].getsockname()[1]
            monkeypatch.setattr(feed_module, "connect", lambda uri, **kwargs: connect(f"ws://127.0.0.1:{port}", **kwargs))
            args = parser().parse_args(["--database", str(tmp_path/"integration.sqlite3"), "--scan-complements"])
            monitor = Monitor(args, api=PublicAPI(transport=httpx.MockTransport(http)))
            task = asyncio.create_task(monitor.run())
            try:
                await asyncio.wait_for(done.wait(), 5)
                rows = monitor.store.db.execute("SELECT record_json FROM complement_opportunities").fetchall()
                assert len(rows) == 1
                record = json.loads(rows[0][0])
                assert record["gross_cost"] == "190.00" and record["gross_edge"] == "10.00"
                assert record["best_quantity_seen"] == "200" and record["observation_count"] == 2
                assert record["closed_at"] is not None and record["close_reason"] == "NO_VALID_POSITIVE_EDGE"
                assert record["estimated_fees"] is record["net_edge"] is None
                samples = monitor.store.db.execute("SELECT observation_json FROM complement_observations ORDER BY id").fetchall()
                assert [json.loads(r[0])["gross_edge"] for r in samples] == ["5.00", "10.00"]
                assert len(requests) == 3  # Gamma and two REST bootstraps, no per-edge network request
                assert all(r.method == "GET" and "authorization" not in r.headers for r in requests)
                assert outgoing == [{"assets_ids":["200","100"], "type":"market",
                                     "initial_dump":True, "custom_feature_enabled":True}]
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                assert monitor.scanner.summary()["currently_open"] == 0
                await monitor.api.close()
                monitor.store.close()
    asyncio.run(scenario())


def test_restart_callback_closes_episode_and_blocks_residual_snapshots(tmp_path, market_raw):
    from polymarket_lab.models import OrderBook, parse_market
    async def scenario():
        api = PublicAPI(transport=httpx.MockTransport(lambda r: httpx.Response(500)))
        args = parser().parse_args(["--database", str(tmp_path/"restart.sqlite3"), "--scan-complements"])
        monitor = Monitor(args, api=api)
        try:
            market = parse_market(market_raw)
            monitor.markets[market.market_id] = market
            monitor.store.registry([market], complete=True)
            stamp = now_ms()
            payloads = []
            for label, price in (("YES", "0.45"), ("NO", "0.50")):
                t = market.yes_no[label]
                monitor.books[t.token_id] = OrderBook(t.token_id, market.market_id, t.outcome, market.condition_id)
                payloads.append(dict(event_type="book", asset_id=t.token_id, market=market.condition_id,
                    timestamp=str(stamp), bids=[], asks=[dict(price=price, size="100")], min_order_size="1"))
            monitor.feed.connected = True
            for p in payloads:
                monitor.feed.handle(p)
            assert monitor.scanner.episodes == 1 and len(monitor.scanner.open) == 1
            monitor.feed.request_restart("rest_divergence")
            assert not monitor.scanner.open
            # Old socket can still finish recv; pending restart must suppress detection.
            for p in payloads:
                monitor.feed.handle(p)
            assert not monitor.scanner.open and monitor.scanner.episodes == 1
            monitor.feed.restart.clear()
            monitor.feed.invalidate("awaiting_websocket_snapshot")
            for p in payloads:
                monitor.feed.handle(dict(p, timestamp=str(stamp-1)))
            assert monitor.scanner.episodes == 2 and len(monitor.scanner.open) == 1
            monitor.feed.connected = False
            monitor.feed.invalidate("disconnected")
            assert not monitor.scanner.open
        finally:
            await api.close()
            monitor.store.close()
    asyncio.run(scenario())
