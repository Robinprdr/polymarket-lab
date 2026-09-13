"""WebSocket lifecycle and atomic event application. No REST/WS blind merging."""
from __future__ import annotations
import asyncio
from collections import Counter, defaultdict
from copy import deepcopy
import json
import logging
import random
import time
from decimal import Decimal
from websockets.asyncio.client import connect
from .models import number
from .public_api import WS_MARKET

LOG = logging.getLogger(__name__)


class Feed:
    def __init__(self, books, incident, *, silence_seconds=30, snapshot_timeout=20):
        self.books = books
        self.incident = incident
        self.silence_seconds = silence_seconds
        self.snapshot_timeout = snapshot_timeout
        self.connected = False
        self.last_message = None
        self.counters = Counter()
        self.restart = asyncio.Event()
        self.ever_connected = False
        self.resolved_conditions = set()

    def invalidate(self, reason):
        for book in self.books.values():
            book.invalidate("market_resolved" if book.condition_id in self.resolved_conditions else reason)

    def request_restart(self, reason):
        self.invalidate(reason)
        self.restart.set()

    def handle(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("Event must be an object")
        kind = payload.get("event_type")
        if kind in ("book", "price_change", "tick_size_change") and payload.get("market") in self.resolved_conditions:
            self.counters["resolved_market_messages"] += 1
            return
        if kind in ("last_trade_price", "best_bid_ask", "new_market"):
            self.counters["ignored_messages"] += 1
            return
        if kind == "market_resolved":
            self.resolved_conditions.add(payload["market"])
            for book in self.books.values():
                if payload.get("market") == book.condition_id:
                    book.invalidate("market_resolved")
            self.incident("market_resolved", {"condition_id": payload.get("market")})
            return
        if kind == "price_change":
            grouped = defaultdict(list)
            changes = payload["price_changes"]
            if not isinstance(changes, list) or not changes:
                raise ValueError("Missing price changes")
            for change in changes:
                grouped[str(change["asset_id"])].append(change)
            # Validate every affected book before any mutation, including multi-token events.
            staged = {}
            for token_id, rows in grouped.items():
                if token_id not in self.books:
                    self.counters["unfollowed_messages"] += 1
                    continue
                book = self.books[token_id]
                if book.condition_id != payload.get("market"):
                    raise ValueError("Delta condition mismatch")
                copy = deepcopy(book)
                copy.changes(rows, payload["timestamp"])
                staged[token_id] = copy
            self.books.update(staged)
        elif kind in ("book", "tick_size_change"):
            token_id = str(payload["asset_id"])
            if token_id not in self.books:
                self.counters["unfollowed_messages"] += 1
                return
            book = self.books[token_id]
            if kind == "book":
                if payload.get("timestamp") is None:
                    raise ValueError("WebSocket snapshot missing source timestamp")
                book.snapshot(payload, source="websocket")
            else:
                book._identity(payload)
                book._timestamp(payload)
                tick = number(payload["new_tick_size"])
                if not 0 < tick <= 1:
                    raise ValueError("Invalid tick size")
                book.tick_size = tick
                book.revision += 1  # a tick event does not refresh the depth age
        else:
            self.counters["unknown_messages"] += 1
            raise ValueError(f"Unknown market event: {kind}")
        self.counters[f"{kind}_events"] += 1

    async def session(self, ws):
        self.restart.clear()
        self.invalidate("awaiting_websocket_snapshot")
        await ws.send(json.dumps({"assets_ids": list(self.books), "type": "market",
                                  "initial_dump": True, "custom_feature_enabled": True}))
        self.connected = True
        self.ever_connected = True
        start = last_ping = time.monotonic()
        self.last_message = None
        self.incident("websocket_connected", {"tokens": len(self.books)})
        while not self.restart.is_set():
            now = time.monotonic()
            if now - last_ping >= 10:
                await ws.send("PING")
                last_ping = now
            if now - (self.last_message or start) > self.silence_seconds:
                self.counters["interruptions"] += 1
                raise TimeoutError("Public market feed silent")
            missing = [b.token_id for b in self.books.values()
                       if not b.ws_ready and b.invalid_reason != "market_resolved"]
            if missing and now - start > self.snapshot_timeout:
                raise TimeoutError(f"Missing initial snapshots for {len(missing)} tokens")
            try:
                frame = await asyncio.wait_for(ws.recv(), timeout=1)
            except asyncio.TimeoutError:
                continue
            self.last_message = time.monotonic()
            self.counters["messages_received"] += 1
            if frame == "PONG":
                self.counters["pongs"] += 1
                continue
            try:
                payload = json.loads(frame, parse_float=Decimal)
                events = payload if isinstance(payload, list) else [payload]
                for event in events:
                    self.handle(event)
            except (ValueError, KeyError, TypeError, ArithmeticError, AttributeError) as exc:
                self.counters["invalid_messages"] += 1
                self.incident("invalid_websocket_message", {"error": str(exc)})
                # No sequence numbers in this protocol: invalidate conservatively on unknown loss.
                self.request_restart("invalid_message")
                raise ValueError("Invalid market frame; fresh snapshots required") from exc

    async def run(self):
        attempt = 0
        while True:
            if not self.books:
                await asyncio.sleep(1)
                continue
            began = time.monotonic()
            try:
                async with connect(WS_MARKET, ping_interval=None, open_timeout=20,
                                   close_timeout=5, max_size=8*1024*1024, max_queue=64,
                                   proxy=None) as ws:
                    await self.session(ws)
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.counters["errors"] += 1
                self.incident("websocket_error", {"type": type(exc).__name__, "error": str(exc)})
                LOG.warning("WebSocket interrupted: %s", exc)
                attempt = 0 if time.monotonic() - began > 60 else min(attempt + 1, 6)
            finally:
                self.connected = False
                self.invalidate("disconnected")
                self.incident("websocket_disconnected", {})
            self.counters["reconnections"] += 1
            await asyncio.sleep(min(30, 2 ** attempt) + random.uniform(0, 1))
