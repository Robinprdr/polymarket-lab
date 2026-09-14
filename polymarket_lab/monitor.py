"""CLI observer: python -m polymarket_lab.monitor. Public data only."""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
import logging
from pathlib import Path
import signal
import time
from .feed import Feed
from .models import OrderBook, parse_market, now_ms, dumps
from .public_api import PublicAPI
from .storage import Storage
from .opportunities import ComplementScanner

LOG = logging.getLogger(__name__)


class Monitor:
    def __init__(self, args, *, api=None):
        self.args = args
        self.api = api or PublicAPI()
        self.store = Storage(args.database)
        self.books = {}
        self.markets = {}
        self.counters = Counter()
        self.discovery_complete = False
        self.discovery_status = "not_started"
        self.discovery_at = None
        self.discovery_seen = 0
        self.feed = Feed(self.books, self.incident)
        self.reconciliation = {}
        self.stopping = False
        self.scanner = ComplementScanner(self.store, stale_ms=args.stale_seconds*1000) if args.scan_complements else None
        if self.scanner is not None:
            self.feed.on_books_changed = self.scan_changed

    def scan_changed(self, token_ids):
        if self.scanner is None:
            return
        touched = {self.books[t].market_id for t in token_ids if t in self.books}
        available = self.feed.connected and not self.feed.restart.is_set() and not self.stopping
        for market_id in sorted(touched):
            market = self.markets.get(market_id)
            if market is not None:
                self.scanner.scan(market, self.books, available=available)

    def expire_opportunities(self):
        self.scanner.expire(self.markets, self.books,
            available=self.feed.connected and not self.feed.restart.is_set() and not self.stopping)

    def incident(self, kind, details):
        self.store.incident(kind, details)
        LOG.info("%s %s", kind, dumps(details))

    async def discover(self):
        found, cursor, cursors = {}, None, set()
        seen_ids = set()
        pages, invalid = 0, 0
        self.discovery_status = "running"
        self.discovery_complete = False
        try:
            while True:
                rows, next_cursor = await self.api.market_page(cursor)
                pages += 1
                for raw in rows:
                    try:
                        seen_ids.add(str(raw["id"]))
                        market = parse_market(raw)
                        found[market.market_id] = market
                    except (ValueError, KeyError, TypeError, AttributeError) as exc:
                        invalid += 1
                        self.counters["invalid_metadata"] += 1
                        self.counters["errors"] += 1
                        self.incident("invalid_metadata", {"error": str(exc)})
                self.discovery_seen = len(seen_ids)
                if not next_cursor:
                    self.discovery_complete = invalid == 0
                    self.discovery_status = "complete" if not invalid else "invalid_metadata"
                    break
                if next_cursor in cursors:
                    raise ValueError("Repeated Gamma cursor; discovery incomplete")
                cursors.add(next_cursor)
                if self.args.discovery_pages and pages >= self.args.discovery_pages:
                    self.discovery_status = "capped"
                    break
                cursor = next_cursor
        except Exception as exc:
            self.discovery_status = "error"
            self.counters["errors"] += 1
            self.incident("discovery_error", {"error": str(exc), "pages": pages})
        self.discovery_at = now_ms()
        if found or self.discovery_complete:
            self.store.registry(found.values(), complete=self.discovery_complete)
            # Partial scans do not silently evict a previously observed registry.
            if self.discovery_complete:
                self.markets = found
            else:
                self.markets.update(found)
            # Subscribe only metadata validated in this scan (never old cached active flags).
            selected = sorted((m for m in found.values() if m.followable), key=lambda m: m.market_id)
            selected = selected[:self.args.max_markets]
            wanted = {t.token_id: (m, t) for m in selected for t in m.tokens}
            if set(wanted) != set(self.books):
                self.feed.request_restart("subscription_set_changed")
                self.books.clear()
                for tid, (m, t) in wanted.items():
                    self.books[tid] = OrderBook(tid, m.market_id, t.outcome, m.condition_id,
                                               tick_size=m.tick_size, minimum_order_size=m.minimum_order_size)
                self.reconciliation.clear()
                # Only bootstrap before the feed runs. WS initial snapshots are the streaming barrier.
                if not self.feed.ever_connected and not self.feed.connected:
                    await self.bootstrap()
            else:
                for tid, (m, t) in wanted.items():
                    book = self.books[tid]
                    if (book.market_id, book.condition_id, book.outcome) != (m.market_id, m.condition_id, t.outcome):
                        self.feed.request_restart("token_metadata_changed")
                        self.books[tid] = OrderBook(tid, m.market_id, t.outcome, m.condition_id,
                            tick_size=m.tick_size, minimum_order_size=m.minimum_order_size)
                    else:
                        book.minimum_order_size = m.minimum_order_size
        self.incident("discovery_finished", {"seen": len(seen_ids), "normalized": len(found),
                                              "pages": pages, "status": self.discovery_status})
        if self.scanner is not None:
            self.expire_opportunities()
            self.scan_changed(self.books)

    async def bootstrap(self):
        async def one(token_id, book):
            try:
                payload = await self.api.book(token_id)
                # Do not mutate a replaced object after a concurrent subscription change.
                if self.books.get(token_id) is not book or self.feed.connected:
                    return
                book.snapshot(payload, source="rest")
                self.store.snapshot(book, "bootstrap", full=True)
                self.counters["rest_bootstraps"] += 1
            except Exception as exc:
                self.counters["errors"] += 1
                self.incident("bootstrap_error", {"token_id": token_id, "error": str(exc)})
        await asyncio.gather(*(one(t, b) for t, b in list(self.books.items())))

    async def reconcile_one(self, token_id):
        before = self.books[token_id]
        version = before.revision
        result = {"timestamp": now_ms(), "status": "pending"}
        try:
            payload = await self.api.book(token_id)
            candidate = OrderBook(token_id, before.market_id, before.outcome, before.condition_id)
            candidate.snapshot(payload)
            current = self.books.get(token_id)
            if current is not before or current.revision != version:
                result["status"] = "inconclusive_concurrent_update"
                self.counters["reconciliation_inconclusive"] += 1
            elif not current.valid or not current.ws_ready:
                result["status"] = "awaiting_websocket_snapshot"
            elif candidate.last_exchange_update is None or current.last_exchange_update is None:
                result["status"] = "inconclusive_missing_timestamp"
            elif candidate.last_exchange_update < current.last_exchange_update:
                result["status"] = "inconclusive_older_rest"
                self.counters["reconciliation_inconclusive"] += 1
            elif (current.bids, current.asks) != (candidate.bids, candidate.asks):
                result["status"] = "divergence_resubscribe"
                self.counters["divergences"] += 1
                self.counters["errors"] += 1
                self.store.snapshot(current, "divergence_ws", full=True)
                self.store.snapshot(candidate, "divergence_rest", full=True)
                self.incident("reconciliation_divergence", {"token_id": token_id,
                    "ws_timestamp": current.last_exchange_update,
                    "rest_timestamp": candidate.last_exchange_update})
                self.feed.request_restart("rest_divergence")
            else:
                result["status"] = "matched"
                self.counters["reconciliation_matches"] += 1
        except Exception as exc:
            result.update(status="error", error=str(exc))
            self.counters["errors"] += 1
            self.incident("reconciliation_error", {"token_id": token_id, "error": str(exc)})
        self.reconciliation[token_id] = result
        return result

    async def reconcile(self):
        for token_id in list(self.books):
            if token_id in self.books:
                await self.reconcile_one(token_id)

    def health(self):
        now = now_ms()
        ages = [b.book_age_ms() for b in self.books.values() if b.book_age_ms() is not None]
        fresh = [b for b in self.books.values() if self.feed.connected and b.ws_ready
                 and not b.stale(self.args.stale_seconds*1000)]
        followed = len({b.market_id for b in self.books.values()})
        all_counters = self.counters + self.feed.counters
        recon_statuses = Counter(v["status"] for v in self.reconciliation.values())
        discovery_age = None if self.discovery_at is None else now-self.discovery_at
        complete = (self.discovery_complete and discovery_age is not None
                    and discovery_age <= 2*self.args.discovery_seconds*1000
                    and followed == sum(m.followable for m in self.markets.values())
                    and bool(self.books) and len(fresh) == len(self.books))
        return {
            "timestamp": now, "markets_discovered": len(self.markets),
            "markets_seen_last_scan": self.discovery_seen,
            "tokens_discovered": len({t.token_id for m in self.markets.values() for t in m.tokens}),
            "markets_followed": followed, "tokens_followed": len(self.books),
            "tokens_fresh": len(fresh), "tokens_stale_or_unready": len(self.books)-len(fresh),
            "discovery_complete": self.discovery_complete, "discovery_status": self.discovery_status,
            "discovery_age_ms": discovery_age, "coverage_complete": complete,
            "websocket_connected": self.feed.connected,
            "last_message_age_ms": None if self.feed.last_message is None else
                round((time.monotonic()-self.feed.last_message)*1000, 1),
            "max_book_age_ms": max(ages, default=None),
            "books": {tid: {"last_update": b.last_exchange_update,
                            "local_receive_time": b.last_local_update,
                            "book_age_ms": b.book_age_ms(),
                            "exchange_age_ms": None if b.last_exchange_update is None else now-b.last_exchange_update,
                            "valid": b.valid, "ws_ready": b.ws_ready,
                            "invalid_reason": b.invalid_reason,
                            "stale": b.stale(self.args.stale_seconds*1000)}
                      for tid, b in self.books.items()},
            "rest_reconciliation_status": dict(recon_statuses) or {"not_started": len(self.books)},
            "reconciliation_by_token": self.reconciliation,
            "rest_requests": self.api.requests, "rest_retries": self.api.retries,
            "messages_received": all_counters["messages_received"],
            "reconnections": all_counters["reconnections"], "errors": all_counters["errors"],
            "invalid_messages": all_counters["invalid_messages"],
            "dropped_messages": None,  # no server sequence, loss count cannot be established
            "counters": dict(all_counters), "database_bytes": self.store.size_bytes(),
            "stopping": self.stopping,
            "complement_summary": self.scanner.summary() if self.scanner else None,
        }

    def emit_health(self):
        metrics = self.health()
        self.store.health(metrics)
        print(" | ".join([
            f"markets discovered={metrics['markets_discovered']} followed={metrics['markets_followed']}",
            f"tokens={metrics['tokens_followed']} fresh={metrics['tokens_fresh']}",
            f"WS={'connected' if metrics['websocket_connected'] else 'OFFLINE'}",
            f"messages={metrics['messages_received']} age_ms={metrics['last_message_age_ms']}",
            f"coverage={'complete' if metrics['coverage_complete'] else 'INCOMPLETE'}",
            f"REST={metrics['rest_reconciliation_status']}",
            f"DB={metrics['database_bytes']}B errors={metrics['errors']}"
        ]), flush=True)

    def snapshots(self):
        for book in self.books.values():
            if book.last_local_update is not None:
                self.store.snapshot(book)
        self.store.prune()

    async def periodic(self, interval, action, *, immediate=False):
        if not immediate:
            await asyncio.sleep(interval)
        while True:
            result = action()
            if hasattr(result, "__await__"):
                await result
            await asyncio.sleep(interval)

    async def run(self):
        # Health runs even during discovery failures/slow startup.
        tasks = [asyncio.create_task(self.periodic(self.args.health_seconds, self.emit_health, immediate=True))]
        try:
            await self.discover()
            tasks.extend([
                asyncio.create_task(self.feed.run()),
                asyncio.create_task(self.periodic(self.args.discovery_seconds, self.discover)),
                asyncio.create_task(self.periodic(self.args.reconcile_seconds, self.reconcile)),
                asyncio.create_task(self.periodic(self.args.snapshot_seconds, self.snapshots)),
            ])
            if self.scanner is not None:
                tasks.append(asyncio.create_task(self.periodic(0.1, self.expire_opportunities)))
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.stopping = True
            self.feed.connected = False
            self.feed.invalidate("stopped")
            if self.scanner is not None:
                self.scanner.close_all("SESSION_STOPPED")
                print("COMPLEMENT SUMMARY " + dumps(self.scanner.summary()), flush=True)
            self.emit_health()


def parser():
    result = argparse.ArgumentParser(description="READ ONLY — NO TRADING CAPABILITY")
    result.add_argument("--database", type=Path, default=Path("data/polymarket.sqlite3"))
    result.add_argument("--scan-complements", action="store_true", help="Observe full-depth YES/NO gross edges; READ ONLY")
    result.add_argument("--max-markets", type=int, default=25, help="1..100 markets; coverage cap is always visible")
    result.add_argument("--discovery-pages", type=int, default=0, help="0: all keyset pages; >0: explicit incomplete sample")
    result.add_argument("--duration", type=float, default=0, help="Seconds including startup; 0 runs until Ctrl+C")
    result.add_argument("--stale-seconds", type=float, default=30)
    result.add_argument("--health-seconds", type=float, default=10)
    result.add_argument("--reconcile-seconds", type=float, default=60)
    result.add_argument("--snapshot-seconds", type=float, default=60)
    result.add_argument("--discovery-seconds", type=float, default=300)
    return result


def validate(args):
    if not 1 <= args.max_markets <= 100 or args.discovery_pages < 0 or args.duration < 0:
        raise ValueError("Invalid market/page/duration limit")
    for name in ("stale_seconds", "health_seconds", "reconcile_seconds", "snapshot_seconds", "discovery_seconds"):
        if not 0 < getattr(args, name) < float("inf"):
            raise ValueError(f"{name} must be positive and finite")
    if not args.duration < float("inf"):
        raise ValueError("duration must be finite")


async def main_async(args):
    monitor = Monitor(args)
    task = asyncio.create_task(monitor.run())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except NotImplementedError:
            pass
    try:
        if args.duration:
            try:
                await asyncio.wait_for(task, timeout=args.duration)
            except asyncio.TimeoutError:
                pass
        else:
            await task
    except asyncio.CancelledError:
        pass
    finally:
        await monitor.api.close()
        monitor.store.close()
    # A bounded observation without any WS baseline is not a successful live smoke test.
    return 0 if monitor.feed.counters["book_events"] else 2


def main():
    args = parser().parse_args()
    try:
        validate(args)
    except ValueError as exc:
        parser().error(str(exc))
    args.database.parent.mkdir(parents=True, exist_ok=True)
    from logging.handlers import RotatingFileHandler
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        handlers=[RotatingFileHandler(args.database.parent / "monitor.log",
                                  maxBytes=2_000_000, backupCount=3), logging.StreamHandler()])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
