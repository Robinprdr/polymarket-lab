"""Exact public metadata and full-depth books. Times are UTC epoch milliseconds."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from decimal import Decimal, InvalidOperation
import json
import time


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def number(value) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValueError("Use decimal strings, integers or Decimal, never binary floats")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Invalid decimal") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("Expected finite nonnegative decimal")
    return result


def optional_number(value):
    return None if value is None else number(value)


def dumps(value) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


def array(value):
    result = json.loads(value, parse_float=Decimal) if isinstance(value, str) else value
    if not isinstance(result, list):
        raise ValueError("Expected array or JSON-encoded array")
    return result


@dataclass(frozen=True)
class Token:
    token_id: str
    market_id: str
    outcome: str


@dataclass
class Market:
    market_id: str
    condition_id: str | None
    event_id: str | None
    question: str | None
    slug: str | None
    active: bool | None
    closed: bool | None
    status: str
    end_date: str | None
    neg_risk: bool | None
    tick_size: Decimal | None
    minimum_order_size: Decimal | None
    fee_metadata: dict
    category: object
    resolution_metadata: dict
    tokens: list[Token]
    retrieved_at: int
    raw: dict

    @property
    def yes_no(self) -> dict[str, Token]:
        mapped = {t.outcome.strip().casefold(): t for t in self.tokens}
        return {k.upper(): mapped[k] for k in ("yes", "no")} if set(mapped) == {"yes", "no"} else {}

    @property
    def followable(self) -> bool:
        return (self.active is True and self.closed is False and bool(self.condition_id)
                and self.raw.get("enableOrderBook") is True
                and self.raw.get("acceptingOrders") is True and bool(self.tokens))


def parse_market(raw: dict, received: int | None = None) -> Market:
    market_id = str(raw["id"])
    outcomes, ids = array(raw.get("outcomes", [])), array(raw.get("clobTokenIds", []))
    if len(outcomes) != len(ids):
        raise ValueError("Outcome/token metadata lengths disagree")
    if any(not isinstance(x, str) or not x.strip() for x in outcomes):
        raise ValueError("Missing outcome label")
    if len({x.strip().casefold() for x in outcomes}) != len(outcomes):
        raise ValueError("Ambiguous duplicate outcome labels")
    if any(not isinstance(x, str) or not x.isdigit() for x in ids) or len(set(ids)) != len(ids):
        raise ValueError("Invalid or duplicate token IDs")
    for key in ("active", "closed", "negRisk"):
        if raw.get(key) is not None and not isinstance(raw[key], bool):
            raise ValueError(f"Invalid boolean: {key}")
    events = raw.get("events") or []
    event = events[0] if len(events) == 1 else {}
    # Labels and corresponding token metadata are paired, never positional YES inference.
    tokens = [Token(t, market_id, o.strip()) for t, o in zip(ids, outcomes, strict=True)]
    return Market(
        market_id, raw.get("conditionId"), str(event["id"]) if "id" in event else None,
        raw.get("question"), raw.get("slug"), raw.get("active"), raw.get("closed"),
        "closed" if raw.get("closed") is True else "active" if raw.get("active") is True else "unknown",
        raw.get("endDate"), raw.get("negRisk"), optional_number(raw.get("orderPriceMinTickSize")),
        optional_number(raw.get("orderMinSize")),
        {k: v for k, v in raw.items() if "fee" in k.casefold()},
        raw.get("category") or raw.get("tags") or event.get("category") or event.get("tags"),
        {k: raw.get(k) for k in ("description", "resolutionSource", "umaResolutionStatus",
                               "questionID", "groupItemTitle", "negRiskMarketID", "negRiskRequestID")},
        tokens, now_ms() if received is None else received, raw,
    )


def parse_levels(rows: list) -> dict[Decimal, Decimal]:
    if not isinstance(rows, list):
        raise ValueError("Levels must be an array")
    levels, seen = {}, set()
    for row in rows:
        price, size = number(row["price"]), number(row["size"])
        if price > 1 or price in seen:
            raise ValueError("Invalid/duplicate price level")
        seen.add(price)
        if size:
            levels[price] = size
    return levels


@dataclass
class OrderBook:
    token_id: str
    market_id: str
    outcome: str
    condition_id: str | None = None
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_exchange_update: int | None = None
    last_local_update: int | None = None
    tick_size: Decimal | None = None
    minimum_order_size: Decimal | None = None
    valid: bool = False
    ws_ready: bool = False
    invalid_reason: str | None = "not_initialized"
    revision: int = 0
    source: str | None = None
    _monotonic_update: float | None = None

    def invalidate(self, reason: str):
        self.valid, self.ws_ready, self.invalid_reason = False, False, reason
        self.revision += 1

    def book_age_ms(self, monotonic: float | None = None) -> float | None:
        if self._monotonic_update is None:
            return None
        elapsed = max(0., ((time.monotonic() if monotonic is None else monotonic) - self._monotonic_update) * 1000)
        source_age_at_receive = (max(0, self.last_local_update - self.last_exchange_update)
                                 if self.last_exchange_update is not None and self.last_local_update is not None else 0)
        return elapsed + source_age_at_receive

    def stale(self, threshold_ms: float, monotonic: float | None = None) -> bool:
        age = self.book_age_ms(monotonic)
        return not self.valid or age is None or age > threshold_ms

    def _timestamp(self, payload, *, reset_ordering=False):
        value = payload.get("timestamp")
        if value is None:
            return None
        stamp = int(value)
        if stamp < 0 or str(stamp) != str(value):
            raise ValueError("Invalid source timestamp")
        if stamp > now_ms() + 5000:
            raise ValueError("Source timestamp too far in the future (clock skew)")
        if not reset_ordering and self.last_exchange_update is not None and stamp < self.last_exchange_update:
            raise ValueError("Out-of-order source timestamp")
        return stamp

    def _identity(self, payload):
        if str(payload["asset_id"]) != self.token_id or payload.get("market") != self.condition_id:
            raise ValueError("Book identity mismatch")

    def _touch(self, stamp, received, monotonic):
        self.last_exchange_update = stamp
        self.last_local_update = now_ms() if received is None else received
        self._monotonic_update = time.monotonic() if monotonic is None else monotonic
        self.revision += 1

    def snapshot(self, payload, *, source="rest", received=None, monotonic=None, reset_ordering=False):
        self._identity(payload)
        stamp = self._timestamp(payload, reset_ordering=reset_ordering)
        bids, asks = parse_levels(payload["bids"]), parse_levels(payload["asks"])
        tick = optional_number(payload.get("tick_size", self.tick_size))
        minimum = optional_number(payload.get("min_order_size", self.minimum_order_size))
        self.bids, self.asks, self.tick_size, self.minimum_order_size = bids, asks, tick, minimum
        self.valid, self.ws_ready, self.invalid_reason = True, source == "websocket", None
        self.source = source
        self._touch(stamp, received, monotonic)

    def changes(self, rows, stamp, *, received=None, monotonic=None):
        if not self.ws_ready or not self.valid:
            raise ValueError("Delta before WebSocket snapshot")
        timestamp = self._timestamp({"timestamp": stamp})
        parsed = []
        for row in rows:
            if str(row["asset_id"]) != self.token_id or row["side"] not in ("BUY", "SELL"):
                raise ValueError("Invalid delta identity/side")
            price, size = number(row["price"]), number(row["size"])
            if price > 1:
                raise ValueError("Price exceeds 1")
            parsed.append((row["side"], price, size))
        for side, price, size in parsed:
            levels = self.bids if side == "BUY" else self.asks
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size  # absolute aggregate size, not an increment
        self.source = "websocket"
        self._touch(timestamp, received, monotonic)

    def depth(self, side: str, limit: int | None = None) -> list[dict[str, str]]:
        if side not in ("bids", "asks"):
            raise ValueError("Unknown book side")
        pairs = sorted(getattr(self, side).items(), reverse=side == "bids")
        return [{"price": str(p), "size": str(s)} for p, s in pairs[:limit]]

    def export(self, limit=None):
        return {"token_id": self.token_id, "market_id": self.market_id, "outcome": self.outcome,
                "bids": self.depth("bids", limit), "asks": self.depth("asks", limit),
                "last_exchange_update": self.last_exchange_update,
                "last_local_update": self.last_local_update, "book_age_ms": self.book_age_ms(),
                "tick_size": self.tick_size, "minimum_order_size": self.minimum_order_size,
                "valid": self.valid, "ws_ready": self.ws_ready, "invalid_reason": self.invalid_reason,
                "source": self.source, "full_depth": limit is None}
