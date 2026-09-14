"""Pure full-depth YES/NO mathematics. No I/O and no execution capability."""
from bisect import bisect_left
from dataclasses import dataclass
from decimal import Decimal, localcontext

from .models import Market, OrderBook, number

ZERO = Decimal(0)


def precision_for(values):
    """Enough coefficient digits for exact sums/products, including tiny edges."""
    values = [v for v in values if v]
    width = max((v.adjusted() for v in values), default=0) - min(
        (v.as_tuple().exponent for v in values), default=0) + 1
    return max(64, 2 * (width + len(str(len(values))) + 4))


@dataclass
class CostCurve:
    levels: list
    quantities: list
    costs: list

    @classmethod
    def build(cls, levels):
        quantities, costs = [], []
        q = c = ZERO
        for price, size in levels:
            q += size
            c += price * size
            quantities.append(q)
            costs.append(c)
        return cls(levels, quantities, costs)

    def cost(self, quantity):
        i = bisect_left(self.quantities, quantity)
        prior_q = self.quantities[i-1] if i else ZERO
        prior_c = self.costs[i-1] if i else ZERO
        return prior_c + (quantity-prior_q)*self.levels[i][0]

    def consumed(self, quantity):
        result = []
        for price, size in self.levels:
            take = min(size, quantity)
            if take:
                result.append({"price": price, "size": take})
            quantity -= take
            if not quantity:
                break
        return result


def sorted_asks(asks):
    result = []
    for price, size in asks.items():
        price, size = number(price), number(size)
        if price > 1 or size <= 0:
            raise ValueError("Invalid ask level")
        result.append((price, size))
    return sorted(result)


def optimize(yes_asks, no_asks, minimum_yes=None, minimum_no=None):
    """Maximise gross edge on the feasible interval; smallest q wins ties.

    Minima here are quantities in contracts. Unknown minima stay unknown.
    Every linear segment reaches its maximum at an endpoint. No search grid.
    """
    yes, no = sorted_asks(yes_asks), sorted_asks(no_asks)
    if not yes or not no:
        return None
    minima = [number(x) for x in (minimum_yes, minimum_no) if x is not None]
    values = [x for level in yes+no for x in level] + minima
    with localcontext() as ctx:
        ctx.prec = precision_for(values)
        yc, nc = CostCurve.build(yes), CostCurve.build(no)
        lower = max(minima, default=ZERO)
        upper = min(yc.quantities[-1], nc.quantities[-1])
        if lower > upper:
            return None
        candidates = sorted({lower, upper, *yc.quantities, *nc.quantities})
        best = None
        for q in candidates:
            if q <= 0 or not lower <= q <= upper:
                continue
            y, n = yc.cost(q), nc.cost(q)
            edge = q-y-n
            if edge > 0 and (best is None or edge > best[3]):
                best = q, y, n, edge
        if best is None:
            return None
        q, y, n, edge = best
        yl, nl = yc.consumed(q), nc.consumed(q)
        cost = y+n
        # Ratios may repeat. Monetary values remain exact; ratios use 50 digits.
        ctx.prec = 50
        return dict(optimal_quantity=q, yes_cost=y, no_cost=n, gross_cost=cost,
                    gross_payoff=q, gross_edge=edge,
                    gross_roi=edge/cost if cost else None,
                    yes_average_price=y/q, no_average_price=n/q,
                    marginal_yes_price=yl[-1]["price"], marginal_no_price=nl[-1]["price"],
                    yes_levels_consumed=len(yl), no_levels_consumed=len(nl),
                    yes_consumed_depth=yl, no_consumed_depth=nl)


def binary_market(market: Market):
    return (len(market.tokens) == 2 and bool(market.yes_no)
            and len({t.token_id for t in market.tokens}) == 2
            and all(t.market_id == market.market_id for t in market.tokens)
            and bool(market.condition_id))


def eligible_books(market: Market, books: dict[str, OrderBook], *, stale_ms=30000, monotonic=None):
    """Cheap validity/freshness guard, also used when only time has elapsed."""
    if not binary_market(market) or not market.followable:
        return None
    if market.status.casefold() in {"resolved", "closed"} or market.raw.get("resolved") is True:
        return None
    if str(market.resolution_metadata.get("umaResolutionStatus", "")).casefold() == "resolved":
        return None
    pair = []
    for label in ("YES", "NO"):
        token = market.yes_no[label]
        book = books.get(token.token_id)
        if (book is None or book.token_id != token.token_id or book.market_id != market.market_id
                or book.condition_id != market.condition_id or book.outcome.strip().upper() != label
                or not book.valid or not book.ws_ready or book.stale(stale_ms, monotonic)
                or not book.asks or book.last_exchange_update is None or book.invalid_reason is not None):
            return None
        pair.append(book)
    return pair


def evaluate(market: Market, books: dict[str, OrderBook], *, stale_ms=30000, monotonic=None):
    """Conservative eligibility then gross optimisation. No per-opportunity GET."""
    pair = eligible_books(market, books, stale_ms=stale_ms, monotonic=monotonic)
    if pair is None:
        return None
    yes, no = pair
    try:
        result = optimize(yes.asks, no.asks, yes.minimum_order_size, no.minimum_order_size)
    except (ValueError, TypeError, ArithmeticError):
        return None
    if result is None:
        return None
    # Phase 1 has no provenance/unit field for minimum_order_size. Gamma's
    # documented orderMinSize is USDC, so additionally enforce known notionals;
    # never substitute a missing book minimum with a fabricated share quantity.
    notional = market.raw.get("orderMinSize")
    try:
        notional = None if notional is None else number(notional)
    except (ValueError, TypeError):
        return None
    minimum_status = "KNOWN" if all(b.minimum_order_size is not None for b in pair) else "UNKNOWN"
    notionals_met = notional is None or min(result["yes_cost"], result["no_cost"]) >= notional
    # A false flag is direct evidence; category and missing schedules are not.
    fee_status = "NOT_APPLICABLE" if market.fee_metadata.get("feesEnabled") is False else "UNKNOWN"
    fees = ZERO if fee_status == "NOT_APPLICABLE" else None
    executable = (fees is not None and minimum_status == "KNOWN" and notionals_met)
    result.update(market_id=market.market_id, condition_id=market.condition_id,
        event_id=market.event_id, question=market.question,
        yes_token_id=yes.token_id, no_token_id=no.token_id,
        strategy_type="STRICT_ARBITRAGE", observation_status="EXECUTABLE" if executable else "THEORETICAL",
        fee_status=fee_status, fee_metadata=market.fee_metadata,
        estimated_fees=fees, net_edge=result["gross_edge"] if fees is not None else None,
        net_roi=result["gross_roi"] if fees is not None else None,
        minimum_order_size_yes=yes.minimum_order_size, minimum_order_size_no=no.minimum_order_size,
        minimum_constraint_status=minimum_status, gamma_minimum_notional=notional,
        minimum_notional_satisfied=notionals_met,
        book_age_yes_ms=yes.book_age_ms(monotonic), book_age_no_ms=no.book_age_ms(monotonic),
        yes_exchange_timestamp=yes.last_exchange_update, no_exchange_timestamp=no.last_exchange_update,
        yes_revision=yes.revision, no_revision=no.revision, execution_risk="MULTI_LEG_NON_ATOMIC")
    return result
