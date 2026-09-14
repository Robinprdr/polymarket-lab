from copy import deepcopy
from decimal import Decimal, localcontext
from fractions import Fraction
import random

import pytest

from polymarket_lab.complement import optimize, evaluate
from polymarket_lab.models import OrderBook, parse_market, now_ms, Token

D = Decimal


def asks(*levels):
    return {D(price): D(size) for price, size in levels}


@pytest.fixture
def pair(market_raw):
    raw = deepcopy(market_raw)
    raw["orderMinSize"] = 1
    market = parse_market(raw)
    stamp = now_ms()
    books = {}
    for label, price in (("YES", "0.45"), ("NO", "0.50")):
        token = market.yes_no[label]
        book = OrderBook(token.token_id, market.market_id, token.outcome, market.condition_id)
        book.snapshot(dict(asset_id=token.token_id, market=market.condition_id, timestamp=str(stamp),
            bids=[], asks=[dict(price=price, size="100")], min_order_size="1"),
            source="websocket", received=stamp, monotonic=10)
        books[token.token_id] = book
    return market, books


def test_no_arbitrage():
    assert optimize(asks(("0.52", "100")), asks(("0.49", "100"))) is None


def test_single_level_exact():
    o = optimize(asks(("0.45", "100")), asks(("0.50", "100")))
    assert (o["optimal_quantity"], o["gross_cost"], o["gross_payoff"], o["gross_edge"]) == (D(100), D(95), D(100), D(5))
    assert o["yes_cost"] == D(45) and o["no_cost"] == D(50)
    assert o["yes_average_price"] == o["marginal_yes_price"] == D(".45")
    assert o["yes_levels_consumed"] == o["no_levels_consumed"] == 1
    assert o["yes_consumed_depth"] == [{"price": D(".45"), "size": D(100)}]


def test_asymmetric_full_depth_and_sort():
    o = optimize(asks((".48", "100"), (".45", "50")), asks((".51", "200"), (".49", "20")))
    assert o["optimal_quantity"] == 150
    assert (o["yes_cost"], o["no_cost"], o["gross_cost"], o["gross_edge"]) == tuple(map(D, ["70.50", "76.10", "146.60", "3.40"]))
    assert o["yes_average_price"] == D(".47")
    assert o["no_average_price"] == D("0.50733333333333333333333333333333333333333333333333")
    assert o["gross_roi"] == D("0.023192360163710777626193724420190995907230559345157")
    assert o["marginal_yes_price"] == D(".48") and o["marginal_no_price"] == D(".51")
    assert o["yes_levels_consumed"] == o["no_levels_consumed"] == 2
    assert o["no_consumed_depth"][-1]["size"] == 130


def test_first_book_exhausted():
    o = optimize(asks((".45", "7")), asks((".50", "100")))
    assert o["optimal_quantity"] == 7 and o["gross_edge"] == D(".35")


def test_deeper_levels_destroy_edge_stop_at_interior_breakpoint():
    o = optimize(asks((".4", "10"), (".8", "100")), asks((".5", "200")))
    assert o["optimal_quantity"] == 10 and o["gross_edge"] == 1


def test_marginal_profit_flat_chooses_smallest_quantity():
    o = optimize(asks((".4", "10"), (".5", "100")), asks((".5", "200")))
    assert o["optimal_quantity"] == 10


def test_minimum_feasible_boundary_not_depth_breakpoint():
    y, n = asks((".4", "10"), (".6", "100")), asks((".5", "200"))
    o = optimize(y, n, D(15), D(12))
    assert o["optimal_quantity"] == 15 and o["gross_edge"] == D(".5")
    assert optimize(y, n, D(20)) is None
    assert optimize(y, n, D(201)) is None


def test_many_decimals_and_low_ambient_precision():
    with localcontext() as ctx:
        ctx.prec = 6
        o = optimize(asks(("0.49999999999999999999999999999999999", "1")), asks((".5", "1")))
    assert o["gross_edge"] == D("1e-35")
    assert o["gross_cost"] == D("0.99999999999999999999999999999999999")


@pytest.mark.parametrize("price,size", [(0.4, "2"), (".4", "0"), (".4", "-1"), ("NaN", "2"), ("1.1", "2")])
def test_invalid_depth_rejected(price, size):
    with pytest.raises((ValueError, ArithmeticError)):
        optimize({price: size}, asks((".5", "2")))


@pytest.mark.parametrize("mutate", [
    lambda m, b: setattr(b["100"], "valid", False),
    lambda m, b: setattr(b["100"], "ws_ready", False),
    lambda m, b: setattr(b["100"], "condition_id", "other"),
    lambda m, b: setattr(b["100"], "market_id", "other"),
    lambda m, b: setattr(b["100"], "token_id", "other"),
    lambda m, b: setattr(b["100"], "outcome", "No"),
    lambda m, b: setattr(b["100"], "asks", {}),
    lambda m, b: b.pop("100"),
    lambda m, b: setattr(m, "closed", True),
    lambda m, b: setattr(m, "status", "resolved"),
    lambda m, b: m.resolution_metadata.update(umaResolutionStatus="resolved"),
    lambda m, b: setattr(m, "tokens", [Token("100", "7", "Yes"), Token("200", "7", "YES")]),
    lambda m, b: m.tokens.append(Token("300", "7", "Yes")),
])
def test_invalid_market_or_pair_never_detected(pair, mutate):
    m, b = pair
    mutate(m, b)
    assert evaluate(m, b, monotonic=10) is None


def test_stale_and_reconnect(pair):
    m, b = pair
    assert evaluate(m, b, monotonic=41) is None
    b["100"].invalidate("disconnected")
    assert evaluate(m, b, monotonic=10) is None


def test_unknown_fees_stay_null_and_theoretical(pair):
    m, b = pair
    o = evaluate(m, b, monotonic=10)
    assert o["fee_status"] == "UNKNOWN" and o["observation_status"] == "THEORETICAL"
    assert o["estimated_fees"] is o["net_edge"] is o["net_roi"] is None
    assert o["execution_risk"] == "MULTI_LEG_NON_ATOMIC"


def test_explicit_no_fees_and_known_constraints(pair):
    m, b = pair
    m.fee_metadata = {"feesEnabled": False}
    o = evaluate(m, b, monotonic=10)
    assert o["observation_status"] == "EXECUTABLE"
    assert o["fee_status"] == "NOT_APPLICABLE" and o["estimated_fees"] == 0
    assert o["net_edge"] == o["gross_edge"] == 5


def test_unknown_minimum_and_insufficient_minimum(pair):
    m, b = pair
    m.fee_metadata = {"feesEnabled": False}
    b["100"].minimum_order_size = None
    assert evaluate(m, b, monotonic=10)["observation_status"] == "THEORETICAL"
    b["100"].minimum_order_size = D(101)
    assert evaluate(m, b, monotonic=10) is None


def test_gamma_notional_not_confused_with_share_quantity(pair):
    m, b = pair
    m.fee_metadata = {"feesEnabled": False}
    m.raw["orderMinSize"] = D(80)
    o = evaluate(m, b, monotonic=10)
    assert not o["minimum_notional_satisfied"] and o["observation_status"] == "THEORETICAL"


def test_missing_or_enabled_fee_metadata_not_assumed_free(pair):
    m, b = pair
    for fees in ({}, {"feesEnabled": "false"}, {"feesEnabled": True, "feeSchedule": {"rate": "0.04"}}):
        m.fee_metadata = fees
        assert evaluate(m, b, monotonic=10)["net_edge"] is None


def test_zero_cost_has_undefined_roi():
    o = optimize(asks(("0", "2")), asks(("0", "3")))
    assert o["gross_edge"] == 2 and o["gross_roi"] is None


def test_breakpoints_match_independent_fraction_oracle():
    rng = random.Random(123)
    for _ in range(50):
        sides = [{D(p)/100: D(rng.randint(1, 9)) for p in rng.sample(range(1, 99), 4)} for _ in range(2)]
        limit = int(min(sum(s.values()) for s in sides))
        minimum = rng.randint(1, limit)
        def cost(side, q):
            result = Fraction(0)
            for p, size in sorted(side.items()):
                take = min(q, int(size))
                result += Fraction(p)*take
                q -= take
            return result
        profits = [(q, Fraction(q)-sum(cost(s, q) for s in sides)) for q in range(minimum, limit+1)]
        q, edge = max(profits, key=lambda v: (v[1], -v[0]))
        actual = optimize(*sides, D(minimum))
        if edge <= 0:
            assert actual is None
        else:
            assert actual["optimal_quantity"] == q
            assert Fraction(actual["gross_edge"]) == edge
