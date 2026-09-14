"""Observation episodes only. No orders, fills, or atomic-execution claims."""
from decimal import Decimal, localcontext
import time
from uuid import uuid4

from .complement import evaluate, binary_market, eligible_books
from .models import now_ms, dumps


class ComplementScanner:
    def __init__(self, store, *, stale_ms=30000, output=print):
        self.store, self.stale_ms, self.output = store, stale_ms, output
        self.session_id = str(uuid4())
        self.open = {}
        self.signatures = {}
        self.started = {}
        self.seen_markets, self.binary_markets = set(), set()
        self.episodes = 0
        self.known = self.unknown = 0
        self.max_edge = self.max_quantity = Decimal(0)
        self.max_roi = None
        self.durations = []
        self.store.censor_open_opportunities(now_ms())

    def scan(self, market, books, *, available=True, timestamp=None, mono_ns=None):
        timestamp = now_ms() if timestamp is None else timestamp
        mono_ns = time.monotonic_ns() if mono_ns is None else mono_ns
        key = (market.market_id, market.condition_id, "STRICT_ARBITRAGE")
        self.seen_markets.add(market.market_id)
        if binary_market(market):
            self.binary_markets.add(market.market_id)
        observation = evaluate(market, books, stale_ms=self.stale_ms, monotonic=mono_ns/1_000_000_000) if available else None
        if observation is None:
            gap = not available or eligible_books(market, books, stale_ms=self.stale_ms, monotonic=mono_ns/1_000_000_000) is None
            reason = "NOT_OBSERVABLE" if gap else "NO_VALID_POSITIVE_EDGE"
            self.close(key, reason, timestamp=timestamp, censored=gap)
            return
        signature = dumps({k: v for k, v in observation.items() if not k.startswith("book_age_")})
        if self.signatures.get(key) == signature:
            return  # repeated messages/timers do not inflate counts or spam the console
        episode = self.open.get(key)
        new = episode is None
        old_best = None if new else episode["best_gross_edge_seen"]
        old_status = None if new else episode["observation_status"]
        if new:
            self.started[key] = mono_ns
            episode = dict(id=str(uuid4()), session_id=self.session_id,
                first_seen_at=timestamp, observation_count=0, closed_at=None, close_reason=None,
                best_gross_edge_seen=observation["gross_edge"], best_net_edge_seen=observation["net_edge"],
                best_quantity_seen=observation["optimal_quantity"], best_roi_seen=observation["gross_roi"],
                censored=False, survival_horizons_ms=[50, 100, 250, 500],
                survival_status="NOT_MEASURED", fee_statuses_seen=[])
            self.open[key] = episode
            self.episodes += 1
        episode.update(observation)
        episode["last_seen_at"] = timestamp
        episode["duration_ms"] = max(0, (mono_ns-self.started[key])//1_000_000)
        episode["observation_count"] += 1
        if observation["gross_edge"] > episode["best_gross_edge_seen"]:
            episode["best_gross_edge_seen"] = observation["gross_edge"]
            episode["best_quantity_seen"] = observation["optimal_quantity"]
        for target, value in (("best_net_edge_seen", observation["net_edge"]), ("best_roi_seen", observation["gross_roi"])):
            if value is not None and (episode[target] is None or value > episode[target]):
                episode[target] = value
        status = observation["fee_status"]
        if status not in episode["fee_statuses_seen"]:
            episode["fee_statuses_seen"].append(status)
            if status == "UNKNOWN":
                self.unknown += 1
            else:
                self.known += 1
        self.max_edge = max(self.max_edge, observation["gross_edge"])
        self.max_quantity = max(self.max_quantity, observation["optimal_quantity"])
        if observation["gross_roi"] is not None:
            self.max_roi = observation["gross_roi"] if self.max_roi is None else max(self.max_roi, observation["gross_roi"])
        self.store.opportunity(episode, sample=True)
        self.signatures[key] = signature
        # No numeric significance threshold: each strict best is meaningful; cap
        # update output to one per second per episode, while storing every sample.
        report_best = old_best is not None and observation["gross_edge"] > old_best
        if new or old_status != observation["observation_status"] or (report_best and mono_ns-episode.get("last_print_ns", 0) >= 1_000_000_000):
            self.output(self.format(observation, "OPEN" if new else "UPDATE"))
            episode["last_print_ns"] = mono_ns

    @staticmethod
    def format(o, event):
        with localcontext() as ctx:
            ctx.prec = 64
            roi = "UNDEFINED" if o["gross_roi"] is None else f"{o['gross_roi']*100:.4f}%"
        net = "UNKNOWN" if o["net_edge"] is None else str(o["net_edge"])
        return (f"COMPLEMENT {event} market={o['market_id']} q={o['optimal_quantity']} "
                f"gross_cost={o['gross_cost']} gross_edge={o['gross_edge']} gross_roi={roi} "
                f"net_edge={net} status={o['observation_status']} "
                f"age_yes_ms={o['book_age_yes_ms']:.0f} age_no_ms={o['book_age_no_ms']:.0f} "
                "execution_risk=MULTI_LEG_NON_ATOMIC")

    def close(self, key, reason, *, timestamp=None, censored=False):
        episode = self.open.pop(key, None)
        if episode is None:
            return
        episode.update(closed_at=now_ms() if timestamp is None else timestamp,
                       close_reason=reason, censored=censored)
        self.store.opportunity(episode)
        self.durations.append(episode["duration_ms"])
        self.signatures.pop(key, None)
        self.started.pop(key, None)
        self.output(f"COMPLEMENT CLOSE market={episode['market_id']} duration_ms={episode['duration_ms']} reason={reason}")

    def close_all(self, reason):
        for key in list(self.open):
            self.close(key, reason, censored=True)

    def expire(self, markets, books, *, available=True):
        """Close observations when time alone makes a leg stale (no WS needed)."""
        for key in list(self.open):
            market = markets.get(key[0])
            if market is None or market.condition_id != key[1]:
                self.close(key, "MARKET_REMOVED", censored=True)
            elif not available or eligible_books(market, books, stale_ms=self.stale_ms) is None:
                self.close(key, "OBSERVATION_GAP", censored=True)

    def summary(self):
        durations = sorted(self.durations)
        middle = len(durations)//2
        median = None
        if durations:
            median = durations[middle] if len(durations)%2 else Decimal(durations[middle-1]+durations[middle])/2
        return dict(markets_scanned=len(self.seen_markets), valid_binary_markets=len(self.binary_markets),
            opportunity_episodes=self.episodes, currently_open=len(self.open),
            max_gross_edge=self.max_edge, max_gross_roi=self.max_roi, max_quantity=self.max_quantity,
            median_duration_ms=median, fees_known_count=self.known, fees_unknown_count=self.unknown)
