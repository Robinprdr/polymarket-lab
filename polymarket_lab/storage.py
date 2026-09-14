"""Single-writer SQLite storage; decimals are serialized as text, never REAL."""
from dataclasses import asdict
from pathlib import Path
import sqlite3
from .models import dumps, now_ms


class Storage:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS markets (
            market_id TEXT PRIMARY KEY, event_id TEXT, condition_id TEXT, question TEXT,
            slug TEXT, active INTEGER, status TEXT, end_date TEXT, neg_risk INTEGER,
            tick_size TEXT, minimum_order_size TEXT, fee_metadata TEXT, category TEXT,
            resolution_metadata TEXT, retrieved_at INTEGER NOT NULL, metadata_json TEXT NOT NULL,
            in_latest_discovery INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS tokens (
            token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL REFERENCES markets(market_id),
            outcome TEXT NOT NULL, retrieved_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS system_health (
            id INTEGER PRIMARY KEY, timestamp INTEGER NOT NULL, metrics_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS book_snapshots (
            id INTEGER PRIMARY KEY, token_id TEXT NOT NULL REFERENCES tokens(token_id),
            timestamp INTEGER NOT NULL, kind TEXT NOT NULL, full_depth INTEGER NOT NULL,
            snapshot_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY, timestamp INTEGER NOT NULL, kind TEXT NOT NULL,
            details_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS snapshots_time ON book_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS health_time ON system_health(timestamp);
        CREATE INDEX IF NOT EXISTS incidents_time ON incidents(timestamp);
        CREATE TABLE IF NOT EXISTS complement_opportunities (
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
            market_id TEXT NOT NULL, condition_id TEXT NOT NULL,
            strategy_type TEXT NOT NULL, observation_status TEXT NOT NULL,
            first_seen_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL,
            duration_ms INTEGER NOT NULL, observation_count INTEGER NOT NULL,
            closed_at INTEGER, close_reason TEXT,
            optimal_quantity TEXT NOT NULL, gross_cost TEXT NOT NULL,
            gross_edge TEXT NOT NULL, gross_roi TEXT,
            fee_status TEXT NOT NULL, estimated_fees TEXT, net_edge TEXT, net_roi TEXT,
            best_gross_edge_seen TEXT NOT NULL, best_net_edge_seen TEXT,
            best_quantity_seen TEXT NOT NULL, best_roi_seen TEXT,
            record_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS complement_observations (
            id INTEGER PRIMARY KEY, episode_id TEXT NOT NULL REFERENCES complement_opportunities(id),
            observed_at INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL,
            observation_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS complement_market ON complement_opportunities(market_id,condition_id);
        CREATE INDEX IF NOT EXISTS complement_samples ON complement_observations(episode_id,elapsed_ms);
        PRAGMA user_version=2;
        ''')

    def registry(self, markets, *, complete):
        with self.db:
            if complete:
                self.db.execute("UPDATE markets SET in_latest_discovery=0")
            for m in markets:
                values = (m.market_id, m.event_id, m.condition_id, m.question, m.slug, m.active,
                          m.status, m.end_date, m.neg_risk,
                          None if m.tick_size is None else str(m.tick_size),
                          None if m.minimum_order_size is None else str(m.minimum_order_size),
                          dumps(m.fee_metadata), dumps(m.category), dumps(m.resolution_metadata),
                          m.retrieved_at, dumps(asdict(m)))
                self.db.execute('''INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                    ON CONFLICT(market_id) DO UPDATE SET event_id=excluded.event_id,
                    condition_id=excluded.condition_id, question=excluded.question, slug=excluded.slug,
                    active=excluded.active, status=excluded.status, end_date=excluded.end_date,
                    neg_risk=excluded.neg_risk, tick_size=excluded.tick_size,
                    minimum_order_size=excluded.minimum_order_size, fee_metadata=excluded.fee_metadata,
                    category=excluded.category, resolution_metadata=excluded.resolution_metadata,
                    retrieved_at=excluded.retrieved_at, metadata_json=excluded.metadata_json,
                    in_latest_discovery=1''', values)
                for t in m.tokens:
                    self.db.execute('''INSERT INTO tokens VALUES (?,?,?,?)
                        ON CONFLICT(token_id) DO UPDATE SET market_id=excluded.market_id,
                        outcome=excluded.outcome,retrieved_at=excluded.retrieved_at''',
                                    (t.token_id, t.market_id, t.outcome, m.retrieved_at))

    def health(self, metrics):
        with self.db:
            self.db.execute("INSERT INTO system_health(timestamp,metrics_json) VALUES (?,?)",
                            (metrics["timestamp"], dumps(metrics)))

    def opportunity(self, episode, *, sample=False):
        """Latest episode + every positive changed observation, in one transaction.

        Full scalar fields and consumed levels are retained in record_json and
        observation_json. Frequently queried financial values also have TEXT columns.
        These research tables are intentionally excluded from Phase 1 retention.
        """
        names = ("id", "session_id", "market_id", "condition_id", "strategy_type",
                 "observation_status", "first_seen_at", "last_seen_at", "duration_ms",
                 "observation_count", "closed_at", "close_reason", "optimal_quantity",
                 "gross_cost", "gross_edge", "gross_roi", "fee_status", "estimated_fees",
                 "net_edge", "net_roi", "best_gross_edge_seen", "best_net_edge_seen",
                 "best_quantity_seen", "best_roi_seen")
        from decimal import Decimal
        values = [str(episode[k]) if isinstance(episode.get(k), Decimal) else episode.get(k) for k in names]
        columns = ",".join(names) + ",record_json"
        updates = ",".join(f"{k}=excluded.{k}" for k in names[1:]) + ",record_json=excluded.record_json"
        with self.db:
            self.db.execute(f"INSERT INTO complement_opportunities ({columns}) VALUES ({','.join('?' for _ in range(len(names)+1))}) "
                            f"ON CONFLICT(id) DO UPDATE SET {updates}", (*values, dumps(episode)))
            if sample:
                self.db.execute("INSERT INTO complement_observations(episode_id,observed_at,elapsed_ms,observation_json) VALUES (?,?,?,?)",
                    (episode["id"], episode["last_seen_at"], episode["duration_ms"], dumps(episode)))

    def censor_open_opportunities(self, timestamp):
        """A process restart cannot establish continuity of an old episode."""
        import json
        rows = self.db.execute("SELECT record_json FROM complement_opportunities WHERE closed_at IS NULL").fetchall()
        for (raw,) in rows:
            episode = json.loads(raw)
            episode.update(closed_at=timestamp, close_reason="PROCESS_INTERRUPTED", censored=True)
            self.opportunity(episode)

    def incident(self, kind, details):
        with self.db:
            self.db.execute("INSERT INTO incidents(timestamp,kind,details_json) VALUES (?,?,?)",
                            (now_ms(), kind, dumps(details)))

    def snapshot(self, book, kind="periodic", full=False):
        with self.db:
            self.db.execute('''INSERT INTO book_snapshots
                (token_id,timestamp,kind,full_depth,snapshot_json) VALUES (?,?,?,?,?)''',
                            (book.token_id, now_ms(), kind, full, dumps(book.export(None if full else 5))))

    def prune(self, timestamp=None):
        now = now_ms() if timestamp is None else timestamp
        with self.db:
            self.db.execute("DELETE FROM book_snapshots WHERE timestamp < ? AND full_depth=1", (now-86400000,))
            self.db.execute("DELETE FROM book_snapshots WHERE timestamp < ?", (now-7*86400000,))
            self.db.execute("DELETE FROM system_health WHERE timestamp < ?", (now-7*86400000,))
            self.db.execute("DELETE FROM incidents WHERE timestamp < ?", (now-30*86400000,))
        self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def size_bytes(self):
        return sum(p.stat().st_size for p in (self.path, Path(str(self.path)+"-wal"),
                                             Path(str(self.path)+"-shm")) if p.exists())

    def close(self):
        self.db.close()
