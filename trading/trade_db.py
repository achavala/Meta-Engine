"""
Meta Engine — Trade Database (SQLite)
======================================
Stores all trade history for 6 months with full audit trail.
Database location: <Meta Engine>/data/trades.db
"""

import json
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("meta_engine.trading.db")

DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "trades.db"

# ═══════════════════════════════════════════════════════
# Schema
# ═══════════════════════════════════════════════════════
_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        TEXT    UNIQUE NOT NULL,
    created_at      TEXT    DEFAULT (datetime('now')),

    -- Session
    session         TEXT    NOT NULL,           -- 'AM' or 'PM'
    scan_date       TEXT    NOT NULL,           -- '2026-02-10'

    -- Instrument
    symbol          TEXT    NOT NULL,           -- Underlying (e.g. 'AAPL')
    option_symbol   TEXT,                       -- OCC symbol
    option_type     TEXT    NOT NULL,           -- 'call' or 'put'
    strike_price    REAL,
    expiry_date     TEXT,
    contracts       INTEGER DEFAULT 5,

    -- Prices
    entry_price     REAL    DEFAULT 0,          -- Option premium at entry
    current_price   REAL    DEFAULT 0,          -- Latest option premium
    exit_price      REAL    DEFAULT 0,          -- Option premium at close
    underlying_price REAL   DEFAULT 0,          -- Stock price at entry

    -- Meta Engine context
    meta_score      REAL    DEFAULT 0,
    meta_signals    TEXT    DEFAULT '[]',        -- JSON array
    source_engine   TEXT    DEFAULT '',          -- 'PutsEngine' or 'Moonshot'

    -- Alpaca order IDs
    entry_order_id  TEXT    DEFAULT '',
    exit_order_id   TEXT    DEFAULT '',

    -- Status
    status          TEXT    DEFAULT 'pending',   -- pending/filled/open/closed/cancelled/expired
    exit_reason     TEXT    DEFAULT '',           -- take_profit/stop_loss/time_stop/manual

    -- P&L
    pnl             REAL    DEFAULT 0,           -- Total $ P&L
    pnl_pct         REAL    DEFAULT 0,           -- % return
    
    -- Partial profit tracking
    partial_profit_taken INTEGER DEFAULT 0,       -- 1 if partial profit taken at 2x
    partial_profit_price REAL    DEFAULT 0,      -- Price at which partial profit was taken
    partial_profit_pct   REAL    DEFAULT 0,       -- P&L % at partial profit exit

    -- Timestamps
    filled_at       TEXT,
    closed_at       TEXT,
    updated_at      TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS daily_summary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT    UNIQUE NOT NULL,
    total_trades    INTEGER DEFAULT 0,
    winning_trades  INTEGER DEFAULT 0,
    losing_trades   INTEGER DEFAULT 0,
    total_pnl       REAL    DEFAULT 0,
    best_trade_sym  TEXT    DEFAULT '',
    best_trade_pnl  REAL    DEFAULT 0,
    worst_trade_sym TEXT    DEFAULT '',
    worst_trade_pnl REAL    DEFAULT 0,
    created_at      TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_scan_date ON trades(scan_date);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
"""


class TradeDB:
    """SQLite database for trade history and P&L tracking."""

    def __init__(self, db_path: str = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_tables()

    # ── Connection helpers ────────────────────────────────
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_tables(self):
        with self._get_conn() as conn:
            conn.executescript(_CREATE_TABLES)
            # Migration: Add partial profit columns if they don't exist
            try:
                conn.execute("""
                    ALTER TABLE trades 
                    ADD COLUMN partial_profit_taken INTEGER DEFAULT 0
                """)
            except sqlite3.OperationalError:
                pass  # Column already exists
            try:
                conn.execute("""
                    ALTER TABLE trades 
                    ADD COLUMN partial_profit_price REAL DEFAULT 0
                """)
            except sqlite3.OperationalError:
                pass  # Column already exists
            try:
                conn.execute("""
                    ALTER TABLE trades 
                    ADD COLUMN partial_profit_pct REAL DEFAULT 0
                """)
            except sqlite3.OperationalError:
                pass  # Column already exists
            conn.commit()
        logger.debug(f"Trade DB ready at {self.db_path}")

    # ── Insert / Update ───────────────────────────────────
    def insert_trade(self, trade: Dict[str, Any]) -> int:
        """Insert a new trade record. Returns row id."""
        cols = [
            "trade_id", "session", "scan_date", "symbol", "option_symbol",
            "option_type", "strike_price", "expiry_date", "contracts",
            "entry_price", "underlying_price", "meta_score", "meta_signals",
            "source_engine", "entry_order_id", "status",
        ]
        vals = [trade.get(c, "") for c in cols]
        # Serialize meta_signals if it's a list
        idx = cols.index("meta_signals")
        if isinstance(vals[idx], list):
            vals[idx] = json.dumps(vals[idx])

        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        with self._get_conn() as conn:
            cur = conn.execute(
                f"INSERT INTO trades ({col_names}) VALUES ({placeholders})", vals
            )
            conn.commit()
            return cur.lastrowid

    def update_trade(self, trade_id: str, **fields) -> None:
        """Update specific fields on a trade."""
        if not fields:
            return
        fields["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [trade_id]
        with self._get_conn() as conn:
            conn.execute(
                f"UPDATE trades SET {set_clause} WHERE trade_id = ?", vals
            )
            conn.commit()

    # ── Queries ───────────────────────────────────────────
    def get_open_positions(self) -> List[Dict]:
        """Get all trades with status 'open' or 'filled'."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status IN ('open', 'filled') "
                "ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_pending_trades(self) -> List[Dict]:
        """Get all trades with status 'pending'."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status = 'pending' "
                "ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_trade(self, trade_id: str) -> Optional[Dict]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM trades WHERE trade_id = ?", (trade_id,)
            ).fetchone()
        return dict(row) if row else None

    def get_trades_by_date(self, scan_date: str) -> List[Dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE scan_date = ? ORDER BY created_at",
                (scan_date,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_trades(self, days: int = 180) -> List[Dict]:
        """Get trades from the last N days (default 6 months)."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE scan_date >= ? "
                "ORDER BY created_at DESC",
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_closed_trades(self, days: int = 180) -> List[Dict]:
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status = 'closed' AND scan_date >= ? "
                "ORDER BY closed_at DESC",
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Aggregates ────────────────────────────────────────
    def get_summary_stats(self, days: int = 180) -> Dict[str, Any]:
        """Compute aggregate stats for dashboard cards."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*)                                        AS total_trades,
                    SUM(CASE WHEN status='closed' AND pnl > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status='closed' AND pnl <= 0 THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN status='closed' THEN pnl ELSE 0 END)            AS total_pnl,
                    SUM(CASE WHEN status IN ('open','filled') THEN 1 ELSE 0 END)  AS open_positions,
                    SUM(CASE WHEN scan_date = date('now') THEN pnl ELSE 0 END)    AS today_pnl
                FROM trades WHERE scan_date >= ?
            """, (cutoff,)).fetchone()

        total = dict(row) if row else {}
        # Ensure no None values (empty DB)
        for key in ("total_trades", "wins", "losses", "total_pnl", "open_positions", "today_pnl"):
            if total.get(key) is None:
                total[key] = 0
        wins = total["wins"]
        losses = total["losses"]
        closed = wins + losses
        total["win_rate"] = round((wins / closed * 100), 1) if closed > 0 else 0.0
        return total

    def get_daily_pnl_series(self, days: int = 180) -> List[Dict]:
        """Get daily cumulative P&L for charting."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT scan_date,
                       SUM(pnl) AS daily_pnl,
                       COUNT(*) AS trades
                FROM trades
                WHERE status = 'closed' AND scan_date >= ?
                GROUP BY scan_date
                ORDER BY scan_date
            """, (cutoff,)).fetchall()

        series = []
        cumulative = 0.0
        for r in rows:
            d = dict(r)
            cumulative += d.get("daily_pnl", 0) or 0
            d["cumulative_pnl"] = round(cumulative, 2)
            series.append(d)
        return series

    def cleanup_old(self, keep_days: int = 180):
        """Delete trades older than keep_days."""
        cutoff = (datetime.utcnow() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        with self._get_conn() as conn:
            conn.execute("DELETE FROM trades WHERE scan_date < ?", (cutoff,))
            conn.commit()
        logger.info(f"Cleaned up trades older than {cutoff}")
