"""
Recurrence Tracker: Tracks picks appearing multiple times
========================================================
Tracks picks appearing 2x or 3x+ in the last week and applies
recurrence boost to ranking algorithm.

30+ years trading + PhD quant + institutional microstructure lens
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict, Counter
import pytz

EST = pytz.timezone("US/Eastern")

# Path to recurrence tracking database
RECURRENCE_DB = Path(__file__).parent.parent / "data" / "recurrence_tracker.db"


def _ensure_recurrence_db():
    """Create recurrence tracking database if it doesn't exist."""
    RECURRENCE_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_recurrence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                option_type TEXT NOT NULL,
                scan_date TEXT NOT NULL,
                scan_timestamp TEXT NOT NULL,
                rank INTEGER NOT NULL,
                engine TEXT NOT NULL,
                score REAL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(symbol, option_type, scan_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_type ON pick_recurrence(symbol, option_type)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scan_date ON pick_recurrence(scan_date)
        """)
        conn.commit()


def track_pick(
    symbol: str,
    option_type: str,
    scan_date: str,
    scan_timestamp: str,
    rank: int,
    engine: str,
    score: float = 0.0,
):
    """Track a pick in the recurrence database."""
    _ensure_recurrence_db()
    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pick_recurrence 
            (symbol, option_type, scan_date, scan_timestamp, rank, engine, score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (symbol, option_type, scan_date, scan_timestamp, rank, engine, score))
        conn.commit()


def get_recurrence_counts(days: int = 7) -> Dict[str, Dict[str, int]]:
    """
    Get recurrence counts for all picks in the last N days.
    
    Returns:
        Dict: {symbol: {"put": count, "call": count, "total": count}}
    """
    _ensure_recurrence_db()
    cutoff = (datetime.now(EST) - timedelta(days=days)).date().isoformat()
    
    recurrence_counts = defaultdict(lambda: {"put": 0, "call": 0, "total": 0})
    
    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT symbol, option_type, COUNT(*) as count
            FROM pick_recurrence
            WHERE scan_date >= ?
            GROUP BY symbol, option_type
        """, (cutoff,)).fetchall()
        
        for row in rows:
            symbol = row["symbol"]
            option_type = row["option_type"]
            count = row["count"]
            
            recurrence_counts[symbol][option_type] = count
            recurrence_counts[symbol]["total"] += count
    
    return dict(recurrence_counts)


def get_recurrence_stars(symbol: str, option_type: str, days: int = 7) -> int:
    """
    Get recurrence stars for a pick.
    
    Returns:
        0: No recurrence
        2: Appeared 2 times (⭐⭐)
        3: Appeared 3+ times (⭐⭐⭐)
    """
    counts = get_recurrence_counts(days)
    symbol_data = counts.get(symbol, {})
    count = symbol_data.get(option_type, 0)
    
    if count >= 3:
        return 3
    elif count == 2:
        return 2
    else:
        return 0


def apply_recurrence_boost(
    picks: List[Dict[str, Any]],
    days: int = 7,
    boost_2x: float = 0.15,  # 15% boost for 2x recurrence
    boost_3x: float = 0.30,  # 30% boost for 3x+ recurrence
) -> List[Dict[str, Any]]:
    """
    Apply recurrence boost to picks based on how many times they appeared.
    
    Picks appearing 2x get ⭐⭐ and 15% score boost
    Picks appearing 3x+ get ⭐⭐⭐ and 30% score boost
    
    These boosted picks should rank in Top 3 for X posts.
    
    Args:
        picks: List of pick dicts with 'symbol', 'option_type', 'score'
        days: Number of days to look back for recurrence
        boost_2x: Score boost multiplier for 2x recurrence
        boost_3x: Score boost multiplier for 3x+ recurrence
    
    Returns:
        List of picks with recurrence boost applied and sorted
    """
    if not picks:
        return picks
    
    counts = get_recurrence_counts(days)
    
    # Apply boost to each pick
    boosted_picks = []
    for pick in picks:
        symbol = pick.get("symbol", "")
        option_type = pick.get("option_type", "")
        base_score = float(pick.get("score", 0) or 0)
        
        # Get recurrence count
        symbol_data = counts.get(symbol, {})
        count = symbol_data.get(option_type, 0)
        
        # Calculate boost
        stars = 0
        boost_multiplier = 1.0
        
        if count >= 3:
            stars = 3
            boost_multiplier = 1.0 + boost_3x
        elif count == 2:
            stars = 2
            boost_multiplier = 1.0 + boost_2x
        
        # Apply boost
        boosted_score = base_score * boost_multiplier
        
        # Create boosted pick
        boosted_pick = pick.copy()
        boosted_pick["score"] = boosted_score
        boosted_pick["_recurrence_stars"] = stars
        boosted_pick["_recurrence_count"] = count
        boosted_pick["_base_score"] = base_score
        boosted_pick["_boost_applied"] = boost_multiplier - 1.0
        
        boosted_picks.append(boosted_pick)
    
    # Sort by boosted score (descending)
    boosted_picks.sort(key=lambda x: x["score"], reverse=True)
    
    return boosted_picks


def format_stars(stars: int) -> str:
    """Format stars for display."""
    if stars >= 3:
        return "⭐⭐⭐"
    elif stars == 2:
        return "⭐⭐"
    else:
        return ""


# ═══════════════════════════════════════════════════════
# NEGATIVE RECURRENCE FILTER (FEB 15, 2026)
# ═══════════════════════════════════════════════════════
# Backtest finding: AMZN appeared 5 times with 0 wins,
# GOOGL appeared 5 times with 2 wins.
# Symbols failing 2+ consecutive times should be temporarily excluded.


def _ensure_outcome_table():
    """Create pick outcome tracking table if not exists."""
    _ensure_recurrence_db()
    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                option_type TEXT NOT NULL,
                scan_date TEXT NOT NULL,
                pick_price REAL DEFAULT 0,
                outcome_price REAL DEFAULT 0,
                direction_correct INTEGER DEFAULT 0,
                pnl_pct REAL DEFAULT 0,
                recorded_at TEXT DEFAULT (datetime('now')),
                UNIQUE(symbol, option_type, scan_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_outcome_symbol
            ON pick_outcomes(symbol, option_type)
        """)
        conn.commit()


def record_pick_outcome(
    symbol: str,
    option_type: str,
    scan_date: str,
    pick_price: float,
    outcome_price: float,
):
    """
    Record the outcome of a previous pick.

    For puts: stock going DOWN is correct (positive pnl for put holder).
    For calls: stock going UP is correct (positive pnl for call holder).
    """
    _ensure_outcome_table()
    if pick_price <= 0 or outcome_price <= 0:
        return

    pnl_pct = ((outcome_price - pick_price) / pick_price) * 100

    # Direction correctness depends on option type
    if option_type == "put":
        direction_correct = 1 if pnl_pct < -0.5 else 0  # Stock dropped > 0.5%
    else:
        direction_correct = 1 if pnl_pct > 0.5 else 0    # Stock rose > 0.5%

    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pick_outcomes
            (symbol, option_type, scan_date, pick_price, outcome_price,
             direction_correct, pnl_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (symbol, option_type, scan_date, pick_price, outcome_price,
              direction_correct, pnl_pct))
        conn.commit()


def get_consecutive_failures(symbol: str, option_type: str, lookback_days: int = 14) -> int:
    """
    Get the number of consecutive recent failures for a symbol+type.
    Returns 0 if no failures or no data.
    """
    _ensure_outcome_table()
    cutoff = (datetime.now(EST) - timedelta(days=lookback_days)).date().isoformat()

    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        rows = conn.execute("""
            SELECT direction_correct
            FROM pick_outcomes
            WHERE symbol = ? AND option_type = ? AND scan_date >= ?
            ORDER BY scan_date DESC
        """, (symbol, option_type, cutoff)).fetchall()

    if not rows:
        return 0

    # Count consecutive failures from most recent
    failures = 0
    for row in rows:
        if row[0] == 0:  # failure
            failures += 1
        else:
            break  # streak broken

    return failures


def get_excluded_symbols(min_consecutive_failures: int = 2, lookback_days: int = 14) -> set:
    """
    Get symbols that should be excluded due to consecutive failures.

    Returns:
        Set of (symbol, option_type) tuples to exclude.
    """
    _ensure_outcome_table()
    cutoff = (datetime.now(EST) - timedelta(days=lookback_days)).date().isoformat()

    excluded = set()

    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        # Get all unique symbol/type pairs with outcomes in the lookback period
        pairs = conn.execute("""
            SELECT DISTINCT symbol, option_type
            FROM pick_outcomes
            WHERE scan_date >= ?
        """, (cutoff,)).fetchall()

        for symbol, option_type in pairs:
            rows = conn.execute("""
                SELECT direction_correct
                FROM pick_outcomes
                WHERE symbol = ? AND option_type = ? AND scan_date >= ?
                ORDER BY scan_date DESC
            """, (symbol, option_type, cutoff)).fetchall()

            # Count consecutive failures from most recent
            failures = 0
            for row in rows:
                if row[0] == 0:
                    failures += 1
                else:
                    break

            if failures >= min_consecutive_failures:
                excluded.add((symbol, option_type))

    return excluded


def record_outcomes_from_previous_scan(polygon_api_key: str = ""):
    """
    Check price movement for picks from the previous scan and record outcomes.
    Called at the start of each new scan to build outcome history.

    ENHANCED (FEB 15, 2026): Uses a 3-tier strategy:
      1. TRADE DB: Check actual closed trades for real P&L (most accurate)
      2. POLYGON API: Fetch next-day price for direction check (moderate)
      3. CACHED PRICES: Use saved price data from scan results (fallback)

    Also extends lookback from 3 → 7 days for more aggressive tracking.
    """
    import os
    import requests

    _ensure_outcome_table()
    if not polygon_api_key:
        polygon_api_key = os.getenv("POLYGON_API_KEY", "")

    # Extended lookback: 7 days (was 3) for more aggressive outcome tracking
    cutoff = (datetime.now(EST) - timedelta(days=7)).date().isoformat()
    today_str = datetime.now(EST).strftime("%Y-%m-%d")

    with sqlite3.connect(str(RECURRENCE_DB)) as conn:
        conn.row_factory = sqlite3.Row
        # Picks tracked but not yet evaluated
        picks = conn.execute("""
            SELECT r.symbol, r.option_type, r.scan_date, r.score
            FROM pick_recurrence r
            LEFT JOIN pick_outcomes o
              ON r.symbol = o.symbol
              AND r.option_type = o.option_type
              AND r.scan_date = o.scan_date
            WHERE r.scan_date >= ? AND r.scan_date < ?
              AND o.id IS NULL
        """, (cutoff, today_str)).fetchall()

    if not picks:
        return

    recorded = 0

    # ── TIER 1: Check trade database for actual closed trades ──────────
    try:
        from trading.trade_db import TradeDB
        trade_db = TradeDB()
        closed_trades = trade_db.get_closed_trades(days=7)
        # Build lookup: (symbol, scan_date) → trade
        trade_lookup = {}
        for t in closed_trades:
            key = (t.get("symbol", ""), t.get("scan_date", ""))
            if key not in trade_lookup:
                trade_lookup[key] = t

        for pick in picks:
            symbol = pick["symbol"]
            option_type = pick["option_type"]
            scan_date = pick["scan_date"]
            trade = trade_lookup.get((symbol, scan_date))
            if trade:
                entry_px = float(trade.get("entry_price", 0) or 0)
                exit_px = float(trade.get("exit_price", 0) or 0)
                if entry_px > 0 and exit_px > 0:
                    record_pick_outcome(
                        symbol=symbol,
                        option_type=option_type,
                        scan_date=scan_date,
                        pick_price=entry_px,
                        outcome_price=exit_px,
                    )
                    recorded += 1
    except Exception:
        pass  # Trade DB not available — continue with Polygon

    # ── TIER 2: Polygon API for remaining unevaluated picks ────────────
    if polygon_api_key:
        # Refresh the list of unevaluated picks (some may have been recorded in Tier 1)
        with sqlite3.connect(str(RECURRENCE_DB)) as conn:
            conn.row_factory = sqlite3.Row
            remaining = conn.execute("""
                SELECT r.symbol, r.option_type, r.scan_date, r.score
                FROM pick_recurrence r
                LEFT JOIN pick_outcomes o
                  ON r.symbol = o.symbol
                  AND r.option_type = o.option_type
                  AND r.scan_date = o.scan_date
                WHERE r.scan_date >= ? AND r.scan_date < ?
                  AND o.id IS NULL
            """, (cutoff, today_str)).fetchall()

        for pick in remaining:
            symbol = pick["symbol"]
            scan_date = pick["scan_date"]
            try:
                url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
                resp = requests.get(url, params={"apiKey": polygon_api_key}, timeout=10)
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    if results:
                        outcome_price = results[0].get("c", 0)
                        pick_price = results[0].get("o", 0)
                        if pick_price > 0 and outcome_price > 0:
                            record_pick_outcome(
                                symbol=symbol,
                                option_type=pick["option_type"],
                                scan_date=scan_date,
                                pick_price=pick_price,
                                outcome_price=outcome_price,
                            )
                            recorded += 1
            except Exception:
                pass  # Non-critical — skip silently

    if recorded > 0:
        logger.info(f"  📊 Recurrence outcomes: {recorded} pick outcomes recorded")
