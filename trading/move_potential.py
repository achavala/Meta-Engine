"""
Move Potential Score
====================
Quantifies a stock's likelihood of making a significant move (≥3-5%).

Options are convex instruments — a 1% move barely covers premium, but a
5%+ move generates 3-10x returns.  This score identifies names most likely
to deliver the large underlying moves that drive options profits.

Components:
  1. ATR% (14-day)      — recent realized volatility as % of price
  2. Big-Move Frequency  — fraction of days with |return| > 3% in last 60 sessions
  3. Catalyst Proximity  — earnings within 3 days → boosted move probability

FEB 16, 2026 — Initial implementation.
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _fetch_daily_bars(
    symbol: str,
    days: int = 70,
    api_key: str = "",
) -> List[Dict]:
    """
    Fetch daily OHLCV bars from Polygon.io.
    Returns list of {o, h, l, c, v, t} dicts, newest last.
    """
    if not api_key:
        api_key = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
    if not api_key:
        return []

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days + 15)).strftime("%Y-%m-%d")

    try:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start_date}/{end_date}"
        )
        resp = requests.get(
            url,
            params={"adjusted": "true", "sort": "asc", "limit": 100, "apiKey": api_key},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("results", [])
    except Exception as e:
        logger.debug(f"  Bars fetch failed for {symbol}: {e}")
    return []


def compute_atr_pct(bars: List[Dict], period: int = 14) -> float:
    """
    Compute Average True Range as % of closing price.
    
    ATR% is the most robust measure of "how much does this stock move
    on a typical day?"  Higher ATR% → more likely to deliver 3-5% moves.
    
    Returns: float (e.g., 0.04 = 4% average daily range)
    """
    if len(bars) < period + 1:
        return 0.0

    trs = []
    for i in range(1, len(bars)):
        h = bars[i].get("h", 0)
        l = bars[i].get("l", 0)
        pc = bars[i - 1].get("c", 0)
        if pc <= 0:
            continue
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < period:
        return 0.0

    # EMA-style ATR
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period

    # Convert to percentage of latest close
    latest_close = bars[-1].get("c", 0)
    if latest_close <= 0:
        return 0.0

    return atr / latest_close


def compute_big_move_frequency(bars: List[Dict], threshold_pct: float = 3.0, lookback: int = 60) -> float:
    """
    Fraction of days with |close-to-close return| > threshold_pct in the last N sessions.
    
    A stock that has delivered >3% moves on 30% of days in the last 60 sessions
    has proven move potential.  A stock that never moves >3% is unlikely to
    generate the large options returns we need.
    
    Returns: float 0.0-1.0 (fraction of big-move days)
    """
    if len(bars) < 2:
        return 0.0

    recent_bars = bars[-lookback:] if len(bars) >= lookback else bars
    big_days = 0
    total_days = 0

    for i in range(1, len(recent_bars)):
        prev_close = recent_bars[i - 1].get("c", 0)
        curr_close = recent_bars[i].get("c", 0)
        if prev_close <= 0:
            continue
        ret_pct = abs((curr_close - prev_close) / prev_close * 100)
        total_days += 1
        if ret_pct >= threshold_pct:
            big_days += 1

    return big_days / total_days if total_days > 0 else 0.0


def compute_move_potential_score(
    bars: List[Dict],
    has_earnings_catalyst: bool = False,
    period: int = 14,
) -> Tuple[float, Dict[str, float]]:
    """
    Compute the Move Potential Score (0.0-1.0) from daily bars.
    
    Components and weights:
      - ATR%            50% — realized vol is the strongest predictor
      - Big-move freq   30% — historical delivery rate of large moves
      - Catalyst prox   20% — earnings/events boost move probability
    
    Returns:
      (score, components_dict)
    """
    components = {
        "atr_pct": 0.0,
        "big_move_freq": 0.0,
        "catalyst_boost": 0.0,
        "raw_atr_pct": 0.0,
    }

    # 1. ATR% → normalized score
    atr_pct = compute_atr_pct(bars, period)
    components["raw_atr_pct"] = atr_pct

    # Normalize: 2% ATR = 0.3, 4% = 0.6, 6%+ = 0.9, 8%+ = 1.0
    if atr_pct >= 0.08:
        atr_score = 1.0
    elif atr_pct >= 0.06:
        atr_score = 0.9
    elif atr_pct >= 0.04:
        atr_score = 0.6 + (atr_pct - 0.04) / 0.02 * 0.3
    elif atr_pct >= 0.02:
        atr_score = 0.3 + (atr_pct - 0.02) / 0.02 * 0.3
    elif atr_pct >= 0.01:
        atr_score = 0.1 + (atr_pct - 0.01) / 0.01 * 0.2
    else:
        atr_score = atr_pct / 0.01 * 0.1
    components["atr_pct"] = min(atr_score, 1.0)

    # 2. Big-move frequency → score
    bmf = compute_big_move_frequency(bars, threshold_pct=3.0, lookback=60)
    # Normalize: 5% of days = 0.2, 15% = 0.5, 30%+ = 1.0
    if bmf >= 0.30:
        bmf_score = 1.0
    elif bmf >= 0.15:
        bmf_score = 0.5 + (bmf - 0.15) / 0.15 * 0.5
    elif bmf >= 0.05:
        bmf_score = 0.2 + (bmf - 0.05) / 0.10 * 0.3
    else:
        bmf_score = bmf / 0.05 * 0.2
    components["big_move_freq"] = min(bmf_score, 1.0)

    # 3. Catalyst proximity
    components["catalyst_boost"] = 1.0 if has_earnings_catalyst else 0.0

    # Weighted composite
    score = (
        components["atr_pct"]        * 0.50 +
        components["big_move_freq"]  * 0.30 +
        components["catalyst_boost"] * 0.20
    )

    return max(0.0, min(score, 1.0)), components


def batch_compute_move_potential(
    symbols: List[str],
    earnings_set: set = None,
    api_key: str = "",
    max_symbols: int = 40,
) -> Dict[str, Tuple[float, Dict]]:
    """
    Batch-compute Move Potential Score for multiple symbols.
    
    Fetches daily bars from Polygon and computes score for each.
    Limits to max_symbols to avoid excessive API calls.
    
    Returns: {symbol: (score, components)}
    """
    if not api_key:
        api_key = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")

    if earnings_set is None:
        earnings_set = set()

    results = {}
    import time

    for i, sym in enumerate(symbols[:max_symbols]):
        bars = _fetch_daily_bars(sym, days=70, api_key=api_key)
        has_catalyst = sym in earnings_set
        score, components = compute_move_potential_score(bars, has_catalyst)
        results[sym] = (score, components)

        # Rate limiting — Polygon allows 5 req/sec on paid plans
        if (i + 1) % 5 == 0:
            time.sleep(0.25)

    return results
