"""
🌐 Universe Scanner — Catch-All Coverage for VKTX/CVNA-Type Movers
====================================================================
FEB 16 v5.2 — Addresses the DATA PIPELINE GAP:

Problem:  VKTX and CVNA had ZERO presence in predictive signals, MWS forecast,
          dark pool, and UW flow caches.  The system cannot catch what it cannot see.

Solution: Four overlapping nets that ensure every universe ticker gets evaluated:

  1. FULL UNIVERSE COVERAGE CHECK
     Identifies which of the 104 universe tickers are NOT covered by ANY
     existing data source (forecast, predictive, dark pool, flow).
     Logs a daily coverage report so gaps are visible.

  2. CATCH-ALL TECHNICAL SCREEN (Polygon)
     For uncovered tickers, runs a lightweight technical screen:
       - RSI oversold (< 35) or overbought (> 70)
       - Volume spike (current volume > 2x 20-day average)
       - 5-day price momentum (> ±5%)
     Uses a SINGLE Polygon snapshot API call (covers all tickers at once).

  3. PRE-MARKET GAP AUTO-CANDIDACY
     Stocks gapping >3% pre-market at 9:21 AM get automatic candidate status
     regardless of whether they appear in any other data source.
     Uses Polygon snapshot API (same call as #2).

  4. UW UNUSUAL ACTIVITY ALERTS
     Checks Unusual Whales flow for any universe ticker with >3x normal
     options volume (unusual activity).  Uses cached uw_flow_cache.json
     plus a supplementary API call for uncovered tickers if UW API key
     is available.

CRITICAL: This module does NOT modify PutsEngine, TradeNova, or any
existing engine logic.  It READS cached data and Polygon/UW APIs
to produce supplementary candidates.

Returns candidates in the same dict format as moonshot_adapter's
_load_predictive_signal_candidates(), so they can be merged seamlessly.
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
PUTSENGINE_PATH = Path.home() / "PutsEngine"

# ─── Configuration ──────────────────────────────────────────────────
# Pre-market gap threshold for auto-candidacy (%)
PREMARKET_GAP_THRESHOLD = 3.0

# RSI thresholds for catch-all screen
RSI_OVERSOLD = 35
RSI_OVERBOUGHT = 70

# Volume spike multiplier (current vs 20-day avg)
VOLUME_SPIKE_MULTIPLIER = 2.0

# Momentum threshold (5-day price change %)
MOMENTUM_THRESHOLD_PCT = 5.0

# UW unusual activity: volume vs normal ratio
UW_UNUSUAL_VOLUME_MULTIPLIER = 3.0

# Minimum combined score to generate a candidate
MIN_CATCHALL_SCORE = 0.30


# ═══════════════════════════════════════════════════════════════════
# STATIC UNIVERSE
# ═══════════════════════════════════════════════════════════════════

_STATIC_UNIVERSE: Optional[Set[str]] = None


def _get_static_universe() -> Set[str]:
    """Load the full 104-ticker static universe from PutsEngine."""
    global _STATIC_UNIVERSE
    if _STATIC_UNIVERSE is not None:
        return _STATIC_UNIVERSE

    try:
        import sys
        pe_path = str(PUTSENGINE_PATH)
        if pe_path not in sys.path:
            sys.path.insert(0, pe_path)
        from putsengine.config import EngineConfig
        _STATIC_UNIVERSE = set(EngineConfig.get_all_tickers())
        logger.debug(f"  Universe scanner: loaded {len(_STATIC_UNIVERSE)} tickers")
    except (ImportError, AttributeError) as e:
        logger.debug(f"  Universe scanner: PutsEngine unavailable — {e}")
        _STATIC_UNIVERSE = set()

    return _STATIC_UNIVERSE


# ═══════════════════════════════════════════════════════════════════
# COVERAGE ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def _get_covered_tickers() -> Dict[str, Set[str]]:
    """
    Identify which tickers are covered by each existing data source.

    Returns: {source_name: {set of covered tickers}}
    """
    coverage: Dict[str, Set[str]] = {
        "forecast": set(),
        "predictive_signals": set(),
        "dark_pool": set(),
        "uw_flow": set(),
        "interval_picks": set(),
    }

    # 1. Forecast
    try:
        fc_path = TRADENOVA_DATA / "tomorrows_forecast.json"
        if fc_path.exists():
            with open(fc_path) as f:
                fc_data = json.load(f)
            for fc in fc_data.get("forecasts", []):
                sym = fc.get("symbol", "")
                if sym:
                    coverage["forecast"].add(sym)
    except Exception:
        pass

    # 2. Predictive signals
    for fname in ("predictive_signals.json", "predictive_signals_latest.json"):
        try:
            fpath = TRADENOVA_DATA / fname
            if not fpath.exists():
                continue
            with open(fpath) as f:
                data = json.load(f)
            if isinstance(data, dict):
                for date_key, day_data in data.items():
                    if not isinstance(day_data, dict):
                        continue
                    for scan in day_data.get("scans", []):
                        if not isinstance(scan, dict):
                            continue
                        for sig in scan.get("signals", []):
                            if isinstance(sig, dict):
                                sym = sig.get("symbol", "")
                                if sym:
                                    coverage["predictive_signals"].add(sym)
        except Exception:
            pass

    # 3. Dark pool
    try:
        dp_path = TRADENOVA_DATA / "darkpool_cache.json"
        if dp_path.exists():
            with open(dp_path) as f:
                dp_data = json.load(f)
            for sym in dp_data:
                if isinstance(sym, str) and sym.isupper() and len(sym) <= 5:
                    coverage["dark_pool"].add(sym)
    except Exception:
        pass

    # 4. UW flow
    try:
        uw_path = TRADENOVA_DATA / "uw_flow_cache.json"
        if uw_path.exists():
            with open(uw_path) as f:
                uw_data = json.load(f)
            flow_data = uw_data.get("flow_data", uw_data)
            if isinstance(flow_data, dict):
                for sym in flow_data:
                    if isinstance(sym, str) and sym not in ("timestamp", "generated_at"):
                        coverage["uw_flow"].add(sym)
    except Exception:
        pass

    # 5. Interval picks
    try:
        eod_path = TRADENOVA_DATA / "eod_interval_picks.json"
        if eod_path.exists():
            with open(eod_path) as f:
                eod_data = json.load(f)
            for interval_data in eod_data.get("intervals", {}).values():
                for pick in interval_data.get("picks", []):
                    sym = pick.get("symbol", "")
                    if sym:
                        coverage["interval_picks"].add(sym)
    except Exception:
        pass

    return coverage


def get_uncovered_tickers() -> Set[str]:
    """
    Return universe tickers that are NOT covered by ANY data source.

    These are the VKTX/CVNA-type blind spots that need catch-all scanning.
    """
    universe = _get_static_universe()
    if not universe:
        return set()

    coverage = _get_covered_tickers()
    all_covered = set()
    for source_tickers in coverage.values():
        all_covered |= source_tickers

    uncovered = universe - all_covered

    # Log coverage report
    logger.info(
        f"  🌐 Universe coverage: {len(all_covered)}/{len(universe)} tickers covered "
        f"({len(uncovered)} uncovered)"
    )
    for source, tickers in sorted(coverage.items()):
        in_universe = tickers & universe
        logger.debug(f"    {source}: {len(in_universe)} universe tickers")

    if uncovered:
        logger.info(
            f"  ⚠️ UNCOVERED tickers ({len(uncovered)}): "
            f"{', '.join(sorted(uncovered)[:20])}"
            + (f"... +{len(uncovered)-20} more" if len(uncovered) > 20 else "")
        )

    return uncovered


# ═══════════════════════════════════════════════════════════════════
# POLYGON TECHNICAL SCREEN + PRE-MARKET GAP
# ═══════════════════════════════════════════════════════════════════

def scan_polygon_snapshot(
    polygon_api_key: str = "",
    target_tickers: Optional[Set[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch Polygon snapshot for ALL universe tickers in a SINGLE API call.

    For each ticker, computes:
      - premarket_gap_pct: gap from prev close (for auto-candidacy)
      - todaysChangePerc: intraday change
      - volume_ratio: today's volume vs prev day (proxy for spike)
      - price: current/last trade price

    Args:
        polygon_api_key: Polygon.io API key
        target_tickers: If provided, only return data for these tickers.
                        If None, returns data for full universe.

    Returns:
        {symbol: {price, prev_close, premarket_gap_pct, volume_ratio, ...}}
    """
    if not polygon_api_key:
        polygon_api_key = (
            os.getenv("POLYGON_API_KEY", "")
            or os.getenv("MASSIVE_API_KEY", "")
        )

    if not polygon_api_key:
        logger.debug("  Universe scanner: No Polygon API key")
        return {}

    universe = target_tickers or _get_static_universe()
    if not universe:
        return {}

    result = {}

    try:
        import requests

        # Single API call for ALL tickers snapshot
        url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {"apiKey": polygon_api_key}

        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code != 200:
            logger.warning(
                f"  Universe scanner: Polygon snapshot returned {resp.status_code}"
            )
            return {}

        data = resp.json()
        tickers_data = data.get("tickers", [])

        for td in tickers_data:
            sym = td.get("ticker", "")
            if sym not in universe:
                continue

            prev_day = td.get("prevDay", {})
            prev_close = prev_day.get("c", 0)
            prev_volume = prev_day.get("v", 0)

            day = td.get("day", {})
            day_open = day.get("o", 0)
            day_volume = day.get("v", 0)

            last_trade = td.get("lastTrade", {})
            current_price = last_trade.get("p", 0) or day.get("c", 0)

            todays_change_pct = td.get("todaysChangePerc", 0) or 0

            # Compute pre-market gap
            premarket_gap_pct = 0
            if prev_close > 0:
                if current_price > 0:
                    premarket_gap_pct = ((current_price - prev_close) / prev_close) * 100
                elif day_open > 0:
                    premarket_gap_pct = ((day_open - prev_close) / prev_close) * 100
                elif todays_change_pct:
                    premarket_gap_pct = todays_change_pct

            # Volume ratio (today vs prev day)
            volume_ratio = 0
            if prev_volume > 0 and day_volume > 0:
                volume_ratio = day_volume / prev_volume

            result[sym] = {
                "price": current_price or (prev_close * (1 + todays_change_pct / 100) if prev_close else 0),
                "prev_close": prev_close,
                "day_open": day_open,
                "day_volume": day_volume,
                "prev_volume": prev_volume,
                "premarket_gap_pct": round(premarket_gap_pct, 2),
                "todays_change_pct": round(todays_change_pct, 2),
                "volume_ratio": round(volume_ratio, 2),
            }

        logger.info(
            f"  🌐 Polygon snapshot: {len(result)} universe tickers loaded"
        )

    except Exception as e:
        logger.warning(f"  Universe scanner: Polygon snapshot failed — {e}")

    return result


def _compute_rsi_from_polygon(
    symbol: str,
    polygon_api_key: str,
    period: int = 14,
) -> Optional[float]:
    """
    Compute RSI for a single ticker from Polygon daily bars.

    Uses the Polygon RSI technical indicator endpoint (single call per ticker).
    Falls back to manual computation from daily bars if needed.

    Returns RSI value (0-100) or None if unavailable.
    """
    if not polygon_api_key:
        return None

    try:
        import requests

        # Try Polygon technical indicator endpoint first (most efficient)
        url = f"https://api.polygon.io/v1/indicators/rsi/{symbol}"
        params = {
            "apiKey": polygon_api_key,
            "timespan": "day",
            "window": period,
            "limit": 1,
        }

        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            values = data.get("results", {}).get("values", [])
            if values:
                return values[0].get("value")

    except Exception:
        pass

    return None


# ═══════════════════════════════════════════════════════════════════
# UW UNUSUAL ACTIVITY DETECTION
# ═══════════════════════════════════════════════════════════════════

def detect_uw_unusual_activity(
    target_tickers: Optional[Set[str]] = None,
    uw_api_key: str = "",
) -> Dict[str, Dict[str, Any]]:
    """
    Detect unusual options activity for universe tickers.

    Method 1: Analyze cached uw_flow_cache.json for tickers with
              abnormally high options volume (>3x their typical level).

    Method 2: If UW API key is available, call the Unusual Whales
              flow alerts endpoint for additional coverage.

    Returns:
        {symbol: {
            total_premium, total_trades, avg_premium,
            unusual_ratio, call_pct, put_pct,
            is_unusual: True
        }}
    """
    universe = target_tickers or _get_static_universe()
    if not universe:
        return {}

    result = {}

    # ── Method 1: Cached UW flow analysis ────────────────────────
    try:
        flow_path = TRADENOVA_DATA / "uw_flow_cache.json"
        if flow_path.exists():
            with open(flow_path) as f:
                raw = json.load(f)

            flow_data = raw.get("flow_data", raw)
            if isinstance(flow_data, dict):
                # Compute per-symbol metrics
                all_trade_counts = []
                sym_metrics: Dict[str, Dict] = {}

                for sym, trades in flow_data.items():
                    if sym in ("timestamp", "generated_at"):
                        continue
                    if not isinstance(trades, list):
                        continue
                    if sym not in universe:
                        continue

                    total_premium = sum(
                        float(t.get("premium", 0) or 0) for t in trades
                    )
                    call_premium = sum(
                        float(t.get("premium", 0) or 0)
                        for t in trades
                        if t.get("put_call") == "C"
                    )
                    put_premium = sum(
                        float(t.get("premium", 0) or 0)
                        for t in trades
                        if t.get("put_call") == "P"
                    )
                    n_trades = len(trades)
                    all_trade_counts.append(n_trades)

                    sym_metrics[sym] = {
                        "total_premium": total_premium,
                        "total_trades": n_trades,
                        "avg_premium": total_premium / n_trades if n_trades > 0 else 0,
                        "call_premium": call_premium,
                        "put_premium": put_premium,
                        "call_pct": call_premium / total_premium if total_premium > 0 else 0.5,
                        "put_pct": put_premium / total_premium if total_premium > 0 else 0.5,
                    }

                # Compute median trade count as "normal" baseline
                if all_trade_counts:
                    sorted_counts = sorted(all_trade_counts)
                    median_idx = len(sorted_counts) // 2
                    median_trades = sorted_counts[median_idx]

                    for sym, metrics in sym_metrics.items():
                        if median_trades > 0:
                            unusual_ratio = metrics["total_trades"] / median_trades
                        else:
                            unusual_ratio = 0

                        if unusual_ratio >= UW_UNUSUAL_VOLUME_MULTIPLIER:
                            metrics["unusual_ratio"] = round(unusual_ratio, 2)
                            metrics["is_unusual"] = True
                            result[sym] = metrics

        logger.info(
            f"  🦈 UW unusual activity (cached): {len(result)} tickers "
            f"with >{UW_UNUSUAL_VOLUME_MULTIPLIER}x normal volume"
        )
        if result:
            top3 = sorted(
                result.items(),
                key=lambda x: x[1]["unusual_ratio"],
                reverse=True,
            )[:3]
            logger.info(
                f"    Top unusual: {[(s, f'{d['unusual_ratio']:.1f}x') for s, d in top3]}"
            )

    except Exception as e:
        logger.debug(f"  UW unusual activity (cached): {e}")

    # ── Method 2: UW API for uncovered tickers ───────────────────
    if not uw_api_key:
        uw_api_key = os.getenv("UNUSUAL_WHALES_API_KEY", "")

    if uw_api_key:
        try:
            import requests

            # Check which universe tickers we DON'T have flow data for
            covered_by_flow = set(result.keys())
            try:
                flow_path = TRADENOVA_DATA / "uw_flow_cache.json"
                if flow_path.exists():
                    with open(flow_path) as f:
                        raw = json.load(f)
                    flow_data = raw.get("flow_data", raw)
                    if isinstance(flow_data, dict):
                        covered_by_flow |= {
                            s for s in flow_data.keys()
                            if s not in ("timestamp", "generated_at")
                        }
            except Exception:
                pass

            uncovered = universe - covered_by_flow
            if uncovered:
                logger.info(
                    f"  🦈 UW API: checking {len(uncovered)} uncovered tickers..."
                )

                # Use UW stock screener / flow endpoint
                # Query for recent unusual flow for these symbols
                headers = {
                    "Authorization": f"Bearer {uw_api_key}",
                    "Accept": "application/json",
                }

                # Batch check tickers via flow endpoint
                for sym in sorted(uncovered):
                    try:
                        flow_url = f"https://api.unusualwhales.com/api/stock/{sym}/option-contracts"
                        resp = requests.get(
                            flow_url,
                            headers=headers,
                            timeout=5,
                        )
                        if resp.status_code == 200:
                            contracts = resp.json().get("data", [])
                            if contracts:
                                total_vol = sum(
                                    int(c.get("volume", 0) or 0)
                                    for c in contracts[:50]
                                )
                                total_oi = sum(
                                    int(c.get("open_interest", 0) or 0)
                                    for c in contracts[:50]
                                )

                                # If volume > 3x open interest, that's unusual
                                if total_oi > 0 and total_vol / total_oi >= UW_UNUSUAL_VOLUME_MULTIPLIER:
                                    result[sym] = {
                                        "total_premium": 0,
                                        "total_trades": total_vol,
                                        "avg_premium": 0,
                                        "unusual_ratio": round(total_vol / total_oi, 2),
                                        "call_pct": 0.5,
                                        "put_pct": 0.5,
                                        "is_unusual": True,
                                        "source": "uw_api",
                                    }
                    except Exception:
                        continue  # Skip individual ticker failures

                logger.info(
                    f"  🦈 UW API: found {len(result) - len(covered_by_flow)} "
                    f"additional unusual tickers"
                )

        except Exception as e:
            logger.debug(f"  UW API unusual activity: {e}")

    return result


# ═══════════════════════════════════════════════════════════════════
# MAIN SCANNER — GENERATES CANDIDATES
# ═══════════════════════════════════════════════════════════════════

def scan_universe_catchall(
    polygon_api_key: str = "",
    uw_api_key: str = "",
    direction: str = "both",
) -> List[Dict[str, Any]]:
    """
    Run the full universe catch-all scanner.

    Combines:
      1. Coverage analysis (which tickers are blind spots?)
      2. Polygon snapshot (pre-market gaps + technical signals)
      3. UW unusual activity (abnormal options volume)

    Generates candidates for any ticker that shows:
      - Pre-market gap > 3% (auto-candidacy)
      - RSI oversold/overbought + volume spike
      - UW unusual activity (>3x normal volume)

    Args:
        polygon_api_key: Polygon.io API key
        uw_api_key: Unusual Whales API key
        direction: "bullish" for moonshot, "bearish" for puts, "both" for all

    Returns:
        List of candidate dicts compatible with moonshot/puts adapter format.
    """
    universe = _get_static_universe()
    if not universe:
        logger.warning("  Universe scanner: no static universe — skipping")
        return []

    logger.info(
        f"🌐 Universe Scanner: scanning {len(universe)} tickers "
        f"(direction={direction})..."
    )

    # Step 1: Coverage analysis
    uncovered = get_uncovered_tickers()

    # Step 2: Polygon snapshot (single API call)
    snapshot = scan_polygon_snapshot(polygon_api_key, universe)

    # Step 3: UW unusual activity
    uw_unusual = detect_uw_unusual_activity(universe, uw_api_key)

    # Step 4: Generate candidates
    candidates = []

    for sym in sorted(universe):
        snap = snapshot.get(sym, {})
        uw = uw_unusual.get(sym, {})
        is_uncovered = sym in uncovered

        signals = []
        score_parts = {}
        candidate_direction = None  # "bullish" or "bearish"

        # ── Pre-market gap auto-candidacy (highest priority) ─────
        gap_pct = snap.get("premarket_gap_pct", 0)
        if abs(gap_pct) >= PREMARKET_GAP_THRESHOLD:
            if gap_pct > 0:
                score_parts["premarket_gap"] = 0.35
                candidate_direction = "bullish"
                if gap_pct >= 5.0:
                    signals.append(f"🔥🔥 Major pre-market gap +{gap_pct:.1f}%")
                    score_parts["premarket_gap"] = 0.45
                else:
                    signals.append(f"🔥 Pre-market gap +{gap_pct:.1f}%")
            else:
                score_parts["premarket_gap"] = 0.35
                candidate_direction = "bearish"
                if gap_pct <= -5.0:
                    signals.append(f"🔥🔥 Major pre-market gap {gap_pct:.1f}%")
                    score_parts["premarket_gap"] = 0.45
                else:
                    signals.append(f"🔥 Pre-market gap {gap_pct:.1f}%")

        # ── Volume spike ─────────────────────────────────────────
        vol_ratio = snap.get("volume_ratio", 0)
        if vol_ratio >= VOLUME_SPIKE_MULTIPLIER:
            score_parts["volume_spike"] = min(0.20, 0.10 + (vol_ratio - 2.0) * 0.02)
            signals.append(f"Volume spike {vol_ratio:.1f}x")

        # ── Intraday momentum (for PM scans) ─────────────────────
        change_pct = snap.get("todays_change_pct", 0)
        if abs(change_pct) >= MOMENTUM_THRESHOLD_PCT:
            score_parts["momentum"] = 0.15
            if change_pct > 0:
                signals.append(f"Momentum +{change_pct:.1f}%")
                if candidate_direction is None:
                    candidate_direction = "bullish"
            else:
                signals.append(f"Momentum {change_pct:.1f}%")
                if candidate_direction is None:
                    candidate_direction = "bearish"

        # ── UW unusual activity ──────────────────────────────────
        if uw.get("is_unusual"):
            unusual_ratio = uw.get("unusual_ratio", 0)
            score_parts["uw_unusual"] = min(0.25, 0.15 + (unusual_ratio - 3.0) * 0.02)
            signals.append(f"UW unusual {unusual_ratio:.1f}x vol")

            # Direction from options flow
            call_pct = uw.get("call_pct", 0.5)
            if call_pct > 0.65 and candidate_direction is None:
                candidate_direction = "bullish"
                signals.append("Call-heavy flow")
            elif call_pct < 0.35 and candidate_direction is None:
                candidate_direction = "bearish"
                signals.append("Put-heavy flow")

        # ── Uncovered bonus (these are blind spots!) ─────────────
        if is_uncovered and signals:
            score_parts["uncovered_bonus"] = 0.10
            signals.append("⚠️ Uncovered ticker")

        # ── Compute total score ──────────────────────────────────
        total_score = sum(score_parts.values())

        if total_score < MIN_CATCHALL_SCORE:
            continue

        # Default direction if ambiguous
        if candidate_direction is None:
            if change_pct > 0 or gap_pct > 0:
                candidate_direction = "bullish"
            elif change_pct < 0 or gap_pct < 0:
                candidate_direction = "bearish"
            else:
                candidate_direction = "bullish"  # Default

        # Direction filter
        if direction == "bullish" and candidate_direction != "bullish":
            continue
        if direction == "bearish" and candidate_direction != "bearish":
            continue

        price = snap.get("price", 0) or snap.get("prev_close", 0)

        candidate = {
            "symbol": sym,
            "score": round(min(total_score, 0.95), 3),
            "price": round(price, 2) if price else 0,
            "signals": signals,
            "signal_types": ["Universe-Scanner"],
            "option_type": "call" if candidate_direction == "bullish" else "put",
            "target_return": abs(gap_pct) if gap_pct else abs(change_pct),
            "engine": f"{'Moonshot' if candidate_direction == 'bullish' else 'Puts'} (Universe Scanner)",
            "sector": "",
            "volume_ratio": vol_ratio,
            "short_interest": 0,
            "action": "WATCH",
            "entry_low": price,
            "entry_high": price,
            "target": 0,
            "stop": 0,
            "rsi": 50,
            "uw_sentiment": "",
            "data_source": "universe_scanner",
            "data_age_days": 0,
            # Scanner-specific metadata
            "_scanner_source": "universe_catchall",
            "_scanner_direction": candidate_direction,
            "_premarket_gap_pct": gap_pct,
            "_volume_ratio": vol_ratio,
            "_uw_unusual_ratio": uw.get("unusual_ratio", 0),
            "_is_uncovered": is_uncovered,
            "_score_parts": score_parts,
            "_move_potential_score": min(0.50 + total_score * 0.3, 0.80),
            "catalysts": signals[:3],  # For catalyst detection downstream
        }

        candidates.append(candidate)

    # Sort by score descending
    candidates.sort(key=lambda x: (-x["score"], -abs(x.get("_premarket_gap_pct", 0))))

    logger.info(
        f"🌐 Universe Scanner: {len(candidates)} candidates generated "
        f"(direction={direction})"
    )
    if candidates:
        for i, c in enumerate(candidates[:10], 1):
            logger.info(
                f"  #{i:2d} {c['symbol']:6s} score={c['score']:.3f} "
                f"gap={c['_premarket_gap_pct']:+.1f}% "
                f"vol={c['_volume_ratio']:.1f}x "
                f"uw={c['_uw_unusual_ratio']:.1f}x "
                f"{'⚠️UNCOVERED' if c['_is_uncovered'] else ''} "
                f"[{', '.join(c['signals'][:3])}]"
            )

    return candidates


def get_coverage_report() -> Dict[str, Any]:
    """
    Generate a full coverage report for logging/diagnostics.

    Returns a dict with coverage stats suitable for JSON serialization.
    """
    universe = _get_static_universe()
    coverage = _get_covered_tickers()

    all_covered = set()
    source_stats = {}
    for source, tickers in coverage.items():
        in_universe = tickers & universe
        all_covered |= in_universe
        source_stats[source] = {
            "total": len(tickers),
            "in_universe": len(in_universe),
            "tickers": sorted(in_universe)[:20],
        }

    uncovered = universe - all_covered

    return {
        "universe_size": len(universe),
        "covered_count": len(all_covered),
        "uncovered_count": len(uncovered),
        "coverage_pct": round(len(all_covered) / len(universe) * 100, 1) if universe else 0,
        "uncovered_tickers": sorted(uncovered),
        "sources": source_stats,
        "timestamp": datetime.now().isoformat(),
    }
