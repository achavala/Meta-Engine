"""
Real-Time Mover Scanner
========================
Uses Polygon snapshot API (single call) to detect which stocks in the
104-ticker universe are ACTUALLY moving significantly right now.

This is the missing piece: upstream engines (PutsEngine, TradeNova)
rely on overnight scans and cached signals. They completely miss
intraday movers. On Feb 17, 2026, PANW dropped -10.3% and BYND surged
+7.5% — but neither appeared in any upstream data source.

The snapshot API returns real-time price data for all US equities in
one call. We filter to our universe and flag anything moving > threshold.

These movers are injected directly into the puts/moonshot candidate
pools, where they compete with signal-based candidates on equal footing.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

logger = logging.getLogger(__name__)

POLYGON_SNAPSHOT_URL = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"

# Thresholds for mover detection
GAP_UP_THRESHOLD = 2.0     # +2% to flag as bullish mover
GAP_DOWN_THRESHOLD = -2.0  # -2% to flag as bearish mover

# UW flow cache for enriching movers
TRADENOVA_DATA = Path.home() / "TradeNova" / "data"

# In-memory cache: both adapters call this during the same scan cycle,
# so we cache the result for 5 minutes to avoid double API calls.
_SCAN_CACHE: Dict[str, Any] = {}
_SCAN_CACHE_TS: float = 0.0
_SCAN_CACHE_TTL = 300  # 5 minutes


def scan_realtime_movers(
    static_universe: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Scan Polygon snapshot for real-time movers in the universe.

    Results are cached for 5 minutes since both adapters call this
    during the same scan cycle.

    Returns:
        {
            "gap_up_movers": [{"symbol": str, "change_pct": float, "price": float, ...}],
            "gap_down_movers": [{"symbol": str, "change_pct": float, "price": float, ...}],
            "all_prices": {symbol: {"price": float, "change_pct": float, "prev_close": float}},
            "timestamp": str,
        }
    """
    global _SCAN_CACHE, _SCAN_CACHE_TS
    import time
    now = time.time()
    if _SCAN_CACHE and (now - _SCAN_CACHE_TS) < _SCAN_CACHE_TTL:
        logger.info(f"  📡 Mover Scanner: Using cached snapshot ({int(now - _SCAN_CACHE_TS)}s old)")
        return _SCAN_CACHE

    api_key = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
    if not api_key:
        logger.warning("  📡 Mover Scanner: No Polygon API key — cannot detect real-time movers")
        return {"gap_up_movers": [], "gap_down_movers": [], "all_prices": {}, "timestamp": ""}

    universe = static_universe or _get_static_universe()
    if not universe:
        logger.warning("  📡 Mover Scanner: No universe loaded")
        return {"gap_up_movers": [], "gap_down_movers": [], "all_prices": {}, "timestamp": ""}

    uw_flow = _load_uw_flow_ratios()

    try:
        resp = requests.get(
            POLYGON_SNAPSHOT_URL,
            params={"apiKey": api_key},
            timeout=20,
        )
        if resp.status_code != 200:
            logger.warning(f"  📡 Mover Scanner: Polygon snapshot returned {resp.status_code}")
            return {"gap_up_movers": [], "gap_down_movers": [], "all_prices": {}, "timestamp": ""}

        data = resp.json()
        tickers_data = data.get("tickers", [])

        gap_up_movers = []
        gap_down_movers = []
        all_prices = {}

        for td in tickers_data:
            sym = td.get("ticker", "")
            if sym not in universe:
                continue

            prev_close = td.get("prevDay", {}).get("c", 0)
            today_open = td.get("day", {}).get("o", 0)
            today_close = td.get("day", {}).get("c", 0)
            last_trade = td.get("lastTrade", {}).get("p", 0)
            todays_change_pct = td.get("todaysChangePerc", 0)
            volume = td.get("day", {}).get("v", 0)
            prev_volume = td.get("prevDay", {}).get("v", 0)

            current_price = last_trade or today_close or today_open
            if not prev_close or prev_close <= 0 or not current_price:
                continue

            change_pct = ((current_price - prev_close) / prev_close) * 100
            if todays_change_pct and abs(todays_change_pct) > abs(change_pct):
                change_pct = todays_change_pct

            vol_ratio = volume / prev_volume if prev_volume and prev_volume > 0 else 1.0

            uw_info = uw_flow.get(sym, {})
            call_put_ratio = uw_info.get("call_put_ratio", 1.0)
            call_pct = uw_info.get("call_pct", 0.50)

            price_info = {
                "price": round(current_price, 2),
                "change_pct": round(change_pct, 2),
                "prev_close": round(prev_close, 2),
                "volume": volume,
                "volume_ratio": round(vol_ratio, 2),
                "call_put_ratio": round(call_put_ratio, 2),
                "call_pct": round(call_pct, 3),
            }
            all_prices[sym] = price_info

            mover_info = {
                "symbol": sym,
                "change_pct": round(change_pct, 2),
                "price": round(current_price, 2),
                "prev_close": round(prev_close, 2),
                "volume": volume,
                "volume_ratio": round(vol_ratio, 2),
                "call_put_ratio": round(call_put_ratio, 2),
                "call_pct": round(call_pct, 3),
            }

            if change_pct >= GAP_UP_THRESHOLD:
                gap_up_movers.append(mover_info)
            elif change_pct <= GAP_DOWN_THRESHOLD:
                gap_down_movers.append(mover_info)

        gap_up_movers.sort(key=lambda x: x["change_pct"], reverse=True)
        gap_down_movers.sort(key=lambda x: x["change_pct"])

        logger.info(
            f"  📡 Mover Scanner: {len(all_prices)} universe tickers priced, "
            f"{len(gap_up_movers)} gap-up (>+{GAP_UP_THRESHOLD}%), "
            f"{len(gap_down_movers)} gap-down (<{GAP_DOWN_THRESHOLD}%)"
        )

        if gap_up_movers:
            top_desc = ", ".join("{} +{:.1f}%".format(m["symbol"], m["change_pct"]) for m in gap_up_movers[:5])
            logger.info(f"  📈 Top gap-up: {top_desc}")
        if gap_down_movers:
            top_desc = ", ".join("{} {:.1f}%".format(m["symbol"], m["change_pct"]) for m in gap_down_movers[:5])
            logger.info(f"  📉 Top gap-down: {top_desc}")

        result = {
            "gap_up_movers": gap_up_movers,
            "gap_down_movers": gap_down_movers,
            "all_prices": all_prices,
            "timestamp": datetime.now().isoformat(),
        }
        _SCAN_CACHE.update(result)
        _SCAN_CACHE_TS = now
        return result

    except Exception as e:
        logger.error(f"  📡 Mover Scanner: Polygon snapshot failed — {e}")
        logger.info("  📡 Mover Scanner: Attempting per-ticker fallback...")
        return _fallback_per_ticker_scan(universe, uw_flow, api_key)


def build_puts_candidates_from_movers(
    gap_down_movers: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Convert gap-down movers into PutsEngine-compatible candidate dicts.

    A stock dropping -5% in real-time IS the strongest bearish signal.
    These get injected into the puts candidate pool alongside signal-based
    candidates from the PutsEngine scan.
    """
    candidates = []
    for m in gap_down_movers:
        change = m["change_pct"]
        mag = abs(change)

        # Magnitude IS the trade — use wide scale so -10% clearly beats -5%
        magnitude_score = min(mag / 12.0, 1.0)
        vol_bonus = min(m.get("volume_ratio", 1.0) / 5.0, 0.08)
        call_pct = m.get("call_pct", 0.5)
        flow_bonus = max(0, (1.0 - call_pct * 2)) * 0.05

        composite = magnitude_score * 0.85 + vol_bonus + flow_bonus

        signals = [f"realtime_gap_down_{mag:.0f}pct"]
        if m.get("volume_ratio", 0) > 1.5:
            signals.append("high_volume_selloff")
        if call_pct < 0.40:
            signals.append("put_heavy_flow")
        if mag > 5.0:
            signals.append("severe_drop")
        if mag > 3.0:
            signals.append("significant_drop")

        candidates.append({
            "symbol": m["symbol"],
            "score": round(composite, 3),
            "price": m["price"],
            "passed_gates": True,
            "distribution_score": magnitude_score,
            "dealer_score": 0,
            "liquidity_score": vol_bonus,
            "signals": signals,
            "block_reasons": [],
            "engine": "RealTime_Mover_Scanner",
            "engine_type": "realtime_gap_down",
            "data_source": f"polygon_snapshot_realtime ({m['change_pct']:+.1f}%)",
            "data_age_days": 0,
            "_is_realtime_mover": True,
            "_realtime_change_pct": change,
            "_volume_ratio": m.get("volume_ratio", 0),
            "_call_pct": call_pct,
        })

    return candidates


def build_moonshot_candidates_from_movers(
    gap_up_movers: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Convert gap-up movers into Moonshot-compatible candidate dicts.

    A stock surging +5% in real-time IS the strongest bullish signal.
    These get injected into the moonshot candidate pool alongside
    signal-based candidates from TradeNova.
    """
    candidates = []
    for m in gap_up_movers:
        change = m["change_pct"]

        magnitude_score = min(change / 10.0, 1.0)
        vol_bonus = min(m.get("volume_ratio", 1.0) / 5.0, 0.08)
        call_pct = m.get("call_pct", 0.5)
        flow_bonus = min(call_pct * 2, 1.0) * 0.05

        composite = magnitude_score * 0.85 + vol_bonus + flow_bonus

        signals = [f"realtime_gap_up_{change:.0f}pct"]
        if m.get("volume_ratio", 0) > 1.5:
            signals.append("high_volume_rally")
        if call_pct > 0.55:
            signals.append("call_heavy_flow")
        if change > 5.0:
            signals.append("major_surge")
        if change > 3.0:
            signals.append("significant_rally")

        candidates.append({
            "symbol": m["symbol"],
            "score": round(composite, 3),
            "price": m["price"],
            "signals": signals,
            "signal_types": ["realtime_mover"],
            "option_type": "call",
            "target_return": round(change * 2, 1),
            "engine": "RealTime_Mover_Scanner",
            "sector": "",
            "volume_ratio": m.get("volume_ratio", 0),
            "short_interest": 0,
            "action": f"REALTIME_GAP_UP +{change:.1f}%",
            "uw_sentiment": "bullish" if call_pct > 0.55 else "neutral",
            "data_source": f"polygon_snapshot_realtime (+{change:.1f}%)",
            "data_age_days": 0,
            "_is_realtime_mover": True,
            "_realtime_change_pct": change,
            "_volume_ratio": m.get("volume_ratio", 0),
            "_call_pct": call_pct,
        })

    return candidates


def _get_static_universe() -> Set[str]:
    """Load the 104-ticker static universe from PutsEngine."""
    try:
        import sys
        putsengine_path = str(Path.home() / "PutsEngine")
        if putsengine_path not in sys.path:
            sys.path.insert(0, putsengine_path)
        from putsengine.config import EngineConfig
        return set(EngineConfig.get_all_tickers())
    except Exception:
        return set()


def _load_uw_flow_ratios() -> Dict[str, Dict[str, Any]]:
    """Load UW flow data to enrich movers with call/put ratios."""
    try:
        cache_file = TRADENOVA_DATA / "uw_flow_cache.json"
        if not cache_file.exists():
            return {}

        with open(cache_file) as f:
            uw_data = json.load(f)

        flow = uw_data.get("flow_data", uw_data) if isinstance(uw_data, dict) else {}
        if not isinstance(flow, dict):
            return {}

        result = {}
        for sym, entries in flow.items():
            if isinstance(entries, list):
                call_prem = sum(
                    t.get("premium", 0) for t in entries
                    if isinstance(t, dict) and t.get("put_call") == "C"
                )
                put_prem = sum(
                    t.get("premium", 0) for t in entries
                    if isinstance(t, dict) and t.get("put_call") == "P"
                )
                total = call_prem + put_prem
                if total > 0:
                    result[sym] = {
                        "call_put_ratio": call_prem / put_prem if put_prem > 0 else 10.0,
                        "call_pct": call_prem / total,
                        "total_premium": total,
                    }
            elif isinstance(entries, dict):
                cp_ratio = entries.get("call_put_ratio", entries.get("cp_ratio", 1.0))
                result[sym] = {
                    "call_put_ratio": cp_ratio,
                    "call_pct": cp_ratio / (1 + cp_ratio) if cp_ratio else 0.5,
                    "total_premium": entries.get("total_premium", 0),
                }

        return result

    except Exception as e:
        logger.debug(f"  UW flow load failed: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════
# FALLBACK: per-ticker API when snapshot endpoint fails
# ═══════════════════════════════════════════════════════════════════

def _fallback_per_ticker_scan(
    universe: Set[str],
    uw_flow: Dict[str, Dict[str, Any]],
    api_key: str,
) -> Dict[str, Any]:
    """
    When the snapshot endpoint fails (403/429/timeout), fall back to
    per-ticker prev-close API. Slower (104 calls) but always works.
    """
    gap_up_movers = []
    gap_down_movers = []
    all_prices = {}
    fetched = 0

    for sym in sorted(universe):
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{sym}/prev"
            resp = requests.get(url, params={"apiKey": api_key}, timeout=8)
            if resp.status_code != 200:
                continue
            results = resp.json().get("results", [])
            if not results:
                continue
            bar = results[0]
            prev_close = bar.get("c", 0)
            today_open = bar.get("o", 0)
            if not prev_close or prev_close <= 0:
                continue

            change_pct = ((today_open - prev_close) / prev_close) * 100 if today_open else 0

            uw_info = uw_flow.get(sym, {})
            price_info = {
                "price": round(today_open or prev_close, 2),
                "change_pct": round(change_pct, 2),
                "prev_close": round(prev_close, 2),
                "volume": bar.get("v", 0),
                "volume_ratio": 1.0,
                "call_put_ratio": round(uw_info.get("call_put_ratio", 1.0), 2),
                "call_pct": round(uw_info.get("call_pct", 0.50), 3),
            }
            all_prices[sym] = price_info

            mover_info = {
                "symbol": sym,
                "change_pct": round(change_pct, 2),
                "price": round(today_open or prev_close, 2),
                "prev_close": round(prev_close, 2),
                "volume": bar.get("v", 0),
                "volume_ratio": 1.0,
                "call_put_ratio": round(uw_info.get("call_put_ratio", 1.0), 2),
                "call_pct": round(uw_info.get("call_pct", 0.50), 3),
            }

            if change_pct >= GAP_UP_THRESHOLD:
                gap_up_movers.append(mover_info)
            elif change_pct <= GAP_DOWN_THRESHOLD:
                gap_down_movers.append(mover_info)

            fetched += 1
        except Exception:
            continue

    gap_up_movers.sort(key=lambda x: x["change_pct"], reverse=True)
    gap_down_movers.sort(key=lambda x: x["change_pct"])

    logger.info(
        f"  📡 Mover Scanner (FALLBACK): {fetched} tickers fetched, "
        f"{len(gap_up_movers)} gap-up, {len(gap_down_movers)} gap-down"
    )

    return {
        "gap_up_movers": gap_up_movers,
        "gap_down_movers": gap_down_movers,
        "all_prices": all_prices,
        "timestamp": datetime.now().isoformat(),
        "fallback_used": True,
    }


# ═══════════════════════════════════════════════════════════════════
# COVERAGE MONITOR: post-scan self-validation
# ═══════════════════════════════════════════════════════════════════

def validate_scan_coverage(
    puts_top10: List[Dict[str, Any]],
    calls_top10: List[Dict[str, Any]],
    all_prices: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Post-scan check: did we miss any stock moving >5% that's not in the
    Top 10? If so, log a WARNING. This runs automatically after each scan
    to guarantee we never silently miss major movers again.

    Returns a report dict for monitoring.
    """
    if all_prices is None:
        rt = scan_realtime_movers()
        all_prices = rt.get("all_prices", {})

    put_syms = {p.get("symbol", "") for p in puts_top10}
    call_syms = {c.get("symbol", "") for c in calls_top10}

    missed_puts = []
    missed_calls = []

    for sym, info in all_prices.items():
        chg = info.get("change_pct", 0)
        if chg <= -3.0 and sym not in put_syms:
            missed_puts.append({"symbol": sym, "change_pct": chg})
        if chg >= 3.0 and sym not in call_syms:
            missed_calls.append({"symbol": sym, "change_pct": chg})

    missed_puts.sort(key=lambda x: x["change_pct"])
    missed_calls.sort(key=lambda x: x["change_pct"], reverse=True)

    if missed_puts:
        syms_desc = ", ".join(
            "{} {:.1f}%".format(m["symbol"], m["change_pct"])
            for m in missed_puts[:5]
        )
        logger.warning(
            f"  ⚠️ COVERAGE GAP: {len(missed_puts)} gap-down stocks NOT in puts Top 10: {syms_desc}"
        )
    if missed_calls:
        syms_desc = ", ".join(
            "{} +{:.1f}%".format(m["symbol"], m["change_pct"])
            for m in missed_calls[:5]
        )
        logger.warning(
            f"  ⚠️ COVERAGE GAP: {len(missed_calls)} gap-up stocks NOT in calls Top 10: {syms_desc}"
        )

    if not missed_puts and not missed_calls:
        logger.info("  ✅ COVERAGE CHECK: All major movers (>3%) are in Top 10")

    return {
        "missed_puts": missed_puts,
        "missed_calls": missed_calls,
        "puts_top10_count": len(puts_top10),
        "calls_top10_count": len(calls_top10),
        "coverage_ok": len(missed_puts) == 0 and len(missed_calls) == 0,
    }
