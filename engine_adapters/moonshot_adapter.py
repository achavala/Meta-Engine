"""
Moonshot/TradeNova Engine Adapter
==================================
Interface to get Top 10 moonshot candidates from the TradeNova Moonshot Engine.
Imports Moonshot modules directly without modifying the original codebase.

Data Sources (priority order for fallback):
  0. eod_interval_picks.json — Broadest pool: 10 picks per interval scan,
     captures intraday momentum names (MSFT, HIMS, BILL, etc.) that
     final_recommendations.json often misses.
  1. final_recommendations.json — Curated ranked picks with UW sentiment.
  2. final_recommendations_history.json — Historical picks (supplement).
  3. moonshot CSV data — Oldest fallback.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Add TradeNova to path
TRADENOVA_PATH = str(Path.home() / "TradeNova")
if TRADENOVA_PATH not in sys.path:
    sys.path.insert(0, TRADENOVA_PATH)


def get_top_moonshots(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Get the Top N moonshot candidates from the TradeNova Moonshot Engine.
    
    Uses the Moonshot Engine's scanning pipeline to find the best
    lottery-ticket / high-momentum candidates.
    
    Args:
        top_n: Number of top picks to return (default 10)
        
    Returns:
        List of dicts with keys: symbol, score, price, signals, etc.
    """
    try:
        from moonshot.core.engine import MoonshotEngine
        from moonshot.config import MoonshotConfig
        
        logger.info("🟢 Moonshot Engine: Running full scan...")
        
        engine = MoonshotEngine(paper_mode=True)
        
        # Run the moonshot scan
        candidates = engine.scan_for_moonshots()
        
        results = []
        for c in candidates:
            result = {
                "symbol": c.symbol if hasattr(c, 'symbol') else str(c),
                "score": c.score if hasattr(c, 'score') else 0,
                "price": c.price if hasattr(c, 'price') else 0,
                "signals": [],
                "signal_types": [],
                "option_type": getattr(c, 'option_type', 'call'),
                "target_return": getattr(c, 'target_return', 0),
                "engine": "Moonshot",
                "sector": getattr(c, 'sector', ''),
                "volume_ratio": getattr(c, 'volume_ratio', 0),
                "short_interest": getattr(c, 'short_interest', 0),
            }
            
            # Extract signals from candidate
            if hasattr(c, 'signals') and c.signals:
                if isinstance(c.signals, list):
                    result["signals"] = [str(s) for s in c.signals]
                elif isinstance(c.signals, dict):
                    result["signals"] = list(c.signals.keys())
            
            if hasattr(c, 'signal_types') and c.signal_types:
                result["signal_types"] = [str(st) for st in c.signal_types]
            
            results.append(result)
        
        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        
        top_picks = results[:top_n]
        logger.info(f"🟢 Moonshot Engine: Top {len(top_picks)} picks selected")
        for i, p in enumerate(top_picks, 1):
            logger.info(f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — ${p['price']:.2f}")
        
        # If live scan returned nothing (e.g. weekend/holiday), fall back to cached data
        if not top_picks:
            logger.info("🟢 Live scan returned 0 candidates — falling back to cached TradeNova data...")
            return _fallback_from_cached_moonshots(top_n)
        
        return top_picks
        
    except Exception as e:
        logger.error(f"Moonshot scan failed: {e}")
        return _fallback_from_cached_moonshots(top_n)


def _fallback_from_cached_moonshots(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Fallback: Read from TradeNova's cached results.
    Priority order:
      0. eod_interval_picks.json (broadest pool — captures MSFT, HIMS, etc.)
      1. final_recommendations.json (most recent ranked picks)
      2. final_recommendations_history.json (historical picks)
      3. moonshot/data_collection/*.csv (raw moonshot scans)
    """
    results = []
    data_timestamps: Dict[str, str] = {}  # Track data freshness per source

    # --- Source 0: eod_interval_picks.json (broadest candidate pool) ---
    # This file contains 10 picks per interval scan throughout the day.
    # It captures high-momentum intraday names that final_recommendations
    # may miss entirely (e.g., MSFT +3.1%, HIMS +12.8%, BILL +3.7%).
    try:
        eod_file = Path(TRADENOVA_PATH) / "data" / "eod_interval_picks.json"
        if eod_file.exists():
            with open(eod_file) as f:
                eod_data = json.load(f)

            eod_date = eod_data.get("date", "")
            intervals = eod_data.get("intervals", {})
            data_timestamps["eod_interval_picks"] = eod_date

            # Determine data freshness — only use if from today or yesterday
            today_str = datetime.now().strftime("%Y-%m-%d")
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            # Also accept 2-day-old data for Monday mornings (Friday's data)
            three_days_ago_str = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

            if eod_date >= three_days_ago_str:
                # Aggregate all unique symbols across all intervals.
                # Track how many intervals each symbol appears in (persistence)
                # — symbols in more intervals have stronger conviction.
                eod_candidates: Dict[str, Dict[str, Any]] = {}
                interval_counts: Dict[str, int] = {}
                for interval_key, interval_data in intervals.items():
                    for pick in interval_data.get("picks", []):
                        sym = pick.get("symbol", "")
                        if not sym:
                            continue
                        score = pick.get("score", 0)
                        interval_counts[sym] = interval_counts.get(sym, 0) + 1

                        existing = eod_candidates.get(sym)
                        if existing is None or score > existing.get("score", 0):
                            eod_candidates[sym] = {
                                "symbol": sym,
                                "score": score,
                                "price": pick.get("current_price", 0) or pick.get("entry_price", 0),
                                "signals": pick.get("signals", []),
                                "signal_types": [pick.get("engine", "")],
                                "option_type": "call",
                                "target_return": 0,
                                "engine": f"Moonshot (interval:{interval_key})",
                                "sector": "",
                                "volume_ratio": pick.get("weighted_rvol", 0) or pick.get("rvol", 0),
                                "short_interest": pick.get("short_float", 0),
                                "action": pick.get("action", ""),
                                "entry_low": pick.get("entry_price", 0),
                                "entry_high": pick.get("entry_price", 0),
                                "target": 0,
                                "stop": 0,
                                "rsi": 50,
                                "uw_sentiment": "",
                                "data_source": f"eod_interval_picks ({eod_date})",
                                "data_age_days": (datetime.now() - datetime.strptime(eod_date, "%Y-%m-%d")).days if eod_date else -1,
                                "velocity_score": pick.get("velocity_score", 0),
                                "rs_acceleration": pick.get("rs_acceleration", 0),
                            }

                # Attach interval persistence count to each candidate
                for sym, cand in eod_candidates.items():
                    cand["interval_persistence"] = interval_counts.get(sym, 1)

                results.extend(eod_candidates.values())
                logger.info(
                    f"🟢 TradeNova eod_interval_picks: {len(eod_candidates)} unique symbols "
                    f"from {len(intervals)} intervals (date: {eod_date})"
                )
            else:
                logger.info(
                    f"🟢 eod_interval_picks.json skipped — data from {eod_date} is too old "
                    f"(today: {today_str})"
                )
    except Exception as e:
        logger.debug(f"Failed to read eod_interval_picks.json: {e}")

    # --- Source 1: final_recommendations.json (best curated source) ---
    try:
        recs_file = Path(TRADENOVA_PATH) / "data" / "final_recommendations.json"
        if recs_file.exists():
            with open(recs_file) as f:
                data = json.load(f)
            recs = data.get("recommendations", [])
            scan_label = data.get("scan_label", "")
            generated = data.get("generated_at", "")
            data_timestamps["final_recommendations"] = generated
            logger.info(f"🟢 TradeNova cached: {len(recs)} recommendations from '{scan_label}' ({generated})")

            for rec in recs:
                results.append({
                    "symbol": rec.get("symbol", ""),
                    "score": rec.get("composite_score", 0) / 100.0,  # Normalize 0-100 → 0-1
                    "price": rec.get("current_price", 0),
                    "signals": rec.get("signals", []),
                    "signal_types": rec.get("engines", []),
                    "option_type": "call",
                    "target_return": 0,
                    "engine": "Moonshot",
                    "sector": "",
                    "volume_ratio": 0,
                    "short_interest": 0,
                    "conviction": rec.get("conviction", 0),
                    "entry_low": rec.get("entry_low", 0),
                    "entry_high": rec.get("entry_high", 0),
                    "target": rec.get("target", 0),
                    "stop": rec.get("stop", 0),
                    "why": rec.get("why", ""),
                    "rsi": rec.get("rsi", 50),
                    "uw_sentiment": rec.get("uw_sentiment", ""),
                    "data_source": f"final_recommendations ({generated})",
                    "data_age_days": _calc_data_age_days(generated),
                })
    except Exception as e:
        logger.debug(f"Failed to read final_recommendations.json: {e}")

    # --- Source 2: final_recommendations_history.json (supplement) ---
    if len(results) < top_n:
        try:
            hist_file = Path(TRADENOVA_PATH) / "data" / "final_recommendations_history.json"
            if hist_file.exists():
                with open(hist_file) as f:
                    history = json.load(f)
                # Get unique symbols from history that aren't already in results
                existing_symbols = {r["symbol"] for r in results}
                for entry in reversed(history):  # Most recent first
                    hist_gen = entry.get("generated_at", "")
                    for rec in entry.get("recommendations", []):
                        sym = rec.get("symbol", "")
                        if sym and sym not in existing_symbols:
                            results.append({
                                "symbol": sym,
                                "score": rec.get("composite_score", 0) / 100.0,
                                "price": rec.get("current_price", 0),
                                "signals": rec.get("signals", []),
                                "signal_types": rec.get("engines", []),
                                "option_type": "call",
                                "target_return": 0,
                                "engine": "Moonshot",
                                "sector": "",
                                "volume_ratio": 0,
                                "short_interest": 0,
                                "conviction": rec.get("conviction", 0),
                                "why": rec.get("why", ""),
                                "rsi": rec.get("rsi", 50),
                                "uw_sentiment": rec.get("uw_sentiment", ""),
                                "data_source": f"final_recommendations_history ({hist_gen})",
                                "data_age_days": _calc_data_age_days(hist_gen),
                            })
                            existing_symbols.add(sym)
        except Exception as e:
            logger.debug(f"Failed to read history: {e}")

    # --- Source 3: moonshot CSV data (oldest fallback) ---
    if len(results) < top_n:
        try:
            import pandas as pd
            data_dir = Path(TRADENOVA_PATH) / "moonshot" / "data_collection"
            csv_files = sorted(data_dir.glob("moonshot_daily_*.csv"), reverse=True)
            if csv_files:
                csv_name = csv_files[0].name
                df = pd.read_csv(csv_files[0])
                existing_symbols = {r["symbol"] for r in results}
                for _, row in df.iterrows():
                    sym = row.get("symbol", "")
                    if sym and sym not in existing_symbols:
                        results.append({
                            "symbol": sym,
                            "score": row.get("moonshot_score", 0) / 100.0,
                            "price": row.get("stock_price", 0),
                            "signals": [],
                            "signal_types": [],
                            "option_type": row.get("option_type", "call"),
                            "target_return": 0,
                            "engine": "Moonshot (csv)",
                            "sector": "",
                            "volume_ratio": 0,
                            "short_interest": 0,
                            "data_source": f"moonshot CSV ({csv_name})",
                            "data_age_days": -1,
                        })
                        existing_symbols.add(sym)
        except Exception as e:
            logger.debug(f"Failed to read CSV fallback: {e}")

    # Deduplicate — keep the entry with the highest score per symbol
    seen = {}
    for r in results:
        sym = r["symbol"]
        if sym not in seen or r["score"] > seen[sym]["score"]:
            seen[sym] = r

    # Sort with multi-factor tie-breaker:
    #   1. Score (primary — higher is better)
    #   2. Interval persistence (how many scans flagged this symbol — higher = stronger conviction)
    #   3. Conviction from final_recommendations (if available)
    #   4. Volume ratio (higher = more interesting)
    deduped = sorted(
        seen.values(),
        key=lambda x: (
            x.get("score", 0) or 0,
            x.get("interval_persistence", 0) or 0,
            x.get("conviction", 0) or 0,
            x.get("volume_ratio", 0) or 0,
        ),
        reverse=True,
    )

    top_picks = deduped[:top_n]

    # Log data freshness for transparency
    sources_used = set()
    for p in top_picks:
        ds = p.get("data_source", "unknown")
        sources_used.add(ds.split(" (")[0] if " (" in ds else ds)
    logger.info(
        f"🟢 Moonshot (cached): {len(deduped)} total candidates, "
        f"returning top {top_n}. Sources: {', '.join(sorted(sources_used))}"
    )
    for i, p in enumerate(top_picks, 1):
        age = p.get("data_age_days", -1)
        age_tag = f" [DATA AGE: {age}d]" if age >= 0 else ""
        logger.info(
            f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — "
            f"${p.get('price', 0):.2f}{age_tag}"
        )

    return top_picks


def _calc_data_age_days(timestamp_str: str) -> int:
    """
    Calculate how many days old a timestamp string is.
    Accepts ISO format or common date formats.
    Returns -1 if parsing fails.
    """
    if not timestamp_str:
        return -1
    try:
        # Try ISO format first
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        age = (datetime.now() - ts.replace(tzinfo=None)).days
        return max(0, age)
    except (ValueError, TypeError):
        pass
    # Try date-only format
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            ts = datetime.strptime(timestamp_str[:10], fmt)
            age = (datetime.now() - ts).days
            return max(0, age)
        except (ValueError, TypeError):
            continue
    return -1


def get_moonshot_universe() -> List[str]:
    """Get the Moonshot Engine's default symbol universe."""
    try:
        from moonshot.run_daily_scan import DEFAULT_SYMBOLS
        return DEFAULT_SYMBOLS
    except ImportError:
        # Fallback default symbols
        return [
            'NVDA', 'AAPL', 'TSLA', 'MSFT', 'META', 'AMZN', 'GOOG', 'AMD', 'AVGO', 'MU',
            'SMCI', 'COIN', 'MSTR', 'PLTR', 'HOOD',
            'GME', 'AMC', 'BBBY', 'RKLB', 'IONQ',
            'SOFI', 'RIVN', 'LCID', 'NIO', 'SNOW',
        ]
