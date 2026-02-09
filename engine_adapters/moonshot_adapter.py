"""
Moonshot/TradeNova Engine Adapter
==================================
Interface to get Top 10 moonshot candidates from the TradeNova Moonshot Engine.
Imports Moonshot modules directly without modifying the original codebase.
"""

import sys
import json
from pathlib import Path
from datetime import datetime
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
      1. final_recommendations.json (most recent ranked picks)
      2. final_recommendations_history.json (historical picks)
      3. moonshot/data_collection/*.csv (raw moonshot scans)
    """
    results = []

    # --- Source 1: final_recommendations.json (best source) ---
    try:
        recs_file = Path(TRADENOVA_PATH) / "data" / "final_recommendations.json"
        if recs_file.exists():
            with open(recs_file) as f:
                data = json.load(f)
            recs = data.get("recommendations", [])
            scan_label = data.get("scan_label", "")
            generated = data.get("generated_at", "")
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
                        })
                        existing_symbols.add(sym)
        except Exception as e:
            logger.debug(f"Failed to read CSV fallback: {e}")

    # Deduplicate and sort
    seen = {}
    for r in results:
        sym = r["symbol"]
        if sym not in seen or r["score"] > seen[sym]["score"]:
            seen[sym] = r

    deduped = sorted(seen.values(), key=lambda x: x["score"], reverse=True)

    logger.info(f"🟢 Moonshot (cached): {len(deduped)} total candidates, returning top {top_n}")
    return deduped[:top_n]


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
