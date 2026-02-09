"""
PutsEngine Adapter
==================
Interface to get Top 10 PUT candidates from the PutsEngine system.
Imports PutsEngine modules directly without modifying the original codebase.
"""

import sys
import asyncio
import json
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Add PutsEngine to path
PUTSENGINE_PATH = str(Path.home() / "PutsEngine")
if PUTSENGINE_PATH not in sys.path:
    sys.path.insert(0, PUTSENGINE_PATH)


def get_top_puts(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Get the Top N PUT candidates from PutsEngine.
    
    Uses PutsEngine's internal scanning pipeline to find the best
    put candidates sorted by composite score.
    
    Args:
        top_n: Number of top picks to return (default 10)
        
    Returns:
        List of dicts with keys: symbol, score, price, signals, engine_type, etc.
    """
    try:
        from putsengine.config import EngineConfig, get_settings
        from putsengine.engine import PutsEngine
        
        logger.info(f"🔴 PutsEngine: Scanning {len(EngineConfig.get_all_tickers())} tickers...")
        
        settings = get_settings()
        engine = PutsEngine(settings)
        
        # Get all tickers from PutsEngine universe
        all_tickers = sorted(EngineConfig.get_all_tickers())
        
        results = []
        
        async def _scan_all():
            """Async scan of all tickers."""
            nonlocal results
            scanned = 0
            errors = 0
            
            for symbol in all_tickers:
                try:
                    candidate = await engine.run_single_symbol(symbol)
                    scanned += 1
                    
                    if candidate and candidate.composite_score > 0:
                        result = {
                            "symbol": symbol,
                            "score": candidate.composite_score,
                            "price": candidate.current_price,
                            "passed_gates": candidate.passed_all_gates,
                            "distribution_score": candidate.distribution_score,
                            "dealer_score": candidate.dealer_score,
                            "liquidity_score": candidate.liquidity_score,
                            "signals": [],
                            "block_reasons": [r.value for r in candidate.block_reasons] if candidate.block_reasons else [],
                            "engine": "PutsEngine",
                            "engine_type": "N/A",
                        }
                        
                        # Extract signals
                        if candidate.distribution and hasattr(candidate.distribution, 'signals'):
                            result["signals"] = list(candidate.distribution.signals.keys()) if isinstance(candidate.distribution.signals, dict) else candidate.distribution.signals
                        
                        # Get engine type
                        if candidate.acceleration:
                            et = getattr(candidate.acceleration, 'engine_type', 'N/A')
                            result["engine_type"] = et.value if hasattr(et, 'value') else str(et)
                        
                        results.append(result)
                        
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        logger.debug(f"  Skip {symbol}: {str(e)[:60]}")
            
            logger.info(f"  Scanned {scanned}/{len(all_tickers)}, errors: {errors}")
        
        # Run async scan
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_scan_all())
        finally:
            loop.close()
        
        # Sort by composite score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        
        top_picks = results[:top_n]
        logger.info(f"🔴 PutsEngine: Top {len(top_picks)} picks selected")
        for i, p in enumerate(top_picks, 1):
            logger.info(f"  #{i} {p['symbol']} — Score: {p['score']:.3f} — ${p['price']:.2f}")
        
        return top_picks
        
    except Exception as e:
        logger.error(f"PutsEngine scan failed: {e}")
        return _fallback_from_cached_results(top_n)


def _fallback_from_cached_results(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Multi-tier fallback: Read from PutsEngine's cached data files.
    
    Tier 1: scheduled_scan_results.json (latest market_open scan)
    Tier 2: scan_history.json (most recent scan with actual picks)
    Tier 3: logs/convergence/latest_top9.json (convergence pipeline - richest data)
    Tier 4: pattern_scan_results.json (pattern-based scans)
    
    Used when the live scan fails (e.g., pydantic conflict, API rate limits).
    """
    all_candidates = []
    source_used = "none"
    
    # ── Tier 1: scheduled_scan_results.json ──────────────────────────
    try:
        results_file = Path(PUTSENGINE_PATH) / "scheduled_scan_results.json"
        if results_file.exists():
            with open(results_file) as f:
                data = json.load(f)
            
            for engine_key in ["gamma_drain", "distribution", "liquidity"]:
                for c in data.get(engine_key, []):
                    all_candidates.append({
                        "symbol": c.get("symbol", ""),
                        "score": c.get("score", c.get("composite_score", 0)),
                        "price": c.get("current_price", 0) or c.get("close", 0),
                        "passed_gates": True,
                        "distribution_score": c.get("distribution_score", 0),
                        "dealer_score": c.get("dealer_score", 0),
                        "liquidity_score": c.get("liquidity_score", 0),
                        "signals": c.get("signals", []),
                        "block_reasons": [],
                        "engine": f"PutsEngine ({engine_key})",
                        "engine_type": engine_key,
                    })
            
            if all_candidates:
                source_used = "scheduled_scan_results.json"
                logger.info(f"  Tier 1 (scheduled_scan): {len(all_candidates)} candidates")
    except Exception as e:
        logger.debug(f"  Tier 1 failed: {e}")
    
    # ── Tier 2: scan_history.json (most recent scan with picks) ──────
    if not all_candidates:
        try:
            history_file = Path(PUTSENGINE_PATH) / "scan_history.json"
            if history_file.exists():
                with open(history_file) as f:
                    history = json.load(f)
                
                scans = history.get("scans", [])
                # Find the most recent scan with actual picks
                for scan in reversed(scans):
                    gamma = scan.get("gamma_drain", [])
                    dist = scan.get("distribution", [])
                    liq = scan.get("liquidity", [])
                    total_picks = len(gamma) + len(dist) + len(liq)
                    
                    if total_picks > 0:
                        scan_ts = scan.get("timestamp", "unknown")
                        logger.info(f"  Tier 2: Found scan with {total_picks} picks from {scan_ts}")
                        
                        for engine_key, picks in [("gamma_drain", gamma), ("distribution", dist), ("liquidity", liq)]:
                            for c in picks:
                                all_candidates.append({
                                    "symbol": c.get("symbol", ""),
                                    "score": c.get("score", c.get("composite_score", 0)),
                                    "price": c.get("current_price", 0) or c.get("close", 0),
                                    "passed_gates": True,
                                    "distribution_score": c.get("distribution_score", 0),
                                    "dealer_score": c.get("dealer_score", 0),
                                    "liquidity_score": c.get("liquidity_score", 0),
                                    "signals": c.get("signals", []),
                                    "block_reasons": [],
                                    "engine": f"PutsEngine ({engine_key})",
                                    "engine_type": engine_key,
                                    "scan_timestamp": scan_ts,
                                })
                        
                        source_used = f"scan_history.json ({scan_ts})"
                        break
        except Exception as e:
            logger.debug(f"  Tier 2 failed: {e}")
    
    # ── Tier 3: convergence/latest_top9.json (richest data) ──────────
    if not all_candidates:
        try:
            conv_file = Path(PUTSENGINE_PATH) / "logs" / "convergence" / "latest_top9.json"
            if conv_file.exists():
                with open(conv_file) as f:
                    conv = json.load(f)
                
                generated = conv.get("generated_at_et", "unknown")
                for c in conv.get("top9", []):
                    gamma_sigs = c.get("gamma_signals", [])
                    all_candidates.append({
                        "symbol": c.get("symbol", ""),
                        "score": c.get("convergence_score", 0),
                        "price": c.get("current_price", 0),
                        "passed_gates": True,
                        "distribution_score": c.get("ews_score", 0),
                        "dealer_score": c.get("gamma_score", 0),
                        "liquidity_score": 0,
                        "signals": gamma_sigs,
                        "block_reasons": [],
                        "engine": f"PutsEngine (convergence)",
                        "engine_type": c.get("gamma_engine", "convergence"),
                        "convergence_data": {
                            "sources_agreeing": c.get("sources_agreeing", 0),
                            "source_list": c.get("source_list", []),
                            "weather_forecast": c.get("weather_forecast", ""),
                            "expected_drop": c.get("expected_drop", ""),
                            "timing": c.get("timing", ""),
                            "recommendation": c.get("ews_recommendation", ""),
                            "days_on_list": c.get("days_on_list", 0),
                        },
                    })
                
                if all_candidates:
                    source_used = f"convergence/latest_top9.json ({generated})"
                    logger.info(f"  Tier 3 (convergence): {len(all_candidates)} picks from {generated}")
        except Exception as e:
            logger.debug(f"  Tier 3 failed: {e}")
    
    # ── Tier 4: pattern_scan_results.json ────────────────────────────
    if not all_candidates:
        try:
            pattern_file = Path(PUTSENGINE_PATH) / "pattern_scan_results.json"
            if pattern_file.exists():
                with open(pattern_file) as f:
                    patterns = json.load(f)
                
                for pat_type in ["pump_reversal", "two_day_rally", "high_vol_run"]:
                    for c in patterns.get(pat_type, []):
                        sym = c.get("symbol", c.get("ticker", ""))
                        if sym:
                            all_candidates.append({
                                "symbol": sym,
                                "score": c.get("score", c.get("confidence", 0.5)),
                                "price": c.get("current_price", 0) or c.get("close", 0) or c.get("price", 0),
                                "passed_gates": True,
                                "distribution_score": 0,
                                "dealer_score": 0,
                                "liquidity_score": 0,
                                "signals": [pat_type],
                                "block_reasons": [],
                                "engine": f"PutsEngine ({pat_type})",
                                "engine_type": pat_type,
                            })
                
                if all_candidates:
                    scan_time = patterns.get("scan_time", "unknown")
                    source_used = f"pattern_scan_results.json ({scan_time})"
                    logger.info(f"  Tier 4 (patterns): {len(all_candidates)} candidates")
        except Exception as e:
            logger.debug(f"  Tier 4 failed: {e}")
    
    # ── Deduplicate and return ───────────────────────────────────────
    if not all_candidates:
        logger.warning("No PutsEngine data found across all tiers")
        return []
    
    seen = {}
    for c in all_candidates:
        sym = c["symbol"]
        if sym and (sym not in seen or c["score"] > seen[sym]["score"]):
            seen[sym] = c
    
    deduped = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
    
    logger.info(f"🔴 PutsEngine (cached via {source_used}): {len(deduped)} candidates, returning top {top_n}")
    return deduped[:top_n]


def get_puts_universe() -> List[str]:
    """Get the full PutsEngine ticker universe."""
    try:
        from putsengine.config import EngineConfig
        return sorted(EngineConfig.get_all_tickers())
    except ImportError:
        logger.warning("Cannot import PutsEngine config, returning empty universe")
        return []
