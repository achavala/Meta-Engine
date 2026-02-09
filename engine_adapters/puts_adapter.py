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
    Fallback: Read from PutsEngine's cached scan results file.
    Used when the live scan fails (e.g., API rate limits, market closed).
    """
    try:
        results_file = Path(PUTSENGINE_PATH) / "scheduled_scan_results.json"
        if not results_file.exists():
            logger.warning("No cached PutsEngine results found")
            return []
        
        with open(results_file) as f:
            data = json.load(f)
        
        all_candidates = []
        
        for engine_key in ["gamma_drain", "distribution", "liquidity"]:
            for c in data.get(engine_key, []):
                all_candidates.append({
                    "symbol": c.get("symbol", ""),
                    "score": c.get("score", 0),
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
        
        # Deduplicate by symbol (keep highest score)
        seen = {}
        for c in all_candidates:
            sym = c["symbol"]
            if sym not in seen or c["score"] > seen[sym]["score"]:
                seen[sym] = c
        
        deduped = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
        
        logger.info(f"🔴 PutsEngine (cached): {len(deduped)} candidates, returning top {top_n}")
        return deduped[:top_n]
        
    except Exception as e:
        logger.error(f"Failed to read cached PutsEngine results: {e}")
        return []


def get_puts_universe() -> List[str]:
    """Get the full PutsEngine ticker universe."""
    try:
        from putsengine.config import EngineConfig
        return sorted(EngineConfig.get_all_tickers())
    except ImportError:
        logger.warning("Cannot import PutsEngine config, returning empty universe")
        return []
