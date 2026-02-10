"""
Cross-Engine Analyzer
=====================
Runs Top 10 picks from each engine through the opposite engine:
  - PutsEngine Top 10 → Moonshot analysis (bullish signal check)
  - Moonshot Top 10 → PutsEngine analysis (bearish signal check)

This creates a "conflict matrix" showing where engines agree/disagree.

The PutsEngine analysis lens replicates institutional-grade bearish
signal detection when the live PutsEngine is unavailable, using:
  - 30-day daily bars for pattern analysis
  - RSI (14-period), EMA20, RVOL calculations
  - Distribution signals: multi-day weakness, lower highs, gap patterns
  - Liquidity signals: VWAP positioning, volume anomalies
  - Technical alignment: support/resistance, momentum divergence
"""

import sys
import asyncio
import json
import math
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging

import requests

logger = logging.getLogger(__name__)

# Paths
PUTSENGINE_PATH = str(Path.home() / "PutsEngine")
TRADENOVA_PATH = str(Path.home() / "TradeNova")

for p in [PUTSENGINE_PATH, TRADENOVA_PATH]:
    if p not in sys.path:
        sys.path.insert(0, p)


# ---------------------------------------------------------------------------
# Rich Market Data Fetcher (30-day bars + snapshot)
# ---------------------------------------------------------------------------

def _get_market_data(symbol: str, api_key: str) -> Dict[str, Any]:
    """
    Fetch comprehensive market data for a symbol using Polygon API.
    Returns price, volume, change, plus 30-day daily bars for deep analysis.
    """
    data = {
        "symbol": symbol,
        "price": 0, "open": 0, "high": 0, "low": 0,
        "change_pct": 0, "volume": 0, "rsi": 50,
        "daily_bars": [],       # List of OHLCV dicts (last 30 days)
        "ema20": 0,
        "rvol": 1.0,            # Relative volume (vs 20-day avg)
        "avg_volume_20d": 0,
        "vwap": 0,
    }

    if not api_key:
        return data

    try:
        # --- 1. Fetch 30-day daily bars ---
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start_date}/{end_date}"
        )
        resp = requests.get(
            url,
            params={"adjusted": "true", "sort": "asc", "limit": 50, "apiKey": api_key},
            timeout=15,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                bars = []
                for r in results:
                    bars.append({
                        "o": r.get("o", 0),
                        "h": r.get("h", 0),
                        "l": r.get("l", 0),
                        "c": r.get("c", 0),
                        "v": r.get("v", 0),
                        "vw": r.get("vw", 0),
                        "t": r.get("t", 0),
                    })
                data["daily_bars"] = bars

                # Use most-recent bar for current data
                latest = bars[-1]
                data["price"] = latest["c"]
                data["open"] = latest["o"]
                data["high"] = latest["h"]
                data["low"] = latest["l"]
                data["volume"] = latest["v"]
                data["vwap"] = latest.get("vw", 0)

                if latest["o"] > 0:
                    data["change_pct"] = ((latest["c"] - latest["o"]) / latest["o"]) * 100

                # --- Calculate 20-day avg volume & RVOL ---
                if len(bars) >= 21:
                    vol_window = [b["v"] for b in bars[-21:-1]]
                    avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1
                    data["avg_volume_20d"] = avg_vol
                    data["rvol"] = latest["v"] / avg_vol if avg_vol > 0 else 1.0

                # --- Calculate EMA-20 ---
                closes = [b["c"] for b in bars]
                if len(closes) >= 20:
                    data["ema20"] = _calc_ema(closes, 20)

                # --- Calculate RSI-14 ---
                if len(closes) >= 15:
                    data["rsi"] = _calc_rsi(closes, 14)

    except Exception as e:
        logger.debug(f"Failed to get 30-day bars for {symbol}: {e}")

    # Fallback: if daily_bars fetch failed, try previous-close endpoint
    if data["price"] == 0:
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
            resp = requests.get(url, params={"apiKey": api_key}, timeout=10)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    bar = results[0]
                    data["price"] = bar.get("c", 0)
                    data["open"] = bar.get("o", 0)
                    data["high"] = bar.get("h", 0)
                    data["low"] = bar.get("l", 0)
                    data["volume"] = bar.get("v", 0)
                    if bar.get("o", 0) > 0:
                        data["change_pct"] = ((bar["c"] - bar["o"]) / bar["o"]) * 100
        except Exception as e:
            logger.debug(f"Fallback prev-close also failed for {symbol}: {e}")

    return data


# ---------------------------------------------------------------------------
# Technical Indicator Helpers
# ---------------------------------------------------------------------------

def _calc_ema(prices: List[float], period: int) -> float:
    """Calculate Exponential Moving Average."""
    if len(prices) < period:
        return prices[-1] if prices else 0
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period  # SMA seed
    for p in prices[period:]:
        ema = (p - ema) * multiplier + ema
    return ema


def _calc_rsi(prices: List[float], period: int = 14) -> float:
    """Calculate RSI (Relative Strength Index) — returns last value."""
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _load_puts_cached_data() -> Dict[str, Dict]:
    """Load PutsEngine cached scan results into a symbol lookup dict."""
    cache = {}
    try:
        results_file = Path(PUTSENGINE_PATH) / "scheduled_scan_results.json"
        if results_file.exists():
            with open(results_file) as f:
                data = json.load(f)
            for engine_key in ["gamma_drain", "distribution", "liquidity"]:
                for c in data.get(engine_key, []):
                    sym = c.get("symbol", "")
                    score = c.get("score", 0)
                    # Keep highest score per symbol
                    if sym and (sym not in cache or score > cache[sym].get("score", 0)):
                        cache[sym] = {
                            "score": score,
                            "signals": c.get("signals", []),
                            "engine_type": engine_key,
                            "price": c.get("current_price", 0) or c.get("close", 0),
                        }
    except Exception as e:
        logger.debug(f"Failed to load PutsEngine cache: {e}")
    return cache

# Module-level cache (loaded once)
_puts_cache = None


def _get_puts_cache() -> Dict[str, Dict]:
    global _puts_cache
    if _puts_cache is None:
        _puts_cache = _load_puts_cached_data()
    return _puts_cache


def _analyze_with_puts_lens(symbol: str, market_data: Dict) -> Dict[str, Any]:
    """
    Run a symbol through the PutsEngine analytical lens.

    Three-tier resolution:
      1. Cached PutsEngine scan results (instant, highest fidelity)
      2. Live PutsEngine analysis (most accurate, may fail due to deps)
      3. Standalone institutional-grade analysis using Polygon 30-day bars
         — replicates PutsEngine scoring: distribution, RVOL, RSI, EMA20,
           VWAP, multi-day weakness, lower-highs, gap patterns, and more.
    """
    result = {
        "symbol": symbol,
        "engine": "PutsEngine",
        "bearish_score": 0.0,
        "signals": [],
        "analysis": "",
        "risk_level": "LOW",
    }

    # ── TIER 1: Cached PutsEngine data (fastest, no import needed) ────────
    puts_cache = _get_puts_cache()
    if symbol in puts_cache:
        cached = puts_cache[symbol]
        score = cached["score"]
        result["bearish_score"] = score
        result["signals"] = cached.get("signals", [])
        result["distribution_score"] = score
        result["dealer_score"] = score * 0.8
        result["liquidity_score"] = score * 0.7

        if score >= 0.60:
            result["risk_level"] = "HIGH"
            result["analysis"] = (
                f"⚠️ HIGH bearish risk — PutsEngine cached score {score:.2f} "
                f"({cached['engine_type']}). Signals: {', '.join(cached['signals'][:3])}. "
                f"Distribution + liquidity signals active."
            )
        elif score >= 0.40:
            result["risk_level"] = "MODERATE"
            result["analysis"] = (
                f"🟡 Moderate bearish risk — PutsEngine cached score {score:.2f} "
                f"({cached['engine_type']}). Some distribution signals present."
            )
        else:
            result["risk_level"] = "LOW"
            result["analysis"] = (
                f"🟢 Low bearish risk — PutsEngine cached score {score:.2f}. "
                f"No significant distribution detected."
            )
        return result

    # ── TIER 2: Live PutsEngine analysis ──────────────────────────────────
    try:
        from putsengine.config import get_settings, EngineConfig
        from putsengine.engine import PutsEngine

        settings = get_settings()
        engine = PutsEngine(settings)

        async def _run():
            candidate = await engine.run_single_symbol(symbol)
            if candidate:
                result["bearish_score"] = candidate.composite_score
                result["distribution_score"] = candidate.distribution_score
                result["dealer_score"] = candidate.dealer_score
                result["liquidity_score"] = candidate.liquidity_score

                if candidate.distribution and hasattr(candidate.distribution, 'signals'):
                    sigs = candidate.distribution.signals
                    result["signals"] = list(sigs.keys()) if isinstance(sigs, dict) else sigs

                if candidate.composite_score >= 0.60:
                    result["risk_level"] = "HIGH"
                    result["analysis"] = (
                        f"⚠️ HIGH bearish risk — PutsEngine score "
                        f"{candidate.composite_score:.2f}. "
                        f"Distribution + liquidity signals active."
                    )
                elif candidate.composite_score >= 0.40:
                    result["risk_level"] = "MODERATE"
                    result["analysis"] = (
                        f"🟡 Moderate bearish risk — PutsEngine score "
                        f"{candidate.composite_score:.2f}. "
                        f"Some distribution signals present."
                    )
                else:
                    result["risk_level"] = "LOW"
                    result["analysis"] = (
                        f"🟢 Low bearish risk — PutsEngine score "
                        f"{candidate.composite_score:.2f}. "
                        f"No significant distribution detected."
                    )

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_run())
            if result["analysis"]:      # live scan succeeded
                return result
        finally:
            loop.close()

    except Exception as e:
        logger.debug(f"PutsEngine live analysis for {symbol} failed: {e}")

    # ── TIER 3: Standalone institutional-grade analysis (Polygon bars) ────
    return _standalone_puts_analysis(symbol, market_data, result)


# ---------------------------------------------------------------------------
# Standalone PutsEngine-Grade Analysis (Tier 3)
# ---------------------------------------------------------------------------
# Replicates the core PutsEngine scoring methodology using 30-day daily
# bars from Polygon.  Weights mirror the real PutsEngine scorer:
#
#   Distribution Quality   30%   (price-volume contradictions)
#   Technical Alignment    20%   (RSI, EMA20, VWAP positioning)
#   Volume Analysis        20%   (RVOL, volume trend)
#   Momentum / Trend       15%   (multi-day weakness, consecutive reds)
#   Pattern Recognition    15%   (gap patterns, failed breakout, lower highs)
#
# Minimum actionable score: 0.40 (MODERATE)   |   High conviction: 0.60+
# ---------------------------------------------------------------------------

def _standalone_puts_analysis(
    symbol: str,
    market_data: Dict,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Run a full standalone bearish analysis using 30-day daily bars.
    This is the Tier-3 fallback when both cached data and live PutsEngine
    are unavailable.  It provides a detailed, actionable assessment.
    """
    bars = market_data.get("daily_bars", [])
    price = market_data.get("price", 0)
    change = market_data.get("change_pct", 0)

    if not bars or len(bars) < 10:
        # Not enough data — still give basic assessment from prev-close
        return _basic_price_assessment(symbol, market_data, result)

    # ── Collect individual signal scores ──────────────────────────────

    signals_detected: List[str] = []
    sub_scores: Dict[str, float] = {}

    # --- A. Distribution Quality (30% weight) ---
    dist_score = 0.0

    # A1. High RVOL on red day (strongest distribution signal)
    rvol = market_data.get("rvol", 1.0)
    latest = bars[-1]
    is_red = latest["c"] < latest["o"]
    is_down_day = latest["c"] < bars[-2]["c"] if len(bars) >= 2 else is_red

    if rvol >= 2.0 and (is_red or is_down_day):
        dist_score += 0.35
        signals_detected.append(f"high_rvol_red_day (RVOL {rvol:.1f}x)")
    elif rvol >= 1.5 and change < -1.5:
        dist_score += 0.25
        signals_detected.append(f"elevated_rvol_selling (RVOL {rvol:.1f}x, {change:+.1f}%)")
    elif rvol >= 1.3 and change < -3.0:
        dist_score += 0.20
        signals_detected.append(f"rvol_distribution ({rvol:.1f}x on {change:+.1f}%)")

    # A2. Flat price + rising volume (stealth distribution)
    if len(bars) >= 5:
        recent5 = bars[-5:]
        closes = [b["c"] for b in recent5]
        price_range_pct = (max(closes) - min(closes)) / (sum(closes) / len(closes)) if closes else 0
        vol0, vol4 = recent5[0]["v"], recent5[-1]["v"]
        vol_trend = (vol4 - vol0) / vol0 if vol0 > 0 else 0
        if price_range_pct < 0.02 and vol_trend > 0.20:
            dist_score += 0.15
            signals_detected.append("flat_price_rising_volume")

    # A3. Gap down no recovery
    if len(bars) >= 2:
        prev_bar, curr_bar = bars[-2], bars[-1]
        gap_pct = (curr_bar["o"] - prev_bar["c"]) / prev_bar["c"] if prev_bar["c"] else 0
        if gap_pct < -0.01 and curr_bar["c"] <= curr_bar["o"] and curr_bar["c"] < prev_bar["c"]:
            dist_score += 0.20
            signals_detected.append(f"gap_down_no_recovery ({gap_pct*100:.1f}%)")

    # A4. Gap UP reversal (distribution trap)
    if len(bars) >= 21:
        prev_bar, curr_bar = bars[-2], bars[-1]
        gap_up = (curr_bar["o"] - prev_bar["c"]) / prev_bar["c"] if prev_bar["c"] else 0
        intraday_drop = (curr_bar["c"] - curr_bar["o"]) / curr_bar["o"] if curr_bar["o"] else 0
        if gap_up >= 0.01 and intraday_drop <= -0.02 and rvol >= 1.3:
            dist_score += 0.25
            signals_detected.append(f"gap_up_reversal_trap (gap +{gap_up*100:.1f}%, "
                                    f"reversed {intraday_drop*100:.1f}%)")

    dist_score = min(dist_score, 1.0)
    sub_scores["distribution_quality"] = dist_score

    # --- B. Technical Alignment (20% weight) ---
    tech_score = 0.0

    # B1. Price below EMA-20
    ema20 = market_data.get("ema20", 0)
    if ema20 > 0 and price < ema20:
        pct_below = (ema20 - price) / ema20 * 100
        tech_score += min(0.30, pct_below / 10)  # up to 0.30
        signals_detected.append(f"below_EMA20 ({pct_below:.1f}% below)")

    # B2. Price below VWAP
    vwap = market_data.get("vwap", 0)
    if vwap > 0 and price < vwap:
        tech_score += 0.25
        signals_detected.append("below_VWAP")

    # B3. RSI divergence / overbought-then-declining
    rsi = market_data.get("rsi", 50)
    if rsi <= 30:
        # Already oversold — less attractive for NEW puts
        tech_score += 0.05
        signals_detected.append(f"RSI_oversold ({rsi:.0f})")
    elif rsi <= 40:
        tech_score += 0.20
        signals_detected.append(f"RSI_weak ({rsi:.0f})")
    elif rsi >= 70:
        # Overbought — potential distribution top
        tech_score += 0.15
        signals_detected.append(f"RSI_overbought_risk ({rsi:.0f})")

    # B4. Failed breakout pattern
    if len(bars) >= 20:
        highs_20d = [b["h"] for b in bars[-20:]]
        resistance = max(highs_20d)
        for b in bars[-3:]:
            if b["h"] >= resistance * 0.995 and b["c"] < resistance * 0.98:
                tech_score += 0.25
                signals_detected.append("failed_breakout")
                break

    tech_score = min(tech_score, 1.0)
    sub_scores["technical_alignment"] = tech_score

    # --- C. Volume Analysis (20% weight) ---
    vol_score = 0.0

    # C1. Relative volume
    if rvol >= 2.0:
        vol_score += 0.40
    elif rvol >= 1.5:
        vol_score += 0.30
    elif rvol >= 1.2:
        vol_score += 0.15

    # C2. Volume trend (5-day)
    if len(bars) >= 5:
        vols = [b["v"] for b in bars[-5:]]
        vol_trend_pct = (vols[-1] - vols[0]) / vols[0] if vols[0] > 0 else 0
        if vol_trend_pct > 0.5 and is_red:
            vol_score += 0.30
            signals_detected.append(f"rising_volume_on_decline (+{vol_trend_pct*100:.0f}%)")
        elif vol_trend_pct > 0.2 and is_red:
            vol_score += 0.15

    # C3. Volume spike (today vs 20d avg)
    avg_vol = market_data.get("avg_volume_20d", 0)
    if avg_vol > 0:
        today_vs_avg = latest["v"] / avg_vol
        if today_vs_avg >= 3.0 and is_red:
            vol_score += 0.30
            signals_detected.append(f"volume_spike_3x ({today_vs_avg:.1f}x avg)")

    vol_score = min(vol_score, 1.0)
    sub_scores["volume_analysis"] = vol_score

    # --- D. Momentum / Trend (15% weight) ---
    mom_score = 0.0

    # D1. Multi-day weakness (3+ consecutive lower closes)
    if len(bars) >= 5:
        consecutive_lower = 0
        for i in range(len(bars) - 1, 0, -1):
            if bars[i]["c"] < bars[i - 1]["c"]:
                consecutive_lower += 1
            else:
                break
        if consecutive_lower >= 4:
            mom_score += 0.50
            signals_detected.append(f"multi_day_weakness ({consecutive_lower} consecutive lower closes)")
        elif consecutive_lower >= 3:
            mom_score += 0.35
            signals_detected.append(f"3_day_weakness")

    # D2. Red candle count (last 5 days)
    if len(bars) >= 5:
        red_days = sum(1 for b in bars[-5:] if b["c"] < b["o"])
        if red_days >= 4:
            mom_score += 0.30
            signals_detected.append(f"bearish_pattern ({red_days}/5 red days)")
        elif red_days >= 3:
            mom_score += 0.15

    # D3. Cumulative 5-day return
    if len(bars) >= 6:
        five_day_return = (bars[-1]["c"] - bars[-6]["c"]) / bars[-6]["c"] * 100 if bars[-6]["c"] else 0
        if five_day_return < -10:
            mom_score += 0.30
            signals_detected.append(f"5d_decline ({five_day_return:+.1f}%)")
        elif five_day_return < -5:
            mom_score += 0.20
            signals_detected.append(f"5d_weakness ({five_day_return:+.1f}%)")

    mom_score = min(mom_score, 1.0)
    sub_scores["momentum_trend"] = mom_score

    # --- E. Pattern Recognition (15% weight) ---
    pat_score = 0.0

    # E1. Lower highs pattern (last 10 days)
    if len(bars) >= 10:
        recent_highs = [b["h"] for b in bars[-5:]]
        prior_highs = [b["h"] for b in bars[-10:-5]]
        if max(recent_highs) < max(prior_highs):
            pat_score += 0.30
            signals_detected.append("lower_highs_pattern")

            # E1b. Lower highs with flat/rising RSI = divergence (extra bearish)
            closes = [b["c"] for b in bars]
            if len(closes) >= 20:
                rsi_recent = _calc_rsi(closes[-15:], 14)
                rsi_prior = _calc_rsi(closes[-25:-10], 14) if len(closes) >= 25 else rsi_recent
                if rsi_recent >= rsi_prior - 5:  # RSI flat while price lower
                    pat_score += 0.20
                    signals_detected.append("lower_highs_RSI_divergence")

    # E2. Lower lows (bearish structure)
    if len(bars) >= 10:
        recent_lows = [b["l"] for b in bars[-5:]]
        prior_lows = [b["l"] for b in bars[-10:-5]]
        if min(recent_lows) < min(prior_lows):
            pat_score += 0.20
            signals_detected.append("lower_lows_formation")

    # E3. Price rejected from prior high (overhead supply)
    if len(bars) >= 20:
        max_high_20d = max(b["h"] for b in bars[-20:])
        if latest["h"] >= max_high_20d * 0.99 and latest["c"] < max_high_20d * 0.97:
            pat_score += 0.25
            signals_detected.append("rejection_at_resistance")

    # E4. Bearish engulfing pattern
    if len(bars) >= 2:
        prev_b, curr_b = bars[-2], bars[-1]
        if (prev_b["c"] > prev_b["o"] and  # previous was green
            curr_b["c"] < curr_b["o"] and   # current is red
            curr_b["o"] >= prev_b["c"] and  # opened at or above prev close
            curr_b["c"] <= prev_b["o"]):    # closed at or below prev open
            pat_score += 0.30
            signals_detected.append("bearish_engulfing")

    pat_score = min(pat_score, 1.0)
    sub_scores["pattern_recognition"] = pat_score

    # ── Weighted Composite Score ──────────────────────────────────────
    weights = {
        "distribution_quality": 0.30,
        "technical_alignment":  0.20,
        "volume_analysis":      0.20,
        "momentum_trend":       0.15,
        "pattern_recognition":  0.15,
    }

    composite = sum(sub_scores[k] * weights[k] for k in weights)
    composite = max(0.0, min(1.0, composite))

    # ── Populate result ───────────────────────────────────────────────
    result["bearish_score"] = composite
    result["signals"] = signals_detected
    result["distribution_score"] = sub_scores.get("distribution_quality", 0)
    result["technical_score"] = sub_scores.get("technical_alignment", 0)
    result["volume_score"] = sub_scores.get("volume_analysis", 0)
    result["momentum_score"] = sub_scores.get("momentum_trend", 0)
    result["pattern_score"] = sub_scores.get("pattern_recognition", 0)
    result["sub_scores"] = sub_scores

    # ── Risk Level + Narrative ────────────────────────────────────────
    n_signals = len(signals_detected)
    top_sigs = ", ".join(signals_detected[:4]) if signals_detected else "none"

    if composite >= 0.60:
        result["risk_level"] = "HIGH"
        result["analysis"] = (
            f"⚠️ HIGH bearish risk — composite {composite:.2f} "
            f"({n_signals} signals). Key: {top_sigs}. "
            f"Distribution + technical alignment confirm downside bias. "
            f"RSI {market_data.get('rsi', 50):.0f} | "
            f"RVOL {rvol:.1f}x | EMA20 ${ema20:.2f}."
        )
    elif composite >= 0.40:
        result["risk_level"] = "MODERATE"
        result["analysis"] = (
            f"🟡 MODERATE bearish risk — composite {composite:.2f} "
            f"({n_signals} signals). Key: {top_sigs}. "
            f"Some distribution present but incomplete confirmation. "
            f"RSI {market_data.get('rsi', 50):.0f} | "
            f"RVOL {rvol:.1f}x | EMA20 ${ema20:.2f}."
        )
    elif composite >= 0.20:
        result["risk_level"] = "LOW"
        result["analysis"] = (
            f"🟢 LOW bearish risk — composite {composite:.2f} "
            f"({n_signals} signals). {top_sigs if top_sigs != 'none' else 'No significant bearish signals'}. "
            f"RSI {market_data.get('rsi', 50):.0f} | "
            f"RVOL {rvol:.1f}x. No material distribution detected."
        )
    else:
        result["risk_level"] = "LOW"
        result["analysis"] = (
            f"🟢 CLEAN — composite {composite:.2f}. "
            f"No bearish signals detected from 30-day analysis. "
            f"RSI {market_data.get('rsi', 50):.0f} | "
            f"RVOL {rvol:.1f}x. Bullish thesis intact."
        )

    logger.info(
        f"  {symbol} standalone PutsEngine analysis: score={composite:.2f}, "
        f"risk={result['risk_level']}, signals={n_signals} "
        f"[dist={dist_score:.2f} tech={tech_score:.2f} "
        f"vol={vol_score:.2f} mom={mom_score:.2f} pat={pat_score:.2f}]"
    )

    return result


def _basic_price_assessment(
    symbol: str,
    market_data: Dict,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Minimal fallback when even daily bars are unavailable.
    Uses only the single prev-close bar.
    """
    change = market_data.get("change_pct", 0)
    price = market_data.get("price", 0)

    if change < -5:
        result["risk_level"] = "HIGH"
        result["bearish_score"] = 0.65
        result["analysis"] = (
            f"⚠️ Sharp decline ({change:.1f}%) at ${price:.2f} — "
            f"active distribution likely. Limited data (prev-close only)."
        )
    elif change < -2:
        result["risk_level"] = "MODERATE"
        result["bearish_score"] = 0.42
        result["analysis"] = (
            f"🟡 Moderate weakness ({change:.1f}%) at ${price:.2f} — "
            f"some selling pressure. Limited data (prev-close only)."
        )
    elif change > 5:
        result["risk_level"] = "LOW"
        result["bearish_score"] = 0.10
        result["analysis"] = (
            f"🟢 Strong rally (+{change:.1f}%) at ${price:.2f} — "
            f"no bearish pressure detected."
        )
    else:
        result["risk_level"] = "LOW"
        result["bearish_score"] = 0.15
        result["analysis"] = (
            f"🟢 Neutral price action ({change:+.1f}%) at ${price:.2f} — "
            f"no significant bearish signals."
        )

    return result


# ---------------------------------------------------------------------------
# Moonshot-Grade Analysis (3-tier: MWS cache → Recommendations → Standalone)
# ---------------------------------------------------------------------------
# Replicates the Moonshot Engine's 7-layer analysis using cached TradeNova
# data, then falls back to a standalone technical analysis.  Uses cached data
# ONLY — no new UW API calls are made.
#
# Data sources consumed (all read-only from TradeNova/data/):
#   tomorrows_forecast.json     — MWS 7-layer sensor scores (50 symbols)
#   final_recommendations.json  — Ranked picks + UW sentiment + signals
#   final_recommendations_history.json — Historical picks
#   darkpool_cache.json         — Per-symbol dark pool prints/blocks
#   uw_gex_cache.json           — GEX data (5 symbols)
#   uw_iv_term_cache.json       — IV term structure
#   uw_oi_change_cache.json     — Open interest changes
#   uw_skew_cache.json          — Risk-reversal skew
#   uw_flow_cache.json          — Options flow trades
#   Polygon 30-day daily bars   — Technical indicators (via _get_market_data)
# ---------------------------------------------------------------------------

# Module-level caches for TradeNova data (loaded once)
_mws_forecast_cache = None
_final_recs_cache = None
_uw_darkpool_cache = None
_uw_gex_cache = None
_uw_iv_term_cache = None
_uw_oi_change_cache = None
_uw_skew_cache = None
_uw_flow_cache = None
_mws_market_summary = None


def _load_json_safe(filepath: Path) -> Any:
    """Load a JSON file safely, returning empty dict on failure."""
    try:
        if filepath.exists():
            with open(filepath) as f:
                return json.load(f)
    except Exception as e:
        logger.debug(f"Failed to load {filepath.name}: {e}")
    return {}


def _get_mws_forecast_cache() -> Dict[str, Dict]:
    """Load MWS tomorrows_forecast.json into a symbol → forecast dict."""
    global _mws_forecast_cache, _mws_market_summary
    if _mws_forecast_cache is None:
        _mws_forecast_cache = {}
        data = _load_json_safe(Path(TRADENOVA_PATH) / "data" / "tomorrows_forecast.json")
        _mws_market_summary = data.get("market_summary", {})
        for fc in data.get("forecasts", []):
            sym = fc.get("symbol", "")
            if sym:
                _mws_forecast_cache[sym] = fc
        logger.debug(f"Loaded MWS forecasts for {len(_mws_forecast_cache)} symbols")
    return _mws_forecast_cache


def _get_final_recs_cache() -> Dict[str, Dict]:
    """Load final_recommendations.json + history into a symbol → rec dict."""
    global _final_recs_cache
    if _final_recs_cache is None:
        _final_recs_cache = {}
        # Primary: final_recommendations.json
        data = _load_json_safe(Path(TRADENOVA_PATH) / "data" / "final_recommendations.json")
        for rec in data.get("recommendations", []):
            sym = rec.get("symbol", "")
            if sym:
                _final_recs_cache[sym] = rec
        # Supplement from history (older data, don't override)
        hist_data = _load_json_safe(Path(TRADENOVA_PATH) / "data" / "final_recommendations_history.json")
        if isinstance(hist_data, list):
            for entry in reversed(hist_data):
                for rec in entry.get("recommendations", []):
                    sym = rec.get("symbol", "")
                    if sym and sym not in _final_recs_cache:
                        _final_recs_cache[sym] = rec
        logger.debug(f"Loaded final recs for {len(_final_recs_cache)} symbols")
    return _final_recs_cache


def _get_uw_cache(cache_name: str) -> Dict[str, Any]:
    """Load a UW cache file (darkpool, gex, iv_term, oi_change, skew, flow)."""
    cache_map = {
        "darkpool": ("darkpool_cache.json", None),
        "gex": ("uw_gex_cache.json", "data"),
        "iv_term": ("uw_iv_term_cache.json", "data"),
        "oi_change": ("uw_oi_change_cache.json", "data"),
        "skew": ("uw_skew_cache.json", "data"),
        "flow": ("uw_flow_cache.json", "flow_data"),
    }
    global _uw_darkpool_cache, _uw_gex_cache, _uw_iv_term_cache
    global _uw_oi_change_cache, _uw_skew_cache, _uw_flow_cache

    # Use module-level caches
    cache_vars = {
        "darkpool": "_uw_darkpool_cache",
        "gex": "_uw_gex_cache",
        "iv_term": "_uw_iv_term_cache",
        "oi_change": "_uw_oi_change_cache",
        "skew": "_uw_skew_cache",
        "flow": "_uw_flow_cache",
    }
    cached = globals().get(cache_vars.get(cache_name, ""), None)
    if cached is not None:
        return cached

    fname, inner_key = cache_map.get(cache_name, (None, None))
    if not fname:
        return {}
    data = _load_json_safe(Path(TRADENOVA_PATH) / "data" / fname)
    result = data.get(inner_key, data) if inner_key else data
    if not isinstance(result, dict):
        result = {}
    # Remove metadata keys
    result = {k: v for k, v in result.items() if k not in ("timestamp", "generated_at")}
    globals()[cache_vars[cache_name]] = result
    return result


def _calc_macd(prices: List[float]) -> Dict[str, float]:
    """Calculate MACD (12, 26, 9) — returns dict with macd, signal, histogram."""
    if len(prices) < 26:
        return {"macd": 0, "signal": 0, "histogram": 0}
    ema12 = _calc_ema(prices, 12)
    ema26 = _calc_ema(prices, 26)
    macd_line = ema12 - ema26

    # For signal line, compute MACD series then EMA-9
    macd_series = []
    mult12 = 2 / 13
    mult26 = 2 / 27
    e12 = sum(prices[:12]) / 12
    e26 = sum(prices[:26]) / 26
    for p in prices[26:]:
        e12 = (p - e12) * mult12 + e12
        e26 = (p - e26) * mult26 + e26
        macd_series.append(e12 - e26)

    if len(macd_series) >= 9:
        signal = sum(macd_series[:9]) / 9
        mult9 = 2 / 10
        for m in macd_series[9:]:
            signal = (m - signal) * mult9 + signal
    else:
        signal = macd_series[-1] if macd_series else 0

    histogram = macd_line - signal
    return {"macd": macd_line, "signal": signal, "histogram": histogram}


def _calc_bollinger(prices: List[float], period: int = 20, std_mult: float = 2.0) -> Dict[str, float]:
    """Calculate Bollinger Bands — returns upper, middle, lower, width, position."""
    if len(prices) < period:
        return {"upper": 0, "middle": 0, "lower": 0, "width": 0, "position": 0.5}
    window = prices[-period:]
    middle = sum(window) / period
    variance = sum((p - middle) ** 2 for p in window) / period
    std_dev = variance ** 0.5
    upper = middle + std_mult * std_dev
    lower = middle - std_mult * std_dev
    width = (upper - lower) / middle if middle > 0 else 0
    pos = (prices[-1] - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
    return {"upper": upper, "middle": middle, "lower": lower, "width": width, "position": pos}


def _calc_atr(bars: List[Dict], period: int = 14) -> float:
    """Calculate Average True Range."""
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["h"], bars[i]["l"], bars[i - 1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def _analyze_with_moonshot_lens(symbol: str, market_data: Dict) -> Dict[str, Any]:
    """
    Run a symbol through the Moonshot/TradeNova analytical lens.

    Three-tier resolution (NO new UW API calls — cache only):
      1. MWS 7-layer cached forecast (tomorrows_forecast.json) — highest fidelity
      2. Final recommendations cache (final_recommendations.json) — rich signals
      3. Standalone institutional-grade bullish analysis using Polygon 30-day bars
         — replicates Moonshot scoring: momentum, squeeze detection, technicals,
           volume expansion, RSI/MACD/BB, MA alignment, trend quality.
    """
    result = {
        "symbol": symbol,
        "engine": "Moonshot",
        "bullish_score": 0.0,
        "signals": [],
        "analysis": "",
        "opportunity_level": "LOW",
        "mws_score": 0,
        "bullish_probability": 50,
        "expected_range": [],
        "confidence": "LOW",
        "sensors": [],
        "catalysts": [],
        "sub_scores": {},
        "data_source": "",
        "uw_sentiment": "",
    }

    # ── TIER 1: MWS 7-Layer Cached Forecast ──────────────────────────────
    mws_cache = _get_mws_forecast_cache()
    if symbol in mws_cache:
        fc = mws_cache[symbol]
        mws_score = fc.get("mws_score", 0)
        bull_prob = fc.get("bullish_probability", 50)
        sensors = fc.get("sensors", [])
        confidence = fc.get("confidence", "LOW")
        exp_range = fc.get("expected_range", [])
        action = fc.get("action", "")
        catalysts = fc.get("catalysts", [])
        gex_regime = fc.get("gex_regime", "UNKNOWN")

        result["mws_score"] = mws_score
        result["bullish_probability"] = bull_prob
        result["expected_range"] = exp_range
        result["confidence"] = confidence
        result["sensors"] = sensors
        result["catalysts"] = catalysts
        result["data_source"] = "MWS 7-Layer Forecast"

        # Extract key sensor signals
        sensor_signals = []
        options_score = 0
        micro_score = 0
        tech_score_mws = 0
        for s in sensors:
            name = s.get("name", "")
            score = s.get("score", 50)
            signal = s.get("signal", "neutral")
            details = s.get("details", "")
            if "Options" in name:
                options_score = score
                if signal == "bullish" and score >= 60:
                    sensor_signals.append(f"options_intel_bullish ({details[:60]})")
            if "Microstructure" in name:
                micro_score = score
                if signal == "bullish" and score >= 55:
                    sensor_signals.append(f"microstructure_bullish ({details[:60]})")
            if "Technical" in name:
                tech_score_mws = score
                if signal == "bullish" and score >= 55:
                    sensor_signals.append(f"technical_bullish ({details[:60]})")
                elif signal == "bearish":
                    sensor_signals.append(f"technical_weak ({details[:60]})")
            if "Catalyst" in name and signal == "bullish":
                sensor_signals.append(f"catalyst_active ({details[:60]})")
            if "Macro" in name and signal == "bearish":
                sensor_signals.append(f"macro_headwind ({details[:40]})")

        result["signals"] = sensor_signals

        # Derive bullish_score from MWS (0–100 → 0–1 scale)
        bullish_score = bull_prob / 100.0
        result["bullish_score"] = bullish_score

        # Also check UW caches for enrichment
        _enrich_with_uw_caches(symbol, result)

        # Determine opportunity level
        if bullish_score >= 0.65 or (options_score >= 70 and micro_score >= 55):
            result["opportunity_level"] = "HIGH"
            result["analysis"] = (
                f"🚀 HIGH moonshot potential — MWS score {mws_score:.1f}/100, "
                f"bullish probability {bull_prob}% ({confidence} confidence). "
                f"Expected range ${exp_range[0]:.2f}–${exp_range[1]:.2f}. "
                f"Action: {action}. Key: Options Intel {options_score}/100, "
                f"Microstructure {micro_score}/100, Technical {tech_score_mws}/100. "
                f"{'Catalysts: ' + ', '.join(catalysts[:2]) if catalysts else 'No specific catalysts'}."
            )
        elif bullish_score >= 0.48 or (options_score >= 60):
            result["opportunity_level"] = "MODERATE"
            result["analysis"] = (
                f"📈 MODERATE upside potential — MWS score {mws_score:.1f}/100, "
                f"bullish probability {bull_prob}% ({confidence} confidence). "
                f"Expected range ${exp_range[0]:.2f}–${exp_range[1]:.2f}. "
                f"Action: {action}. Options Intel {options_score}/100, "
                f"Microstructure {micro_score}/100, Technical {tech_score_mws}/100."
            )
        else:
            result["opportunity_level"] = "LOW"
            result["analysis"] = (
                f"📊 LOW moonshot signal — MWS score {mws_score:.1f}/100, "
                f"bullish probability {bull_prob}% ({confidence} confidence). "
                f"Expected range ${exp_range[0]:.2f}–${exp_range[1]:.2f}. "
                f"Action: {action}. Sensors show mixed/bearish alignment."
            )

        logger.info(
            f"  {symbol} MWS forecast: mws={mws_score:.1f}, prob={bull_prob}%, "
            f"opp={result['opportunity_level']}, sensors={len(sensor_signals)}"
        )
        return result

    # ── TIER 2: Final Recommendations Cache ──────────────────────────────
    recs_cache = _get_final_recs_cache()
    if symbol in recs_cache:
        rec = recs_cache[symbol]
        comp_score = rec.get("composite_score", 0)
        conviction = rec.get("conviction", 0)
        signals_list = rec.get("signals", [])
        uw_sentiment = rec.get("uw_sentiment", "")
        uw_call_prem = rec.get("uw_call_premium", 0)
        uw_put_prem = rec.get("uw_put_premium", 0)
        rsi_val = rec.get("rsi", 50)
        macd_bullish = rec.get("macd_bullish", False)
        above_20ma = rec.get("above_20ma", False)
        above_50ma = rec.get("above_50ma", False)
        atr_pct = rec.get("atr_pct", 0)
        persistence = rec.get("scan_persistence", 0)
        engines = rec.get("engines", [])
        why = rec.get("why", "")
        entry_low = rec.get("entry_low", 0)
        entry_high = rec.get("entry_high", 0)
        target = rec.get("target", 0)
        stop = rec.get("stop", 0)
        mws_val = rec.get("mws_score", 0)

        bullish_score = min(comp_score / 100.0, 1.0)
        result["bullish_score"] = bullish_score
        result["signals"] = signals_list
        result["uw_sentiment"] = uw_sentiment
        result["mws_score"] = mws_val
        result["data_source"] = "Final Recommendations"

        result["sub_scores"] = {
            "conviction": conviction,
            "composite": comp_score,
            "uw_call_premium": uw_call_prem,
            "uw_put_premium": uw_put_prem,
            "rsi": rsi_val,
            "macd_bullish": macd_bullish,
            "above_20ma": above_20ma,
            "above_50ma": above_50ma,
            "atr_pct": atr_pct,
            "scan_persistence": persistence,
            "engines": engines,
            "entry_range": f"${entry_low:.2f}–${entry_high:.2f}",
            "target": target,
            "stop": stop,
        }

        # Also check UW caches for enrichment
        _enrich_with_uw_caches(symbol, result)

        call_put_ratio = uw_call_prem / uw_put_prem if uw_put_prem > 0 else 0
        flow_desc = f"${uw_call_prem/1e6:.1f}M calls vs ${uw_put_prem/1e6:.1f}M puts" if uw_call_prem > 0 else "no flow data"

        if bullish_score >= 0.65 or conviction >= 4:
            result["opportunity_level"] = "HIGH"
            result["analysis"] = (
                f"🚀 HIGH moonshot potential — Composite {comp_score:.1f}/100, "
                f"conviction {conviction}/5, UW sentiment: {uw_sentiment}. "
                f"Flow: {flow_desc} (ratio {call_put_ratio:.1f}x). "
                f"RSI {rsi_val:.0f} | MACD {'bullish' if macd_bullish else 'bearish'} | "
                f"{'Above' if above_20ma else 'Below'} 20MA. "
                f"Engines: {', '.join(engines)}. Persistence: {persistence} scans. "
                f"Entry ${entry_low:.2f}–${entry_high:.2f}, Target ${target:.2f}, Stop ${stop:.2f}. "
                f"Why: {why[:100]}."
            )
        elif bullish_score >= 0.45 or conviction >= 3:
            result["opportunity_level"] = "MODERATE"
            result["analysis"] = (
                f"📈 MODERATE upside — Composite {comp_score:.1f}/100, "
                f"conviction {conviction}/5, UW: {uw_sentiment}. "
                f"Flow: {flow_desc}. RSI {rsi_val:.0f} | "
                f"MACD {'bullish' if macd_bullish else 'bearish'}. "
                f"Engines: {', '.join(engines)}. Persistence: {persistence}. "
                f"Why: {why[:80]}."
            )
        else:
            result["opportunity_level"] = "LOW"
            result["analysis"] = (
                f"📊 LOW signal — Composite {comp_score:.1f}/100, "
                f"conviction {conviction}/5. RSI {rsi_val:.0f}. "
                f"Engines: {', '.join(engines)}. {why[:60]}."
            )

        logger.info(
            f"  {symbol} final rec: comp={comp_score:.1f}, conviction={conviction}, "
            f"uw={uw_sentiment}, opp={result['opportunity_level']}"
        )
        return result

    # ── TIER 3: Standalone Moonshot-Equivalent Analysis (Polygon bars) ───
    return _standalone_moonshot_analysis(symbol, market_data, result)


def _enrich_with_uw_caches(symbol: str, result: Dict[str, Any]) -> None:
    """Enrich result with per-symbol UW cached data (no new API calls)."""
    enriched = []

    # Dark pool
    dp = _get_uw_cache("darkpool").get(symbol)
    if dp:
        blocks = dp.get("block_count", 0)
        total_val = dp.get("total_value", 0)
        prints = dp.get("print_count", 0)
        result.setdefault("darkpool", {
            "blocks": blocks, "total_value": total_val, "prints": prints,
        })
        enriched.append(f"darkpool ({blocks} blocks, ${total_val/1e6:.1f}M)")

    # GEX
    gex = _get_uw_cache("gex").get(symbol)
    if gex:
        result.setdefault("gex", gex)
        net = gex.get("net_gex", gex.get("gex_net", 0))
        regime = "POSITIVE" if net > 0 else "NEGATIVE"
        enriched.append(f"GEX {regime} ({net:+,.0f})")

    # Skew
    skew = _get_uw_cache("skew").get(symbol)
    if skew:
        result.setdefault("skew", skew)
        val = skew.get("skew", skew.get("risk_reversal", 0))
        trend = skew.get("trend", "unknown")
        enriched.append(f"skew {val:+.2f} ({trend})")

    # IV Term Structure
    iv = _get_uw_cache("iv_term").get(symbol)
    if iv:
        result.setdefault("iv_term", iv)
        front = iv.get("front_iv", 0)
        back = iv.get("back_iv", 0)
        structure = "INVERTED" if front > back * 1.05 else "NORMAL"
        enriched.append(f"IV {structure} (front {front:.0f}%)")

    # OI Change
    oi = _get_uw_cache("oi_change").get(symbol)
    if oi:
        result.setdefault("oi_change", oi)
        call_oi = oi.get("call_oi_change", 0)
        put_oi = oi.get("put_oi_change", 0)
        enriched.append(f"OI calls+{call_oi:,.0f}/puts+{put_oi:,.0f}")

    # Flow
    flow = _get_uw_cache("flow").get(symbol)
    if flow and isinstance(flow, list):
        total_prem = sum(t.get("premium", 0) for t in flow)
        calls = sum(1 for t in flow if t.get("put_call") == "C")
        puts = sum(1 for t in flow if t.get("put_call") == "P")
        result.setdefault("flow", {"total_premium": total_prem, "calls": calls, "puts": puts})
        enriched.append(f"flow ${total_prem/1e3:.0f}K ({calls}C/{puts}P)")

    if enriched:
        result["signals"] = result.get("signals", []) + [f"UW: {e}" for e in enriched]
        logger.debug(f"  {result['symbol']} UW enrichment: {', '.join(enriched)}")


# ---------------------------------------------------------------------------
# Standalone Moonshot-Grade Analysis (Tier 3)
# ---------------------------------------------------------------------------
# Replicates core Moonshot scoring using 30-day daily bars:
#
#   Technical Quality     25%  (RSI position, MACD alignment, BB position)
#   Momentum / Trend      25%  (multi-day strength, consecutive greens, returns)
#   Volume Analysis       20%  (RVOL, volume expansion on green days)
#   Pattern Recognition   15%  (higher highs/lows, breakout, consolidation)
#   Squeeze / Setup       15%  (BB compression, ATR contraction, coiling)
# ---------------------------------------------------------------------------

def _standalone_moonshot_analysis(
    symbol: str,
    market_data: Dict,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Full standalone bullish Moonshot-equivalent analysis using 30-day bars.
    Tier-3 fallback when both MWS forecast and final recommendations are
    unavailable.  Provides a detailed, actionable bullish assessment.
    """
    bars = market_data.get("daily_bars", [])
    price = market_data.get("price", 0)
    change = market_data.get("change_pct", 0)
    rsi = market_data.get("rsi", 50)
    rvol = market_data.get("rvol", 1.0)
    ema20 = market_data.get("ema20", 0)
    vwap = market_data.get("vwap", 0)

    result["data_source"] = "Standalone Technical Analysis (Polygon 30-day)"

    if not bars or len(bars) < 10:
        return _basic_moonshot_assessment(symbol, market_data, result)

    closes = [b["c"] for b in bars]
    signals_detected: List[str] = []
    sub_scores: Dict[str, float] = {}

    # --- A. Technical Quality (25% weight) ---
    tech_score = 0.0

    # A1. RSI positioning
    if 40 <= rsi <= 60:
        tech_score += 0.20  # neutral/coiling — good for breakout
        signals_detected.append(f"RSI_neutral_coil ({rsi:.0f})")
    elif 30 <= rsi < 40:
        tech_score += 0.30  # oversold bounce potential
        signals_detected.append(f"RSI_oversold_bounce ({rsi:.0f})")
    elif rsi < 30:
        tech_score += 0.35  # deeply oversold — high bounce potential
        signals_detected.append(f"RSI_deeply_oversold ({rsi:.0f})")
    elif 60 < rsi <= 70:
        tech_score += 0.15  # bullish momentum
        signals_detected.append(f"RSI_bullish_momentum ({rsi:.0f})")
    elif rsi > 70:
        tech_score += 0.05  # overbought — limited upside
        signals_detected.append(f"RSI_overbought ({rsi:.0f})")

    # A2. MACD alignment
    macd_data = _calc_macd(closes)
    if macd_data["histogram"] > 0 and macd_data["macd"] > macd_data["signal"]:
        tech_score += 0.25
        signals_detected.append(f"MACD_bullish_cross (hist +{macd_data['histogram']:.3f})")
    elif macd_data["histogram"] > 0:
        tech_score += 0.15
        signals_detected.append(f"MACD_positive (hist +{macd_data['histogram']:.3f})")
    elif macd_data["histogram"] < 0 and abs(macd_data["histogram"]) < 0.5:
        tech_score += 0.10
        signals_detected.append(f"MACD_converging (hist {macd_data['histogram']:.3f})")

    # A3. Bollinger Band position
    bb = _calc_bollinger(closes)
    if bb["position"] <= 0.15:
        tech_score += 0.25  # near lower BB — bounce setup
        signals_detected.append(f"BB_lower_band_bounce (pos {bb['position']:.2f})")
    elif bb["position"] <= 0.35:
        tech_score += 0.15
        signals_detected.append(f"BB_lower_zone (pos {bb['position']:.2f})")
    elif 0.5 <= bb["position"] <= 0.8:
        tech_score += 0.10  # middle to upper — trend continuation
    elif bb["position"] > 1.0:
        tech_score += 0.05  # outside upper — extended
        signals_detected.append(f"BB_above_upper (pos {bb['position']:.2f})")

    # A4. MA alignment
    if len(closes) >= 50:
        sma5 = sum(closes[-5:]) / 5
        sma10 = sum(closes[-10:]) / 10
        sma20 = sum(closes[-20:]) / 20
        sma50 = sum(closes[-50:]) / 50
        if price > sma5 > sma10 > sma20:
            tech_score += 0.20
            signals_detected.append("MA_perfect_alignment (5>10>20)")
        elif price > sma20:
            tech_score += 0.10
            signals_detected.append("above_SMA20")
        if price > sma50:
            tech_score += 0.05
    elif ema20 > 0 and price > ema20:
        tech_score += 0.10
        pct_above = (price - ema20) / ema20 * 100
        signals_detected.append(f"above_EMA20 (+{pct_above:.1f}%)")

    # A5. Price above VWAP
    if vwap > 0 and price > vwap:
        tech_score += 0.10
        signals_detected.append("above_VWAP")

    tech_score = min(tech_score, 1.0)
    sub_scores["technical_quality"] = tech_score

    # --- B. Momentum / Trend (25% weight) ---
    mom_score = 0.0

    # B1. Multi-day strength (consecutive higher closes)
    if len(bars) >= 5:
        consecutive_higher = 0
        for i in range(len(bars) - 1, 0, -1):
            if bars[i]["c"] > bars[i - 1]["c"]:
                consecutive_higher += 1
            else:
                break
        if consecutive_higher >= 4:
            mom_score += 0.35
            signals_detected.append(f"multi_day_strength ({consecutive_higher} consecutive higher closes)")
        elif consecutive_higher >= 3:
            mom_score += 0.25
            signals_detected.append("3_day_strength")
        elif consecutive_higher >= 2:
            mom_score += 0.10

    # B2. Green candle count (last 5 days)
    if len(bars) >= 5:
        green_days = sum(1 for b in bars[-5:] if b["c"] > b["o"])
        if green_days >= 4:
            mom_score += 0.20
            signals_detected.append(f"bullish_pattern ({green_days}/5 green days)")
        elif green_days >= 3:
            mom_score += 0.10

    # B3. 5-day return
    if len(bars) >= 6:
        five_d_ret = (bars[-1]["c"] - bars[-6]["c"]) / bars[-6]["c"] * 100 if bars[-6]["c"] else 0
        if five_d_ret > 10:
            mom_score += 0.30
            signals_detected.append(f"5d_surge (+{five_d_ret:.1f}%)")
        elif five_d_ret > 5:
            mom_score += 0.20
            signals_detected.append(f"5d_strength (+{five_d_ret:.1f}%)")
        elif five_d_ret > 2:
            mom_score += 0.10
        elif five_d_ret < -10:
            # Oversold bounce potential
            mom_score += 0.15
            signals_detected.append(f"5d_oversold ({five_d_ret:+.1f}%) — bounce potential")

    # B4. Relative strength vs prior 20-day
    if len(bars) >= 21:
        ret_20d = (bars[-1]["c"] - bars[-21]["c"]) / bars[-21]["c"] * 100 if bars[-21]["c"] else 0
        if ret_20d > 15:
            mom_score += 0.15
            signals_detected.append(f"20d_momentum (+{ret_20d:.1f}%)")

    mom_score = min(mom_score, 1.0)
    sub_scores["momentum_trend"] = mom_score

    # --- C. Volume Analysis (20% weight) ---
    vol_score = 0.0

    # C1. Relative volume
    if rvol >= 2.0:
        # High volume — context-dependent
        latest = bars[-1]
        if latest["c"] > latest["o"]:  # green day + high volume = bullish
            vol_score += 0.40
            signals_detected.append(f"high_RVOL_green_day ({rvol:.1f}x)")
        else:
            vol_score += 0.10  # high vol on red = distribution
    elif rvol >= 1.3:
        vol_score += 0.20
        signals_detected.append(f"elevated_volume ({rvol:.1f}x)")
    elif rvol >= 1.0:
        vol_score += 0.10

    # C2. Volume expansion on green days (last 5)
    if len(bars) >= 5:
        green_vol_avg = 0
        red_vol_avg = 0
        green_count = 0
        red_count = 0
        for b in bars[-5:]:
            if b["c"] > b["o"]:
                green_vol_avg += b["v"]
                green_count += 1
            else:
                red_vol_avg += b["v"]
                red_count += 1
        if green_count > 0 and red_count > 0:
            green_vol_avg /= green_count
            red_vol_avg /= red_count
            if green_vol_avg > red_vol_avg * 1.3:
                vol_score += 0.30
                signals_detected.append("volume_favors_buyers")
            elif green_vol_avg > red_vol_avg:
                vol_score += 0.15

    # C3. Volume trend (rising volume)
    if len(bars) >= 10:
        vol_first5 = sum(b["v"] for b in bars[-10:-5]) / 5
        vol_last5 = sum(b["v"] for b in bars[-5:]) / 5
        if vol_first5 > 0:
            vol_trend = (vol_last5 - vol_first5) / vol_first5
            if vol_trend > 0.3:
                vol_score += 0.20
                signals_detected.append(f"rising_volume_trend (+{vol_trend*100:.0f}%)")

    vol_score = min(vol_score, 1.0)
    sub_scores["volume_analysis"] = vol_score

    # --- D. Pattern Recognition (15% weight) ---
    pat_score = 0.0

    # D1. Higher highs pattern (last 10 days)
    if len(bars) >= 10:
        recent_highs = [b["h"] for b in bars[-5:]]
        prior_highs = [b["h"] for b in bars[-10:-5]]
        if max(recent_highs) > max(prior_highs):
            pat_score += 0.30
            signals_detected.append("higher_highs_pattern")

    # D2. Higher lows (bullish structure)
    if len(bars) >= 10:
        recent_lows = [b["l"] for b in bars[-5:]]
        prior_lows = [b["l"] for b in bars[-10:-5]]
        if min(recent_lows) > min(prior_lows):
            pat_score += 0.25
            signals_detected.append("higher_lows_formation")

    # D3. Breakout above 20-day high
    if len(bars) >= 20:
        high_20d = max(b["h"] for b in bars[-20:-1])
        if bars[-1]["c"] > high_20d:
            pat_score += 0.30
            signals_detected.append(f"breakout_above_20d_high (${high_20d:.2f})")
        elif bars[-1]["h"] >= high_20d * 0.99:
            pat_score += 0.15
            signals_detected.append("testing_20d_resistance")

    # D4. Bullish engulfing pattern
    if len(bars) >= 2:
        prev_b, curr_b = bars[-2], bars[-1]
        if (prev_b["c"] < prev_b["o"] and     # previous was red
            curr_b["c"] > curr_b["o"] and      # current is green
            curr_b["o"] <= prev_b["c"] and     # opened at or below prev close
            curr_b["c"] >= prev_b["o"]):        # closed at or above prev open
            pat_score += 0.30
            signals_detected.append("bullish_engulfing")

    # D5. Gap up and hold
    if len(bars) >= 2:
        prev_b, curr_b = bars[-2], bars[-1]
        gap = (curr_b["o"] - prev_b["c"]) / prev_b["c"] if prev_b["c"] else 0
        if gap >= 0.01 and curr_b["c"] >= curr_b["o"]:
            pat_score += 0.20
            signals_detected.append(f"gap_up_hold (+{gap*100:.1f}%)")

    pat_score = min(pat_score, 1.0)
    sub_scores["pattern_recognition"] = pat_score

    # --- E. Squeeze / Setup (15% weight) ---
    sq_score = 0.0

    # E1. Bollinger Band compression (squeeze indicator)
    if bb["width"] > 0:
        if bb["width"] < 0.05:
            sq_score += 0.40
            signals_detected.append(f"tight_BB_squeeze (width {bb['width']:.3f})")
        elif bb["width"] < 0.08:
            sq_score += 0.25
            signals_detected.append(f"BB_compression (width {bb['width']:.3f})")
        elif bb["width"] < 0.12:
            sq_score += 0.10

    # E2. ATR compression (contracting range)
    atr = _calc_atr(bars)
    if len(bars) >= 20 and atr > 0:
        atr_prev = _calc_atr(bars[:-5])
        if atr_prev > 0:
            atr_ratio = atr / atr_prev
            if atr_ratio < 0.7:
                sq_score += 0.30
                signals_detected.append(f"ATR_contracting ({atr_ratio:.2f}x)")
            elif atr_ratio < 0.85:
                sq_score += 0.15

    # E3. Coiling pattern (decreasing daily ranges)
    if len(bars) >= 5:
        ranges = [(b["h"] - b["l"]) / b["c"] for b in bars[-5:] if b["c"] > 0]
        if len(ranges) >= 3:
            if ranges[-1] < ranges[0] * 0.6:
                sq_score += 0.25
                signals_detected.append("coiling_pattern")

    # E4. Near round number (psychological level)
    if price > 0:
        nearest_round = round(price / 5) * 5
        dist_pct = abs(price - nearest_round) / price * 100
        if dist_pct < 2:
            sq_score += 0.10
            signals_detected.append(f"near_round_${nearest_round:.0f}")

    sq_score = min(sq_score, 1.0)
    sub_scores["squeeze_setup"] = sq_score

    # ── Weighted Composite Score ──────────────────────────────────────
    weights = {
        "technical_quality":   0.25,
        "momentum_trend":      0.25,
        "volume_analysis":     0.20,
        "pattern_recognition": 0.15,
        "squeeze_setup":       0.15,
    }

    composite = sum(sub_scores.get(k, 0) * weights[k] for k in weights)
    composite = max(0.0, min(1.0, composite))

    # ── Populate result ───────────────────────────────────────────────
    result["bullish_score"] = composite
    result["signals"] = signals_detected
    result["sub_scores"] = sub_scores

    # Also check UW caches for enrichment
    _enrich_with_uw_caches(symbol, result)

    # ── Opportunity Level + Narrative ─────────────────────────────────
    n_signals = len(signals_detected)
    top_sigs = ", ".join(signals_detected[:4]) if signals_detected else "none"

    if composite >= 0.55:
        result["opportunity_level"] = "HIGH"
        result["analysis"] = (
            f"🚀 HIGH moonshot potential — composite {composite:.2f} "
            f"({n_signals} signals). Key: {top_sigs}. "
            f"Technical + momentum alignment support breakout thesis. "
            f"RSI {rsi:.0f} | RVOL {rvol:.1f}x | "
            f"MACD {'bullish' if macd_data['histogram'] > 0 else 'bearish'} | "
            f"BB pos {bb['position']:.2f}."
        )
    elif composite >= 0.35:
        result["opportunity_level"] = "MODERATE"
        result["analysis"] = (
            f"📈 MODERATE upside potential — composite {composite:.2f} "
            f"({n_signals} signals). Key: {top_sigs}. "
            f"Some bullish signals present but incomplete confirmation. "
            f"RSI {rsi:.0f} | RVOL {rvol:.1f}x | "
            f"BB pos {bb['position']:.2f}."
        )
    elif composite >= 0.15:
        result["opportunity_level"] = "LOW"
        result["analysis"] = (
            f"📊 LOW moonshot signal — composite {composite:.2f} "
            f"({n_signals} signals). {top_sigs if top_sigs != 'none' else 'No significant bullish signals'}. "
            f"RSI {rsi:.0f} | RVOL {rvol:.1f}x. "
            f"Insufficient momentum/catalyst for moonshot play."
        )
    else:
        result["opportunity_level"] = "LOW"
        result["analysis"] = (
            f"📊 FLAT — composite {composite:.2f}. "
            f"No actionable bullish signals from 30-day analysis. "
            f"RSI {rsi:.0f} | RVOL {rvol:.1f}x. "
            f"Monitor for squeeze or catalyst to change thesis."
        )

    logger.info(
        f"  {symbol} standalone Moonshot analysis: score={composite:.2f}, "
        f"opp={result['opportunity_level']}, signals={n_signals} "
        f"[tech={sub_scores.get('technical_quality', 0):.2f} "
        f"mom={sub_scores.get('momentum_trend', 0):.2f} "
        f"vol={sub_scores.get('volume_analysis', 0):.2f} "
        f"pat={sub_scores.get('pattern_recognition', 0):.2f} "
        f"sq={sub_scores.get('squeeze_setup', 0):.2f}]"
    )

    return result


def _basic_moonshot_assessment(
    symbol: str,
    market_data: Dict,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    """Minimal fallback when even daily bars are unavailable."""
    change = market_data.get("change_pct", 0)
    price = market_data.get("price", 0)
    result["data_source"] = "Basic Price Assessment"

    if change > 5:
        result["opportunity_level"] = "HIGH"
        result["bullish_score"] = 0.60
        result["analysis"] = (
            f"📈 Strong momentum (+{change:.1f}%) at ${price:.2f} — "
            f"potential continuation. Limited data (prev-close only)."
        )
    elif change > 2:
        result["opportunity_level"] = "MODERATE"
        result["bullish_score"] = 0.40
        result["analysis"] = (
            f"📊 Moderate momentum (+{change:.1f}%) at ${price:.2f} — "
            f"watch for volume confirmation."
        )
    elif change < -5:
        result["opportunity_level"] = "LOW"
        result["bullish_score"] = 0.20
        result["analysis"] = (
            f"📉 Sharp decline ({change:.1f}%) at ${price:.2f} — "
            f"not a moonshot candidate. Oversold bounce possible."
        )
    else:
        result["opportunity_level"] = "LOW"
        result["bullish_score"] = 0.25
        result["analysis"] = (
            f"📊 Neutral ({change:+.1f}%) at ${price:.2f} — "
            f"no clear moonshot setup from available data."
        )

    return result


def _detect_overnight_gap(
    symbol: str,
    pick: Dict[str, Any],
    market_data: Dict[str, Any],
    gap_threshold_pct: float = 5.0,
) -> Optional[Dict[str, Any]]:
    """
    Detect large overnight gaps that may invalidate the original pick thesis.

    Example: TSLA picked at $448.10, but opened at $409.90 (-8.5%).
    Such a gap fundamentally changes the risk/reward — the entry is
    completely different from what the engine analyzed.

    Args:
        symbol: Ticker symbol
        pick: Original pick dict (contains the price at scan time)
        market_data: Live market data from Polygon (contains today's open)
        gap_threshold_pct: Percentage threshold to trigger alert (default 5%)

    Returns:
        Alert dict if gap detected, None otherwise.
    """
    pick_price = pick.get("price", 0)
    today_open = market_data.get("open", 0)

    if pick_price <= 0 or today_open <= 0:
        return None

    gap_pct = ((today_open - pick_price) / pick_price) * 100

    if abs(gap_pct) < gap_threshold_pct:
        return None

    direction = "DOWN" if gap_pct < 0 else "UP"
    severity = "CRITICAL" if abs(gap_pct) >= 10 else "WARNING"

    alert = {
        "symbol": symbol,
        "pick_price": pick_price,
        "today_open": today_open,
        "gap_pct": gap_pct,
        "direction": direction,
        "severity": severity,
        "message": (
            f"⚠️ OVERNIGHT GAP {severity}: {symbol} opened {gap_pct:+.1f}% "
            f"from pick price (${pick_price:.2f} → ${today_open:.2f}). "
            f"Original thesis may be invalidated — reassess entry."
        ),
    }

    logger.warning(
        f"  🚨 {symbol}: Overnight gap {direction} {abs(gap_pct):.1f}% "
        f"(pick ${pick_price:.2f} → open ${today_open:.2f}) [{severity}]"
    )

    return alert


def cross_analyze(
    puts_top10: List[Dict[str, Any]],
    moonshot_top10: List[Dict[str, Any]],
    polygon_api_key: str = "",
) -> Dict[str, Any]:
    """
    Cross-analyze Top 10 picks from each engine through the opposite engine.
    
    Args:
        puts_top10: Top 10 picks from PutsEngine
        moonshot_top10: Top 10 picks from Moonshot Engine
        polygon_api_key: Polygon API key for market data
        
    Returns:
        Dict with:
          - puts_through_moonshot: PutsEngine picks analyzed by Moonshot
          - moonshot_through_puts: Moonshot picks analyzed by PutsEngine
          - conflict_matrix: Where engines agree/disagree
          - combined_ranking: Final ranked list
    """
    logger.info("=" * 60)
    logger.info("🔄 CROSS-ENGINE ANALYSIS STARTING")
    logger.info("=" * 60)
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "puts_through_moonshot": [],
        "moonshot_through_puts": [],
        "conflict_matrix": [],
        "combined_ranking": [],
    }
    
    # 1. Run PutsEngine Top 10 through Moonshot lens
    logger.info("\n📊 Running PutsEngine picks through Moonshot analysis...")
    for pick in puts_top10:
        symbol = pick["symbol"]
        market_data = _get_market_data(symbol, polygon_api_key)
        moonshot_view = _analyze_with_moonshot_lens(symbol, market_data)
        
        cross_result = {
            **pick,
            "moonshot_analysis": moonshot_view,
            "market_data": market_data,
        }
        # ALWAYS use real-time Polygon price — cached prices can be hours stale
        if market_data.get("price", 0) > 0:
            old_price = cross_result.get("price", 0)
            cross_result["price"] = market_data["price"]
            if old_price > 0 and old_price != market_data["price"]:
                pct = ((market_data["price"] - old_price) / old_price) * 100
                if abs(pct) > 1:
                    logger.info(f"  {symbol}: price ${old_price:.2f}→${market_data['price']:.2f} ({pct:+.1f}%)")

        # Overnight gap anomaly detection
        gap_alert = _detect_overnight_gap(symbol, pick, market_data)
        if gap_alert:
            cross_result["overnight_gap_alert"] = gap_alert

        results["puts_through_moonshot"].append(cross_result)
        logger.info(f"  {symbol}: Puts={pick['score']:.2f} | Moonshot={moonshot_view['opportunity_level']}")
    
    # 2. Run Moonshot Top 10 through PutsEngine lens
    logger.info("\n📊 Running Moonshot picks through PutsEngine analysis...")
    for pick in moonshot_top10:
        symbol = pick["symbol"]
        market_data = _get_market_data(symbol, polygon_api_key)
        puts_view = _analyze_with_puts_lens(symbol, market_data)
        
        cross_result = {
            **pick,
            "puts_analysis": puts_view,
            "market_data": market_data,
        }
        # ALWAYS use real-time Polygon price — cached prices can be hours stale
        if market_data.get("price", 0) > 0:
            old_price = cross_result.get("price", 0)
            cross_result["price"] = market_data["price"]
            if old_price > 0 and old_price != market_data["price"]:
                pct = ((market_data["price"] - old_price) / old_price) * 100
                if abs(pct) > 1:
                    logger.info(f"  {symbol}: price ${old_price:.2f}→${market_data['price']:.2f} ({pct:+.1f}%)")

        # Overnight gap anomaly detection
        gap_alert = _detect_overnight_gap(symbol, pick, market_data)
        if gap_alert:
            cross_result["overnight_gap_alert"] = gap_alert

        results["moonshot_through_puts"].append(cross_result)
        logger.info(f"  {symbol}: Moonshot={pick['score']:.2f} | Puts Risk={puts_view['risk_level']}")
    
    # 3. Build conflict matrix
    logger.info("\n📊 Building conflict matrix...")
    all_symbols = set()
    puts_map = {p["symbol"]: p for p in puts_top10}
    moonshot_map = {m["symbol"]: m for m in moonshot_top10}
    all_symbols.update(puts_map.keys())
    all_symbols.update(moonshot_map.keys())
    
    for symbol in sorted(all_symbols):
        in_puts = symbol in puts_map
        in_moonshot = symbol in moonshot_map
        
        conflict_entry = {
            "symbol": symbol,
            "in_puts_top10": in_puts,
            "in_moonshot_top10": in_moonshot,
            "puts_score": puts_map[symbol]["score"] if in_puts else 0,
            "moonshot_score": moonshot_map[symbol]["score"] if in_moonshot else 0,
        }
        
        if in_puts and in_moonshot:
            conflict_entry["verdict"] = "⚡ CONFLICT — Both engines flagged this ticker (bearish + bullish signals)"
            conflict_entry["action"] = "MONITOR CLOSELY — high volatility expected"
        elif in_puts:
            conflict_entry["verdict"] = "🔴 BEARISH ONLY — PutsEngine sees distribution, Moonshot sees no upside"
            conflict_entry["action"] = "PUTS candidate — one-directional bearish signal"
        elif in_moonshot:
            conflict_entry["verdict"] = "🟢 BULLISH ONLY — Moonshot sees opportunity, PutsEngine sees no distribution"
            conflict_entry["action"] = "CALLS candidate — one-directional bullish signal"
        
        results["conflict_matrix"].append(conflict_entry)
    
    # 4. Build combined ranking
    combined = []
    
    for item in results["puts_through_moonshot"]:
        # Use market data price as fallback if pick price is 0
        price = item.get("price", 0)
        if price == 0:
            price = item.get("market_data", {}).get("price", 0)
        entry = {
            "symbol": item["symbol"],
            "source_engine": "PutsEngine",
            "source_score": item["score"],
            "cross_analysis": item["moonshot_analysis"]["analysis"],
            "cross_level": item["moonshot_analysis"]["opportunity_level"],
            "price": price,
            "combined_signal": f"PUT {item['score']:.2f} | Moonshot: {item['moonshot_analysis']['opportunity_level']}",
        }
        # Propagate overnight gap alert if present
        if item.get("overnight_gap_alert"):
            entry["overnight_gap_alert"] = item["overnight_gap_alert"]
        # Propagate data freshness
        if item.get("data_source"):
            entry["data_source"] = item["data_source"]
        if item.get("data_age_days") is not None:
            entry["data_age_days"] = item["data_age_days"]
        combined.append(entry)
    
    for item in results["moonshot_through_puts"]:
        # Use market data price as fallback if pick price is 0
        price = item.get("price", 0)
        if price == 0:
            price = item.get("market_data", {}).get("price", 0)
        entry = {
            "symbol": item["symbol"],
            "source_engine": "Moonshot",
            "source_score": item["score"],
            "cross_analysis": item["puts_analysis"]["analysis"],
            "cross_level": item["puts_analysis"]["risk_level"],
            "price": price,
            "combined_signal": f"MOONSHOT {item['score']:.2f} | Puts Risk: {item['puts_analysis']['risk_level']}",
        }
        # Propagate overnight gap alert if present
        if item.get("overnight_gap_alert"):
            entry["overnight_gap_alert"] = item["overnight_gap_alert"]
        # Propagate data freshness
        if item.get("data_source"):
            entry["data_source"] = item["data_source"]
        if item.get("data_age_days") is not None:
            entry["data_age_days"] = item["data_age_days"]
        combined.append(entry)
    
    combined.sort(key=lambda x: x["source_score"], reverse=True)
    results["combined_ranking"] = combined
    
    logger.info(f"\n✅ Cross-analysis complete: {len(combined)} total entries")
    
    return results
