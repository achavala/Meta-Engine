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


def _analyze_with_moonshot_lens(symbol: str, market_data: Dict) -> Dict[str, Any]:
    """
    Run a symbol through the Moonshot analytical lens.
    Checks for bullish signals: squeeze setup, unusual activity, gamma concentration.
    """
    result = {
        "symbol": symbol,
        "engine": "Moonshot",
        "bullish_score": 0.0,
        "signals": [],
        "analysis": "",
        "opportunity_level": "LOW",
    }
    
    try:
        from moonshot.core.engine import MoonshotEngine
        
        engine = MoonshotEngine(paper_mode=True)
        signal = engine.analyze_candidate(symbol)
        
        if signal:
            result["bullish_score"] = signal.strength.value if hasattr(signal.strength, 'value') else 0.5
            result["signals"] = [str(s) for s in signal.signal_types] if hasattr(signal, 'signal_types') else []
            
            strength = result["bullish_score"]
            if strength >= 0.70:
                result["opportunity_level"] = "HIGH"
                result["analysis"] = f"🚀 HIGH moonshot potential — Strong signals: {', '.join(result['signals'][:3])}"
            elif strength >= 0.40:
                result["opportunity_level"] = "MODERATE"
                result["analysis"] = f"📈 Moderate upside potential — Signals: {', '.join(result['signals'][:3])}"
            else:
                result["opportunity_level"] = "LOW"
                result["analysis"] = f"📊 Low moonshot signal — Insufficient squeeze/catalyst signals"
        else:
            result["analysis"] = "📊 No moonshot signals detected for this ticker"
            
    except Exception as e:
        logger.debug(f"Moonshot analysis for {symbol} failed: {e}")
        result["analysis"] = f"Moonshot scan unavailable: using market data analysis"
        
        # Fallback: basic analysis from market data
        price = market_data.get("price", 0)
        change = market_data.get("change_pct", 0)
        
        if change > 5:
            result["opportunity_level"] = "HIGH"
            result["bullish_score"] = 0.7
            result["analysis"] = f"📈 Strong momentum (+{change:.1f}%). Potential continuation or breakout candidate."
        elif change > 2:
            result["opportunity_level"] = "MODERATE"
            result["bullish_score"] = 0.5
            result["analysis"] = f"📊 Moderate momentum (+{change:.1f}%). Watch for volume confirmation."
        elif change < -5:
            result["opportunity_level"] = "LOW"
            result["bullish_score"] = 0.2
            result["analysis"] = f"📉 Sharp decline ({change:.1f}%). Not a moonshot candidate currently."
        else:
            result["bullish_score"] = 0.3
            result["analysis"] = f"📊 Neutral price action ({change:+.1f}%). No clear moonshot setup."
    
    return result


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
        combined.append({
            "symbol": item["symbol"],
            "source_engine": "PutsEngine",
            "source_score": item["score"],
            "cross_analysis": item["moonshot_analysis"]["analysis"],
            "cross_level": item["moonshot_analysis"]["opportunity_level"],
            "price": item.get("price", 0),
            "combined_signal": f"PUT {item['score']:.2f} | Moonshot: {item['moonshot_analysis']['opportunity_level']}",
        })
    
    for item in results["moonshot_through_puts"]:
        combined.append({
            "symbol": item["symbol"],
            "source_engine": "Moonshot",
            "source_score": item["score"],
            "cross_analysis": item["puts_analysis"]["analysis"],
            "cross_level": item["puts_analysis"]["risk_level"],
            "price": item.get("price", 0),
            "combined_signal": f"MOONSHOT {item['score']:.2f} | Puts Risk: {item['puts_analysis']['risk_level']}",
        })
    
    combined.sort(key=lambda x: x["source_score"], reverse=True)
    results["combined_ranking"] = combined
    
    logger.info(f"\n✅ Cross-analysis complete: {len(combined)} total entries")
    
    return results
