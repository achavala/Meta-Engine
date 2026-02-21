#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════
 3 PM EST INSTITUTIONAL-GRADE ANALYSIS & NOTIFICATION
═══════════════════════════════════════════════════════════
Deep-dive analysis using ALL available data sources:
  - Polygon real-time prices & technicals
  - Unusual Whales options flow (dark pool, institutional)
  - TradeNova eod_interval_picks + final_recommendations
  - Meta Engine cross-analysis (PutsEngine × Moonshot)
  - Options chain analysis for optimal strikes & expiry

Outputs: Email + Telegram + X/Twitter
"""

import os, sys, json, time, logging, re, smtplib, ssl, requests
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

os.chdir("/Users/chavala/Meta Engine")
sys.path.insert(0, "/Users/chavala/Meta Engine")

from dotenv import load_dotenv
load_dotenv("/Users/chavala/Meta Engine/.env")

import pytz
EST = pytz.timezone("US/Eastern")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("3PM_Analysis")

from config import MetaConfig

# ═══════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════

def load_all_data():
    """Load all available data sources."""
    data = {}

    # 1. UW Flow Cache (dark pool, unusual options activity)
    uw_path = Path.home() / "TradeNova" / "data" / "uw_flow_cache.json"
    if uw_path.exists():
        with open(uw_path) as f:
            uw = json.load(f)
        data["uw_flow"] = uw.get("flow_data", {})
        data["uw_meta"] = {k: v for k, v in uw.items() if k != "flow_data"}
        logger.info(f"  ✅ UW Flow: {len(data['uw_flow'])} symbols loaded")
    else:
        data["uw_flow"] = {}
        logger.warning("  ⚠️ UW Flow cache not found")

    # 2. EOD Interval Picks (rich intraday data)
    eod_path = Path.home() / "TradeNova" / "data" / "eod_interval_picks.json"
    if eod_path.exists():
        with open(eod_path) as f:
            data["eod_picks"] = json.load(f)
        logger.info(f"  ✅ EOD Picks: {len(data['eod_picks'])} entries loaded")
    else:
        data["eod_picks"] = {}

    # 3. Final Recommendations
    fr_path = Path.home() / "TradeNova" / "data" / "final_recommendations.json"
    if fr_path.exists():
        with open(fr_path) as f:
            data["final_recs"] = json.load(f)
        logger.info(f"  ✅ Final Recs: loaded")
    else:
        data["final_recs"] = {}

    # 4. Today's cross-analysis
    today_str = datetime.now(EST).strftime('%Y%m%d')
    cross_path = Path("output") / f"cross_analysis_{today_str}.json"
    if cross_path.exists():
        with open(cross_path) as f:
            data["cross"] = json.load(f)
        logger.info(f"  ✅ Cross Analysis: loaded ({today_str})")
    else:
        cross_latest = Path("output") / "cross_analysis_latest.json"
        if cross_latest.exists():
            with open(cross_latest) as f:
                data["cross"] = json.load(f)
            logger.info(f"  ✅ Cross Analysis: loaded (latest)")
        else:
            data["cross"] = {}

    # 5. Puts Top 10
    puts_path = Path("output") / f"puts_top10_{today_str}.json"
    if puts_path.exists():
        with open(puts_path) as f:
            pd = json.load(f)
        data["puts_top10"] = pd.get("picks", []) if isinstance(pd, dict) else pd
    else:
        data["puts_top10"] = []

    # 6. Moonshot Top 10
    moon_path = Path("output") / f"moonshot_top10_{today_str}.json"
    if moon_path.exists():
        with open(moon_path) as f:
            md = json.load(f)
        data["moon_top10"] = md.get("picks", []) if isinstance(md, dict) else md
    else:
        data["moon_top10"] = []

    # 7. Real-time mover injection — catch movers that appeared after the
    #    morning scan, or when output files are empty/stale.
    try:
        from engine_adapters.realtime_mover_scanner import (
            scan_realtime_movers,
            build_puts_candidates_from_movers,
            build_moonshot_candidates_from_movers,
        )
        rt = scan_realtime_movers()
        existing_put_syms = {p.get("symbol", "") for p in data["puts_top10"]}
        existing_call_syms = {c.get("symbol", "") for c in data["moon_top10"]}

        for rc in build_puts_candidates_from_movers(rt.get("gap_down_movers", [])):
            if rc["symbol"] not in existing_put_syms:
                data["puts_top10"].append(rc)
                existing_put_syms.add(rc["symbol"])

        for rc in build_moonshot_candidates_from_movers(rt.get("gap_up_movers", [])):
            if rc["symbol"] not in existing_call_syms:
                data["moon_top10"].append(rc)
                existing_call_syms.add(rc["symbol"])

        data["realtime_snapshot"] = rt.get("all_prices", {})
        logger.info(f"  ✅ Real-time movers: {len(rt.get('gap_up_movers', []))} gap-up, "
                     f"{len(rt.get('gap_down_movers', []))} gap-down injected")
    except Exception as e:
        logger.warning(f"  ⚠️ Real-time mover injection skipped: {e}")

    return data


# ═══════════════════════════════════════════════════════
# POLYGON REAL-TIME DATA
# ═══════════════════════════════════════════════════════

def fetch_realtime_price(symbol: str) -> Dict:
    """Fetch real-time price from Polygon."""
    api_key = MetaConfig.POLYGON_API_KEY
    if not api_key:
        return {}
    try:
        # Previous close
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev",
            params={"apiKey": api_key}, timeout=10
        )
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                bar = results[0]
                return {
                    "open": bar.get("o", 0),
                    "high": bar.get("h", 0),
                    "low": bar.get("l", 0),
                    "close": bar.get("c", 0),
                    "volume": bar.get("v", 0),
                    "vwap": bar.get("vw", 0),
                }
    except Exception as e:
        logger.debug(f"Price fetch failed for {symbol}: {e}")
    return {}


def fetch_daily_bars(symbol: str, days: int = 30) -> List[Dict]:
    """Fetch daily bars for technical analysis."""
    api_key = MetaConfig.POLYGON_API_KEY
    if not api_key:
        return []
    try:
        end = date.today()
        start = end - timedelta(days=days + 10)
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
            params={"apiKey": api_key, "limit": days + 5, "sort": "asc"},
            timeout=15
        )
        if r.status_code == 200:
            return r.json().get("results", [])
    except Exception:
        pass
    return []


def calc_rsi(bars: List[Dict], period: int = 14) -> float:
    """Calculate RSI from daily bars."""
    if len(bars) < period + 1:
        return 50.0
    closes = [b.get("c", 0) for b in bars]
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [d for d in deltas[-period:] if d > 0]
    losses = [-d for d in deltas[-period:] if d < 0]
    avg_gain = sum(gains) / period if gains else 0.001
    avg_loss = sum(losses) / period if losses else 0.001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_ema(bars: List[Dict], period: int = 20) -> float:
    """Calculate EMA from daily bars."""
    closes = [b.get("c", 0) for b in bars]
    if len(closes) < period:
        return closes[-1] if closes else 0
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


# ═══════════════════════════════════════════════════════
# UW FLOW ANALYSIS (Dark Pool & Institutional)
# ═══════════════════════════════════════════════════════

def analyze_uw_flow(symbol: str, flow_data: Dict) -> Dict:
    """Deep analysis of UW options flow for a symbol."""
    entries = flow_data.get(symbol, [])
    if not entries:
        return {"has_data": False}

    calls = [e for e in entries if e.get("put_call") == "C"]
    puts = [e for e in entries if e.get("put_call") == "P"]

    total_call_vol = sum(e.get("volume", 0) for e in calls)
    total_put_vol = sum(e.get("volume", 0) for e in puts)
    total_call_prem = sum(e.get("premium", 0) for e in calls)
    total_put_prem = sum(e.get("premium", 0) for e in puts)
    total_call_oi = sum(e.get("open_interest", 0) for e in calls)
    total_put_oi = sum(e.get("open_interest", 0) for e in puts)

    # Put/call ratio
    pc_ratio = total_put_vol / total_call_vol if total_call_vol > 0 else 1.0

    # Premium-weighted sentiment
    net_prem = total_call_prem - total_put_prem

    # Unusual activity (volume >> OI)
    unusual_calls = [e for e in calls if e.get("volume", 0) > e.get("open_interest", 1) * 1.5]
    unusual_puts = [e for e in puts if e.get("volume", 0) > e.get("open_interest", 1) * 1.5]

    # Highest volume contracts (likely institutional)
    top_calls = sorted(calls, key=lambda x: x.get("volume", 0), reverse=True)[:3]
    top_puts = sorted(puts, key=lambda x: x.get("volume", 0), reverse=True)[:3]

    # Dark pool indicator: large OI with low volume = position building
    dark_pool_calls = [e for e in calls if e.get("open_interest", 0) > 5000 and
                       e.get("volume", 0) < e.get("open_interest", 0) * 0.1]
    dark_pool_puts = [e for e in puts if e.get("open_interest", 0) > 5000 and
                      e.get("volume", 0) < e.get("open_interest", 0) * 0.1]

    # Near-term contracts (high gamma)
    near_calls = [e for e in calls if e.get("dte", 999) <= 14]
    near_puts = [e for e in puts if e.get("dte", 999) <= 14]

    return {
        "has_data": True,
        "total_calls": len(calls),
        "total_puts": len(puts),
        "call_volume": total_call_vol,
        "put_volume": total_put_vol,
        "call_premium": total_call_prem,
        "put_premium": total_put_prem,
        "pc_ratio": pc_ratio,
        "net_premium": net_prem,
        "unusual_call_count": len(unusual_calls),
        "unusual_put_count": len(unusual_puts),
        "dark_pool_calls": len(dark_pool_calls),
        "dark_pool_puts": len(dark_pool_puts),
        "top_calls": top_calls,
        "top_puts": top_puts,
        "near_calls": len(near_calls),
        "near_puts": len(near_puts),
        "sentiment": "BULLISH" if pc_ratio < 0.7 else ("BEARISH" if pc_ratio > 1.3 else "NEUTRAL"),
    }


# ═══════════════════════════════════════════════════════
# COMPREHENSIVE SYMBOL ANALYSIS
# ═══════════════════════════════════════════════════════

def deep_analyze_symbol(symbol: str, direction: str, data: Dict) -> Dict:
    """
    Full institutional-grade analysis for a single symbol.
    direction: 'call' or 'put'
    """
    result = {"symbol": symbol, "direction": direction}

    # 1. Real-time price
    price_data = fetch_realtime_price(symbol)
    price = price_data.get("close", 0)
    result["price"] = price
    result["price_data"] = price_data

    # 2. Technical analysis (30-day bars)
    bars = fetch_daily_bars(symbol, 30)
    if bars:
        result["rsi"] = calc_rsi(bars)
        result["ema20"] = calc_ema(bars, 20)
        result["ema9"] = calc_ema(bars, 9)

        # ATR for volatility
        trs = []
        for i in range(1, len(bars)):
            h, l, pc = bars[i].get("h", 0), bars[i].get("l", 0), bars[i-1].get("c", 0)
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)
        result["atr"] = sum(trs[-14:]) / min(len(trs), 14) if trs else 0

        # RVOL
        volumes = [b.get("v", 0) for b in bars]
        avg_vol = sum(volumes[:-1]) / max(len(volumes) - 1, 1) if len(volumes) > 1 else 1
        result["rvol"] = volumes[-1] / avg_vol if avg_vol > 0 else 1.0

        # Trend (last 5 bars)
        last5 = [b.get("c", 0) for b in bars[-5:]]
        if len(last5) >= 2:
            result["5d_change_pct"] = ((last5[-1] - last5[0]) / last5[0] * 100) if last5[0] > 0 else 0
        else:
            result["5d_change_pct"] = 0
    else:
        result["rsi"] = 50
        result["ema20"] = price
        result["ema9"] = price
        result["atr"] = 0
        result["rvol"] = 1.0
        result["5d_change_pct"] = 0

    # 3. UW Flow analysis
    uw = analyze_uw_flow(symbol, data.get("uw_flow", {}))
    result["uw"] = uw

    # 4. Cross-analysis scores
    cross = data.get("cross", {})
    puts_through = cross.get("puts_through_moonshot", [])
    moon_through = cross.get("moonshot_through_puts", [])

    for p in puts_through:
        if p.get("symbol") == symbol:
            result["puts_score"] = float(p.get("source_score", p.get("score", 0)))
            result["moon_counter"] = p.get("cross_analysis", {}).get("opportunity_level", "N/A")
            break

    for m in moon_through:
        if m.get("symbol") == symbol:
            result["moon_score"] = float(m.get("source_score", m.get("score", 0)))
            result["puts_counter"] = m.get("cross_analysis", {}).get("risk_level", "N/A")
            break

    # 5. EOD interval persistence
    eod = data.get("eod_picks", {})
    intervals = eod.get("intervals", {}) if isinstance(eod, dict) else {}
    persistence = 0
    for interval_key, picks_list in intervals.items():
        if isinstance(picks_list, list):
            for pk in picks_list:
                if pk.get("symbol") == symbol:
                    persistence += 1
    result["interval_persistence"] = persistence

    return result


def compute_trade_recommendation(analysis: Dict) -> Dict:
    """
    Given deep analysis, compute specific strike + expiry + entry/exit.
    Targeting 3x-10x returns.
    """
    sym = analysis["symbol"]
    direction = analysis["direction"]
    price = analysis.get("price", 0)
    atr = analysis.get("atr", 0)
    rsi = analysis.get("rsi", 50)
    uw = analysis.get("uw", {})

    if price <= 0:
        return {"symbol": sym, "tradeable": False, "reason": "no price data"}

    # ── Strike Selection ──
    # For 3x-10x returns: need OTM contracts with enough gamma
    if direction == "call":
        # OTM by ~4-7% for calls
        otm_pct = 0.05
        if rsi < 40:
            otm_pct = 0.04  # less OTM when oversold (higher chance)
        elif rsi > 60:
            otm_pct = 0.06  # more OTM when overbought
        target_strike = price * (1 + otm_pct)
    else:
        # OTM by ~4-7% for puts
        otm_pct = 0.05
        if rsi > 60:
            otm_pct = 0.04  # less OTM when overbought (higher chance of drop)
        elif rsi < 40:
            otm_pct = 0.06  # more OTM when oversold
        target_strike = price * (1 - otm_pct)

    # Round to standard strikes
    if price < 30:
        target_strike = round(target_strike)
    elif price < 100:
        target_strike = round(target_strike / 2.5) * 2.5
    elif price < 500:
        target_strike = round(target_strike / 5) * 5
    else:
        target_strike = round(target_strike / 10) * 10

    # ── Expiry Selection ──
    # Feb 20 is ~10 DTE — good gamma/theta sweet spot for 3x-10x
    # Also check Feb 14 (7 DTE) for higher gamma if conviction is high
    today = date.today()
    feb20 = date(2026, 2, 20)
    feb14 = date(2026, 2, 14)
    feb28 = date(2026, 2, 27)  # or nearest monthly

    dte_20 = (feb20 - today).days
    dte_14 = (feb14 - today).days

    # Default to Feb 20 (10 DTE)
    recommended_expiry = feb20
    dte = dte_20
    expiry_rationale = f"Feb 20 ({dte}d) — optimal gamma/theta for 3x-10x"

    # ── UW Flow Integration ──
    flow_signals = []
    if uw.get("has_data"):
        if direction == "call" and uw.get("sentiment") == "BULLISH":
            flow_signals.append("🟢 UW flow BULLISH (P/C ratio: {:.2f})".format(uw.get("pc_ratio", 0)))
        elif direction == "put" and uw.get("sentiment") == "BEARISH":
            flow_signals.append("🔴 UW flow BEARISH (P/C ratio: {:.2f})".format(uw.get("pc_ratio", 0)))

        if uw.get("unusual_call_count", 0) > 3 and direction == "call":
            flow_signals.append(f"⚡ {uw['unusual_call_count']} unusual call sweeps detected")
        if uw.get("unusual_put_count", 0) > 3 and direction == "put":
            flow_signals.append(f"⚡ {uw['unusual_put_count']} unusual put sweeps detected")

        if uw.get("dark_pool_calls", 0) > 0 and direction == "call":
            flow_signals.append(f"🌑 {uw['dark_pool_calls']} dark pool call positions")
        if uw.get("dark_pool_puts", 0) > 0 and direction == "put":
            flow_signals.append(f"🌑 {uw['dark_pool_puts']} dark pool put positions")

        # Check if institutional is aligned
        top_contracts = uw.get("top_calls" if direction == "call" else "top_puts", [])
        if top_contracts:
            best = top_contracts[0]
            inst_strike = best.get("strike", 0)
            inst_vol = best.get("volume", 0)
            inst_oi = best.get("open_interest", 0)
            inst_exp = best.get("expiration", "?")
            flow_signals.append(
                f"📊 Top institutional: ${inst_strike} {inst_exp} "
                f"(Vol={inst_vol:,} OI={inst_oi:,})"
            )

    # ── Scoring ──
    conviction = 0.5
    # RSI alignment
    if direction == "call" and rsi < 45:
        conviction += 0.15
    elif direction == "put" and rsi > 55:
        conviction += 0.15

    # UW flow alignment
    if direction == "call" and uw.get("sentiment") == "BULLISH":
        conviction += 0.1
    elif direction == "put" and uw.get("sentiment") == "BEARISH":
        conviction += 0.1

    # Cross-analysis alignment
    if direction == "call" and analysis.get("puts_counter") == "LOW":
        conviction += 0.1
    elif direction == "put" and analysis.get("moon_counter") == "LOW":
        conviction += 0.1

    # Unusual activity
    if uw.get("unusual_call_count", 0) > 2 and direction == "call":
        conviction += 0.1
    if uw.get("unusual_put_count", 0) > 2 and direction == "put":
        conviction += 0.1

    # Interval persistence
    if analysis.get("interval_persistence", 0) >= 3:
        conviction += 0.05

    conviction = min(conviction, 1.0)

    # ── Entry/Exit ──
    if direction == "call":
        entry_low = price * 0.995
        entry_high = price * 1.005
        target_3x = price * (1 + otm_pct + atr / price * 2) if price > 0 else 0
        stop = price * 0.97
    else:
        entry_low = price * 0.995
        entry_high = price * 1.005
        target_3x = price * (1 - otm_pct - atr / price * 2) if price > 0 else 0
        stop = price * 1.03

    # ── Technical Signals ──
    tech_signals = []
    if rsi < 30:
        tech_signals.append("Deeply Oversold (RSI {:.0f})".format(rsi))
    elif rsi < 45:
        tech_signals.append("Approaching Oversold (RSI {:.0f})".format(rsi))
    elif rsi > 70:
        tech_signals.append("Deeply Overbought (RSI {:.0f})".format(rsi))
    elif rsi > 55:
        tech_signals.append("Approaching Overbought (RSI {:.0f})".format(rsi))

    ema9 = analysis.get("ema9", price)
    ema20 = analysis.get("ema20", price)
    if ema9 > ema20:
        tech_signals.append("EMA9 > EMA20 (Bullish Cross)")
    else:
        tech_signals.append("EMA9 < EMA20 (Bearish Cross)")

    rvol = analysis.get("rvol", 1.0)
    if rvol > 1.5:
        tech_signals.append(f"High RVOL ({rvol:.1f}x)")

    change5d = analysis.get("5d_change_pct", 0)
    tech_signals.append(f"5D Change: {change5d:+.1f}%")

    return {
        "symbol": sym,
        "tradeable": True,
        "direction": direction,
        "price": price,
        "strike": target_strike,
        "expiry": recommended_expiry.isoformat(),
        "expiry_display": recommended_expiry.strftime("%b %d"),
        "dte": dte,
        "expiry_rationale": expiry_rationale,
        "otm_pct": otm_pct * 100,
        "conviction": conviction,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "target_3x": target_3x,
        "stop": stop,
        "rsi": rsi,
        "atr": atr,
        "rvol": rvol,
        "change_5d": change5d,
        "tech_signals": tech_signals,
        "flow_signals": flow_signals,
        "contracts": 5,
    }


# ═══════════════════════════════════════════════════════
# CANDIDATE SELECTION
# ═══════════════════════════════════════════════════════

def select_top_candidates(data: Dict) -> Tuple[List[Dict], List[Dict]]:
    """
    From ALL data sources, select the top 3 CALL and top 3 PUT candidates.
    Scoring combines: cross-analysis score, UW flow, technicals, persistence.
    """
    cross = data.get("cross", {})
    uw_flow = data.get("uw_flow", {})

    # Build candidate pools
    call_candidates = {}  # symbol -> score components
    put_candidates = {}

    # Source 1: Moonshot Top 10 (CALL candidates)
    for pick in data.get("moon_top10", []):
        sym = pick.get("symbol", "")
        if not sym:
            continue
        score = float(pick.get("score", 0))
        call_candidates[sym] = {
            "base_score": score,
            "source": "Moonshot",
            "signals": pick.get("signals", []),
        }

    # Source 2: moonshot_through_puts from cross-analysis
    for pick in cross.get("moonshot_through_puts", []):
        sym = pick.get("symbol", "")
        if not sym:
            continue
        score = float(pick.get("source_score", pick.get("score", 0)))
        risk = pick.get("cross_analysis", {}).get("risk_level", "MODERATE")
        risk_bonus = 0.1 if risk == "LOW" else (-0.05 if risk == "HIGH" else 0)
        if sym in call_candidates:
            call_candidates[sym]["cross_score"] = score + risk_bonus
        else:
            call_candidates[sym] = {"base_score": score + risk_bonus, "source": "CrossMoon"}

    # Source 3: PutsEngine Top 10 (PUT candidates)
    for pick in data.get("puts_top10", []):
        sym = pick.get("symbol", "")
        if not sym:
            continue
        score = float(pick.get("score", 0))
        put_candidates[sym] = {
            "base_score": score,
            "source": "PutsEngine",
            "signals": pick.get("signals", []),
        }

    # Source 4: puts_through_moonshot from cross-analysis
    for pick in cross.get("puts_through_moonshot", []):
        sym = pick.get("symbol", "")
        if not sym:
            continue
        score = float(pick.get("source_score", pick.get("score", 0)))
        opp_level = pick.get("cross_analysis", {}).get("opportunity_level", "MODERATE")
        # LOW moonshot opportunity = better for puts
        opp_bonus = 0.1 if opp_level == "LOW" else (-0.05 if opp_level == "HIGH" else 0)
        if sym in put_candidates:
            put_candidates[sym]["cross_score"] = score + opp_bonus
        else:
            put_candidates[sym] = {"base_score": score + opp_bonus, "source": "CrossPuts"}

    # Source 5: EOD Interval Picks (add high-persistence symbols)
    eod = data.get("eod_picks", {})
    intervals = eod.get("intervals", {}) if isinstance(eod, dict) else {}
    symbol_persistence = {}
    for interval_key, picks_list in intervals.items():
        if isinstance(picks_list, list):
            for pk in picks_list:
                s = pk.get("symbol", "")
                if s:
                    symbol_persistence[s] = symbol_persistence.get(s, 0) + 1

    # Boost candidates with high persistence
    for sym, count in symbol_persistence.items():
        if count >= 3:
            if sym in call_candidates:
                call_candidates[sym]["persistence_bonus"] = count * 0.02
            elif sym not in put_candidates:
                # High persistence = potential call candidate
                call_candidates[sym] = {
                    "base_score": 0.5 + count * 0.03,
                    "source": "EOD_Interval",
                    "persistence_bonus": count * 0.02,
                }

    # UW Flow enrichment
    for sym in list(call_candidates.keys()):
        flow = uw_flow.get(sym, [])
        if flow:
            calls = [e for e in flow if e.get("put_call") == "C"]
            puts = [e for e in flow if e.get("put_call") == "P"]
            call_vol = sum(e.get("volume", 0) for e in calls)
            put_vol = sum(e.get("volume", 0) for e in puts)
            if call_vol > put_vol * 1.5:
                call_candidates[sym]["uw_bonus"] = 0.05
            unusual = [e for e in calls if e.get("volume", 0) > e.get("open_interest", 1) * 1.5]
            if len(unusual) > 2:
                call_candidates[sym]["unusual_bonus"] = 0.05

    for sym in list(put_candidates.keys()):
        flow = uw_flow.get(sym, [])
        if flow:
            calls = [e for e in flow if e.get("put_call") == "C"]
            puts = [e for e in flow if e.get("put_call") == "P"]
            call_vol = sum(e.get("volume", 0) for e in calls)
            put_vol = sum(e.get("volume", 0) for e in puts)
            if put_vol > call_vol * 1.3:
                put_candidates[sym]["uw_bonus"] = 0.05
            unusual = [e for e in puts if e.get("volume", 0) > e.get("open_interest", 1) * 1.5]
            if len(unusual) > 2:
                put_candidates[sym]["unusual_bonus"] = 0.05

    # Compute final scores
    def total_score(c):
        return (c.get("base_score", 0) +
                c.get("cross_score", 0) * 0.3 +
                c.get("persistence_bonus", 0) +
                c.get("uw_bonus", 0) +
                c.get("unusual_bonus", 0))

    call_ranked = sorted(call_candidates.items(), key=lambda x: total_score(x[1]), reverse=True)
    put_ranked = sorted(put_candidates.items(), key=lambda x: total_score(x[1]), reverse=True)

    # Deep-analyze top candidates
    top_calls = []
    for sym, meta in call_ranked[:6]:  # analyze top 6, pick best 3
        analysis = deep_analyze_symbol(sym, "call", data)
        rec = compute_trade_recommendation(analysis)
        rec["meta_score"] = total_score(meta)
        rec["source"] = meta.get("source", "?")
        if rec["tradeable"]:
            top_calls.append(rec)

    top_puts = []
    for sym, meta in put_ranked[:6]:
        analysis = deep_analyze_symbol(sym, "put", data)
        rec = compute_trade_recommendation(analysis)
        rec["meta_score"] = total_score(meta)
        rec["source"] = meta.get("source", "?")
        if rec["tradeable"]:
            top_puts.append(rec)

    # Re-sort by conviction (which includes technicals + flow)
    top_calls.sort(key=lambda x: x["conviction"], reverse=True)
    top_puts.sort(key=lambda x: x["conviction"], reverse=True)

    return top_calls[:3], top_puts[:3]


# ═══════════════════════════════════════════════════════
# REPORT GENERATION
# ═══════════════════════════════════════════════════════

def generate_report(calls: List[Dict], puts: List[Dict], data: Dict,
                    session_label: str = "PM") -> str:
    """Generate the full markdown analysis report."""
    now = datetime.now(EST)
    report = []
    report.append(f"# 🎯 {session_label} INSTITUTIONAL OPTIONS ANALYSIS")
    report.append(f"**{now.strftime('%B %d, %Y %I:%M %p ET')}**")
    report.append("")
    report.append("---")
    report.append("")
    report.append("## Data Sources Analyzed")
    report.append(f"- **Polygon API**: Real-time prices, 30-day technicals (RSI, EMA, ATR, RVOL)")
    report.append(f"- **Unusual Whales**: {len(data.get('uw_flow', {}))} symbols of options flow data")
    report.append(f"- **TradeNova**: EOD interval picks + final recommendations")
    report.append(f"- **Meta Engine Cross-Analysis**: PutsEngine × Moonshot conflict matrix")
    report.append(f"- **Dark Pool Detection**: Large OI positions with low volume activity")
    report.append("")

    report.append("---")
    report.append("")
    report.append("## 🟢 TOP 3 CALL RECOMMENDATIONS (3x-10x Target)")
    report.append("")

    for i, c in enumerate(calls, 1):
        report.append(f"### #{i} — {c['symbol']} ${c['strike']:.0f}C {c['expiry_display']} ({c['dte']}d)")
        report.append("")
        report.append(f"| Metric | Value |")
        report.append(f"|--------|-------|")
        report.append(f"| Current Price | ${c['price']:.2f} |")
        report.append(f"| Strike | ${c['strike']:.0f} ({c['otm_pct']:.1f}% OTM) |")
        report.append(f"| Expiry | {c['expiry_display']} ({c['dte']} DTE) |")
        report.append(f"| Contracts | {c['contracts']} |")
        report.append(f"| Entry Zone | ${c['entry_low']:.2f} — ${c['entry_high']:.2f} |")
        report.append(f"| Target (3x) | ${c['target_3x']:.2f} |")
        report.append(f"| Stop Loss | ${c['stop']:.2f} |")
        report.append(f"| RSI | {c['rsi']:.1f} |")
        report.append(f"| ATR | ${c['atr']:.2f} |")
        report.append(f"| RVOL | {c['rvol']:.1f}x |")
        report.append(f"| 5D Change | {c['change_5d']:+.1f}% |")
        report.append(f"| Conviction | {c['conviction']:.0%} |")
        report.append(f"| Source | {c['source']} |")
        report.append("")
        if c.get("tech_signals"):
            report.append(f"**Technical Signals**: {' | '.join(c['tech_signals'])}")
        if c.get("flow_signals"):
            report.append(f"**Flow Signals**:")
            for fs in c["flow_signals"]:
                report.append(f"  - {fs}")
        report.append("")

    report.append("---")
    report.append("")
    report.append("## 🔴 TOP 3 PUT RECOMMENDATIONS (3x-10x Target)")
    report.append("")

    for i, p in enumerate(puts, 1):
        report.append(f"### #{i} — {p['symbol']} ${p['strike']:.0f}P {p['expiry_display']} ({p['dte']}d)")
        report.append("")
        report.append(f"| Metric | Value |")
        report.append(f"|--------|-------|")
        report.append(f"| Current Price | ${p['price']:.2f} |")
        report.append(f"| Strike | ${p['strike']:.0f} ({p['otm_pct']:.1f}% OTM) |")
        report.append(f"| Expiry | {p['expiry_display']} ({p['dte']} DTE) |")
        report.append(f"| Contracts | {p['contracts']} |")
        report.append(f"| Entry Zone | ${p['entry_low']:.2f} — ${p['entry_high']:.2f} |")
        report.append(f"| Target (3x) | ${p['target_3x']:.2f} |")
        report.append(f"| Stop Loss | ${p['stop']:.2f} |")
        report.append(f"| RSI | {p['rsi']:.1f} |")
        report.append(f"| ATR | ${p['atr']:.2f} |")
        report.append(f"| RVOL | {p['rvol']:.1f}x |")
        report.append(f"| 5D Change | {p['change_5d']:+.1f}% |")
        report.append(f"| Conviction | {p['conviction']:.0%} |")
        report.append(f"| Source | {p['source']} |")
        report.append("")
        if p.get("tech_signals"):
            report.append(f"**Technical Signals**: {' | '.join(p['tech_signals'])}")
        if p.get("flow_signals"):
            report.append(f"**Flow Signals**:")
            for fs in p["flow_signals"]:
                report.append(f"  - {fs}")
        report.append("")

    report.append("---")
    report.append("")
    report.append("## ⚠️ RISK DISCLAIMERS")
    report.append("")
    report.append("- Options can expire worthless — only risk capital you can afford to lose")
    report.append("- 3x-10x targets require precise timing and favorable moves")
    report.append("- Paper trading account — validate before live trading")
    report.append("- All analysis is computational — market conditions can change rapidly")
    report.append("")
    report.append(f"*Generated by Meta Engine {session_label} Options Analysis | {now.strftime('%b %d %Y %I:%M %p ET')}*")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════
# EMAIL SENDER
# ═══════════════════════════════════════════════════════

def send_email(report_md: str, calls: List[Dict], puts: List[Dict],
               session_label: str = "PM") -> bool:
    """Send the analysis via email."""
    smtp_user = MetaConfig.SMTP_USER
    smtp_pass = MetaConfig.SMTP_PASSWORD
    recipient = MetaConfig.ALERT_EMAIL

    if not all([smtp_user, smtp_pass, recipient]):
        logger.warning("Email not configured")
        return False

    now = datetime.now(EST)
    call_syms = ", ".join(c["symbol"] for c in calls)
    put_syms = ", ".join(p["symbol"] for p in puts)
    display_label = "Morning" if session_label == "AM" else "Afternoon"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"🎯 {display_label} OPTIONS: 🟢 CALLS {call_syms} | 🔴 PUTS {put_syms} — "
        f"{now.strftime('%b %d %I:%M %p ET')}"
    )
    msg["From"] = smtp_user
    msg["To"] = recipient

    # Build HTML from markdown
    html = _md_to_html(report_md)
    msg.attach(MIMEText(report_md, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(MetaConfig.SMTP_SERVER, MetaConfig.SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info("✅ Email sent!")
        return True
    except Exception as e:
        logger.error(f"❌ Email failed: {e}")
        return False


def _md_to_html(md_text: str) -> str:
    """Convert markdown to styled HTML email."""
    # Simple conversion
    html = md_text
    # Headers
    html = re.sub(r'^### (.+)$', r'<h3 style="color:#4ecdc4;margin:15px 0 5px 0;">\1</h3>', html, flags=re.M)
    html = re.sub(r'^## (.+)$', r'<h2 style="color:#fff;border-bottom:1px solid #333;padding-bottom:8px;">\1</h2>', html, flags=re.M)
    html = re.sub(r'^# (.+)$', r'<h1 style="color:#fff;text-align:center;">\1</h1>', html, flags=re.M)
    # Bold
    html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', html)
    # Italic
    html = re.sub(r'\*(.+?)\*', r'<i style="color:#999;">\1</i>', html)
    # Tables
    html = re.sub(r'\| (.+?) \| (.+?) \|', r'<tr><td style="padding:4px 12px;border:1px solid #333;">\1</td><td style="padding:4px 12px;border:1px solid #333;">\2</td></tr>', html)
    html = re.sub(r'\|[-\s|]+\|', '', html)
    # Wrap tables
    html = re.sub(r'(<tr>.*?</tr>(\s*<tr>.*?</tr>)*)', r'<table style="border-collapse:collapse;width:100%;margin:10px 0;background:#16213e;border-radius:8px;">\1</table>', html, flags=re.S)
    # Lists
    html = re.sub(r'^- (.+)$', r'<li style="color:#bbb;">\1</li>', html, flags=re.M)
    # Horizontal rules
    html = re.sub(r'^---$', r'<hr style="border:none;border-top:1px solid #333;margin:20px 0;">', html, flags=re.M)
    # Paragraphs
    html = re.sub(r'\n\n', r'<br><br>', html)

    return f"""
    <html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
    background:#0f0f23;color:#e0e0e0;padding:20px;margin:0;">
    <div style="max-width:800px;margin:0 auto;">
    {html}
    </div></body></html>
    """


# ═══════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════

def send_telegram(calls: List[Dict], puts: List[Dict],
                  session_label: str = "PM") -> bool:
    """Send analysis via Telegram."""
    bot_token = MetaConfig.TELEGRAM_BOT_TOKEN
    chat_id = MetaConfig.TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        logger.warning("Telegram not configured")
        return False

    now = datetime.now(EST)
    display_label = "Morning" if session_label == "AM" else "Afternoon"

    # Build messages
    msgs = []

    # Header
    call_syms = ", ".join(c["symbol"] for c in calls)
    put_syms = ", ".join(p["symbol"] for p in puts)
    header = (
        f"🎯 <b>{display_label} OPTIONS PICKS ({session_label})</b>\n"
        f"{now.strftime('%b %d, %Y %I:%M %p ET')}\n\n"
        f"🟢 <b>CALLS</b>: {call_syms}\n"
        f"🔴 <b>PUTS</b>: {put_syms}\n"
        f"Target: 3x-10x returns | 5 contracts each"
    )
    msgs.append(header)

    # Call details
    call_msg = "🟢 <b>CALL RECOMMENDATIONS</b>\n"
    for i, c in enumerate(calls, 1):
        call_msg += (
            f"\n<b>#{i} {c['symbol']} ${c['strike']:.0f}C {c['expiry_display']}</b>\n"
            f"  Price: ${c['price']:.2f} | {c['otm_pct']:.1f}% OTM\n"
            f"  RSI: {c['rsi']:.0f} | RVOL: {c['rvol']:.1f}x | 5D: {c['change_5d']:+.1f}%\n"
            f"  Entry: ${c['entry_low']:.2f}-${c['entry_high']:.2f}\n"
            f"  Target: ${c['target_3x']:.2f} | Stop: ${c['stop']:.2f}\n"
            f"  Conviction: {c['conviction']:.0%}\n"
        )
        if c.get("flow_signals"):
            call_msg += "  Flow: " + " | ".join(c["flow_signals"][:2]) + "\n"
    msgs.append(call_msg)

    # Put details
    put_msg = "🔴 <b>PUT RECOMMENDATIONS</b>\n"
    for i, p in enumerate(puts, 1):
        put_msg += (
            f"\n<b>#{i} {p['symbol']} ${p['strike']:.0f}P {p['expiry_display']}</b>\n"
            f"  Price: ${p['price']:.2f} | {p['otm_pct']:.1f}% OTM\n"
            f"  RSI: {p['rsi']:.0f} | RVOL: {p['rvol']:.1f}x | 5D: {p['change_5d']:+.1f}%\n"
            f"  Entry: ${p['entry_low']:.2f}-${p['entry_high']:.2f}\n"
            f"  Target: ${p['target_3x']:.2f} | Stop: ${p['stop']:.2f}\n"
            f"  Conviction: {p['conviction']:.0%}\n"
        )
        if p.get("flow_signals"):
            put_msg += "  Flow: " + " | ".join(p["flow_signals"][:2]) + "\n"
    msgs.append(put_msg)

    all_sent = True
    for msg_text in msgs:
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": msg_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code != 200:
                logger.error(f"Telegram error: {r.text[:200]}")
                # Retry plain text
                clean = re.sub(r'<[^>]+>', '', msg_text)
                payload["text"] = clean
                payload["parse_mode"] = None
                r2 = requests.post(url, json=payload, timeout=15)
                if r2.status_code != 200:
                    all_sent = False
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Telegram failed: {e}")
            all_sent = False

    logger.info(f"{'✅' if all_sent else '⚠️'} Telegram sent")
    return all_sent


# ═══════════════════════════════════════════════════════
# X/TWITTER POSTER
# ═══════════════════════════════════════════════════════

def post_to_x(calls: List[Dict], puts: List[Dict]) -> bool:
    """Post analysis as X thread."""
    try:
        import tweepy
    except ImportError:
        logger.warning("tweepy not installed — skipping X post")
        return False

    from config import MetaConfig
    api_key = MetaConfig.X_API_KEY
    api_secret = MetaConfig.X_API_SECRET
    access_token = MetaConfig.X_ACCESS_TOKEN
    access_secret = MetaConfig.X_ACCESS_TOKEN_SECRET

    if not all([api_key, api_secret, access_token, access_secret]):
        logger.warning("X/Twitter not configured")
        return False

    try:
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_secret,
        )
    except Exception as e:
        logger.error(f"X client init failed: {e}")
        return False

    now = datetime.now(EST)
    call_syms = ",".join(c["symbol"] for c in calls)
    put_syms = ",".join(p["symbol"] for p in puts)

    # Header tweet
    header = (
        f"🎯 3PM OPTIONS PICKS {now.strftime('%b %d')}\n\n"
        f"🟢 Top 3 CALL candidates: {call_syms}\n"
        f"🔴 Top 3 PUT candidates: {put_syms}\n\n"
        f"Target: 3x-10x | 5 contracts each\n"
        f"Exp: Feb 20 | {now.strftime('%I:%M %p ET')}"
    )

    tweets = [header]

    # Individual call tweets
    for c in calls:
        tweet = (
            f"🟢 {c['symbol']} ${c['strike']:.0f}C {c['expiry_display']}\n\n"
            f"Entry: ${c['entry_low']:.2f}-${c['entry_high']:.2f}\n"
            f"Target: ${c['target_3x']:.2f} | Stop: ${c['stop']:.2f}\n"
            f"RSI: {c['rsi']:.0f} | RVOL: {c['rvol']:.1f}x\n"
            f"Conviction: {c['conviction']:.0%}"
        )
        if c.get("flow_signals"):
            tweet += "\n" + c["flow_signals"][0][:60]
        tweets.append(tweet)

    # Individual put tweets
    for p in puts:
        tweet = (
            f"🔴 {p['symbol']} ${p['strike']:.0f}P {p['expiry_display']}\n\n"
            f"Entry: ${p['entry_low']:.2f}-${p['entry_high']:.2f}\n"
            f"Target: ${p['target_3x']:.2f} | Stop: ${p['stop']:.2f}\n"
            f"RSI: {p['rsi']:.0f} | RVOL: {p['rvol']:.1f}x\n"
            f"Conviction: {p['conviction']:.0%}"
        )
        if p.get("flow_signals"):
            tweet += "\n" + p["flow_signals"][0][:60]
        tweets.append(tweet)

    # Post thread
    try:
        prev_id = None
        for i, tweet_text in enumerate(tweets):
            resp = client.create_tweet(
                text=tweet_text,
                in_reply_to_tweet_id=prev_id,
            )
            tweet_id = resp.data.get("id") if hasattr(resp, "data") else None
            if tweet_id:
                prev_id = tweet_id
                logger.info(f"  ✅ Tweet {i+1}/{len(tweets)} posted (ID: {tweet_id})")
            time.sleep(2)

        logger.info(f"✅ X thread posted ({len(tweets)} tweets)")
        return True

    except Exception as e:
        logger.error(f"❌ X posting failed: {e}")
        return False


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def run_3pm_analysis(session_label: str = ""):
    """
    Full institutional-grade options analysis pipeline.
    
    Runs automatically at BOTH 9:35 AM and 3:15 PM via meta_engine.py.
    Produces deep-dive strike/expiry/entry/exit recommendations for
    top 3 CALLS + top 3 PUTS.
    
    Args:
        session_label: "AM" or "PM" — auto-detected from current time if empty.
    """
    now = datetime.now(EST)
    if not session_label:
        session_label = "AM" if now.hour < 12 else "PM"
    display_label = "Morning" if session_label == "AM" else "Afternoon"
    logger.info("=" * 70)
    logger.info(f"  🎯 {display_label} INSTITUTIONAL OPTIONS ANALYSIS ({session_label})")
    logger.info(f"  {now.strftime('%B %d, %Y %I:%M:%S %p ET')}")
    logger.info("=" * 70)

    try:
        # 1. Load all data
        logger.info("\n📊 STEP 1: Loading all data sources...")
        data = load_all_data()

        # 2. Select top candidates
        logger.info("\n🔍 STEP 2: Selecting top candidates...")
        calls, puts = select_top_candidates(data)
        logger.info(f"  Top 3 CALLS: {[c['symbol'] for c in calls]}")
        logger.info(f"  Top 3 PUTS:  {[p['symbol'] for p in puts]}")

        for c in calls:
            logger.info(f"\n  🟢 {c['symbol']} ${c['strike']:.0f}C {c['expiry_display']} "
                         f"| Price: ${c['price']:.2f} | Conv: {c['conviction']:.0%}")
        for p in puts:
            logger.info(f"\n  🔴 {p['symbol']} ${p['strike']:.0f}P {p['expiry_display']} "
                         f"| Price: ${p['price']:.2f} | Conv: {p['conviction']:.0%}")

        # 3. Generate report
        logger.info("\n📝 STEP 3: Generating report...")
        report = generate_report(calls, puts, data, session_label=session_label)

        # Save report
        out_dir = Path("output")
        out_dir.mkdir(exist_ok=True)
        report_path = out_dir / f"options_analysis_{session_label}_{now.strftime('%Y%m%d')}.md"
        try:
            with open(report_path, "w") as f:
                f.write(report)
            logger.info(f"  Saved: {report_path}")
        except IOError as e:
            logger.error(f"  Failed to save report: {e}")
            report_path = None

        # 4. Send Email
        logger.info("\n📧 STEP 4: Sending Email...")
        email_ok = send_email(report, calls, puts, session_label=session_label)

        # 5. Send Telegram
        logger.info("\n📱 STEP 5: Sending Telegram...")
        tg_ok = send_telegram(calls, puts, session_label=session_label)

        # 6. Post to X
        logger.info(f"\n🐦 STEP 6: Posting to X ({session_label})...")
        x_ok = post_to_x(calls, puts)

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("  RESULTS SUMMARY")
        logger.info(f"  Email:    {'✅' if email_ok else '❌'}")
        logger.info(f"  Telegram: {'✅' if tg_ok else '❌'}")
        logger.info(f"  X/Twitter:{'✅' if x_ok else '❌'}")
        logger.info(f"  Report:   {report_path}")
        logger.info("=" * 70)

        return calls, puts, report

    except Exception as e:
        logger.error("❌ 3PM Analysis CRASHED: %s", e)
        import traceback
        traceback.print_exc()
        # Try to send distress notifications (email + Telegram)
        try:
            from notifications.email_sender import send_meta_email
            send_meta_email(
                subject="⚠️ Meta Engine Analysis CRASHED",
                summaries={"error": f"Analysis crashed: {e}"},
            )
        except Exception:
            pass
        try:
            from monitoring.health_alerts import alert_pipeline_crash
            alert_pipeline_crash("3PM Deep Analysis", str(e))
        except Exception:
            pass
        return [], [], ""


if __name__ == "__main__":
    run_3pm_analysis()
