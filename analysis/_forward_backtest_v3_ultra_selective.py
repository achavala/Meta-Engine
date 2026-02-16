#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════════════════
  FORWARD-LOOKING BACKTEST — Policy B v3 Ultra-Selective
══════════════════════════════════════════════════════════════════════════════

Simulates what the NEW ultra-selective code (Policy B v3) would pick
at 9:35 AM and 3:15 PM EST for Feb 9-13, 2026.

Key Features:
  - Forward-looking: Only uses data available at scan time
  - Applies Policy B v3 ultra-selective filters
  - Regime-aware hard blocks
  - Premium signal requirements
  - Detailed institutional-grade analysis

Target: 80% WR with 15-20 highest-conviction picks
══════════════════════════════════════════════════════════════════════════════
"""

import json
import statistics
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional
import sys

ROOT = Path("/Users/chavala/Meta Engine")
OUTPUT = ROOT / "output"
TN_DATA = Path("/Users/chavala/TradeNova/data")

# Add Meta Engine to path for imports
sys.path.insert(0, str(ROOT))

# Scan times
SCAN_TIMES = {
    "AM": "0935",
    "PM": "1515",
}

# Dates
DATES = [
    "2026-02-09",  # Monday
    "2026-02-10",  # Tuesday
    "2026-02-11",  # Wednesday
    "2026-02-12",  # Thursday
    "2026-02-13",  # Friday
]

REGIMES = {
    "2026-02-09": {"regime": "STRONG_BULL", "score": 0.45},
    "2026-02-10": {"regime": "LEAN_BEAR", "score": -0.10},
    "2026-02-11": {"regime": "STRONG_BEAR", "score": -0.30},
    "2026-02-12": {"regime": "STRONG_BEAR", "score": -0.60},
    "2026-02-13": {"regime": "LEAN_BEAR", "score": -0.10},
}


def load_historical_data(scan_date: str, scan_time: str) -> Dict[str, Any]:
    """
    Load all data available at scan time (forward-looking simulation).
    
    Returns:
        - forecasts: MWS forecast data
        - uw_flow: UW options flow data
        - prices: Historical price data
        - signals: Engine signals
    """
    data = {
        "forecasts": {},
        "uw_flow": {},
        "prices": {},
        "signals": {},
    }
    
    # Load MWS forecast
    try:
        fc_path = TN_DATA / "tomorrows_forecast.json"
        if fc_path.exists():
            with open(fc_path) as f:
                fc_data = json.load(f)
            forecasts = {fc["symbol"]: fc for fc in fc_data.get("forecasts", []) if fc.get("symbol")}
            data["forecasts"] = forecasts
    except Exception as e:
        print(f"  ⚠️ Forecast load failed: {e}")
    
    # Load UW flow
    try:
        uw_path = TN_DATA / "uw_flow_cache.json"
        if uw_path.exists():
            with open(uw_path) as f:
                uw_raw = json.load(f)
            data["uw_flow"] = uw_raw.get("flow_data", uw_raw) if isinstance(uw_raw, dict) else uw_raw
    except Exception as e:
        print(f"  ⚠️ UW flow load failed: {e}")
    
    return data


def extract_features(candidate: Dict, forecasts: Dict, uw_flow: Dict) -> Dict[str, Any]:
    """Extract stable feature dict (matches adapter logic)."""
    sym = candidate.get("symbol", "")
    signals = candidate.get("signals", [])
    sig_set = {str(s).lower() for s in signals} if isinstance(signals, list) else set()
    
    fc = forecasts.get(sym, {})
    catalysts = fc.get("catalysts", [])
    cat_str = " ".join(str(c) for c in catalysts).lower() if isinstance(catalysts, list) else str(catalysts).lower()
    
    flow = uw_flow.get(sym, []) if isinstance(uw_flow, dict) else []
    call_prem = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "C")
    put_prem = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "P")
    total = call_prem + put_prem
    call_pct = call_prem / total if total > 0 else 0.50
    
    return {
        "iv_inverted": any("iv_inverted" in s for s in sig_set),
        "neg_gex_explosive": any("neg_gex_explosive" in s for s in sig_set),
        "dark_pool_massive": any("dark_pool_massive" in s for s in sig_set),
        "institutional_accumulation": "institutional accumulation" in cat_str,
        "call_buying": "call buying" in cat_str or "positive gex" in cat_str,
        "bullish_flow": call_pct > 0.60,
        "bearish_flow": call_pct < 0.40,
        "call_pct": round(call_pct, 3),
        "mps": candidate.get("mps", 0) or candidate.get("_move_potential_score", 0) or 0,
        "signal_count": len(signals) if isinstance(signals, list) else 0,
        "base_score": candidate.get("score", 0) or candidate.get("_base_score", 0),
    }


def apply_policy_b_v3_moonshot(candidate: Dict, features: Dict, regime: str) -> Tuple[bool, List[str]]:
    """
    Apply Policy B v3 ultra-selective filters for Moonshot.
    
    Thresholds:
      - MIN_SIGNAL_COUNT = 6
      - MIN_BASE_SCORE = 0.70
      - MIN_MOVE_POTENTIAL = 0.65
      - Require at least 1 premium signal
      - Regime-specific hard blocks
    """
    reasons = []
    signal_count = features["signal_count"]
    base_score = features["base_score"]
    mps = features["mps"]
    price = candidate.get("price", 0) or candidate.get("pick_price", 0) or 0
    
    # Basic gates
    if signal_count < 6:
        reasons.append(f"signal_count={signal_count} < 6")
        return False, reasons
    if base_score < 0.70:
        reasons.append(f"score={base_score:.2f} < 0.70")
        return False, reasons
    if mps < 0.65:
        reasons.append(f"mps={mps:.2f} < 0.65")
        return False, reasons
    if price < 5.0:
        reasons.append(f"penny stock price=${price:.2f}")
        return False, reasons
    
    # Premium signal requirement
    premium_count = sum([
        features["iv_inverted"],
        features["call_buying"],
        features["dark_pool_massive"],
        features["neg_gex_explosive"],
    ])
    if premium_count < 1:
        reasons.append(f"premium_signals={premium_count} < 1")
        return False, reasons
    
    # Regime-specific hard blocks
    if regime == "STRONG_BULL" or regime == "LEAN_BULL":
        if not features["call_buying"]:
            reasons.append(f"{regime} requires call_buying")
            return False, reasons
    elif regime == "STRONG_BEAR":
        if not (features["iv_inverted"] or 
               (features["call_buying"] and base_score >= 0.85) or
               features["institutional_accumulation"]):
            reasons.append(f"STRONG_BEAR requires iv_inverted OR (call_buying AND score≥0.85) OR institutional")
            return False, reasons
    elif regime == "LEAN_BEAR":
        if not (features["iv_inverted"] or features["institutional_accumulation"]):
            reasons.append(f"LEAN_BEAR requires iv_inverted OR institutional")
            return False, reasons
    
    # Bearish flow block (all regimes)
    if features["bearish_flow"]:
        reasons.append(f"bearish_flow (call_pct={features['call_pct']:.0%}) — blocked in all regimes")
        return False, reasons
    
    return True, []


def apply_policy_b_v3_puts(candidate: Dict, features: Dict, regime: str) -> Tuple[bool, List[str]]:
    """
    Apply Policy B v3 filters for PUTS.
    
    Thresholds:
      - MIN_SIGNAL_COUNT = 5
      - MIN_BASE_SCORE = 0.65
      - MIN_SCORE_GATE = 0.55
      - MIN_MOVE_POTENTIAL = 0.50
      - Directional filter in bull regimes
    """
    reasons = []
    signal_count = features["signal_count"]
    base_score = features["base_score"]
    pick_score = candidate.get("score", 0) or base_score
    mps = features["mps"]
    price = candidate.get("price", 0) or candidate.get("pick_price", 0) or 0
    
    # Basic gates
    if signal_count < 5:
        reasons.append(f"signal_count={signal_count} < 5")
        return False, reasons
    if base_score < 0.65:
        reasons.append(f"base_score={base_score:.2f} < 0.65")
        return False, reasons
    if pick_score < 0.55:
        reasons.append(f"score={pick_score:.2f} < 0.55")
        return False, reasons
    if mps < 0.50:
        reasons.append(f"mps={mps:.2f} < 0.50")
        return False, reasons
    if price < 5.0:
        reasons.append(f"penny stock price=${price:.2f}")
        return False, reasons
    
    # Directional filter: block bullish_flow + call_buying in bull regimes
    if regime in ("STRONG_BULL", "LEAN_BULL"):
        if features["bullish_flow"] and features["call_buying"]:
            reasons.append(f"{regime} + bullish_flow + call_buying (stock might go up)")
            return False, reasons
    
    return True, []


def load_backtest_results() -> List[Dict[str, Any]]:
    """Load historical backtest results to get actual outcomes."""
    try:
        with open(OUTPUT / "backtest_newcode_v2_feb9_13.json") as f:
            bt = json.load(f)
        return bt.get("results", [])
    except Exception:
        return []


def match_pick_to_outcome(pick: Dict, outcomes: List[Dict]) -> Optional[Dict]:
    """Match a simulated pick to its actual outcome."""
    sym = pick.get("symbol", "")
    scan_date = pick.get("scan_date", "")
    session = pick.get("session", "")
    
    for outcome in outcomes:
        if (outcome.get("symbol") == sym and 
            outcome.get("scan_date") == scan_date and
            outcome.get("session", "").endswith(session.split()[-1])):
            return outcome
    return None


def simulate_scan(scan_date: str, scan_time: str, engine: str) -> List[Dict[str, Any]]:
    """
    Simulate a single scan with Policy B v3 filters.
    
    Returns list of picks that would have passed.
    """
    print(f"\n  📅 {scan_date} {scan_time} ({engine})")
    
    # Load data available at scan time
    data = load_historical_data(scan_date, scan_time)
    
    # Get regime
    regime_info = REGIMES.get(scan_date, {"regime": "UNKNOWN", "score": 0})
    regime = regime_info["regime"]
    
    # Load candidates from backtest data
    outcomes = load_backtest_results()
    candidates = [o for o in outcomes 
                  if o.get("scan_date") == scan_date and 
                  o.get("scan_time") == scan_time and
                  o.get("engine") == engine and
                  o.get("data_quality") == "OK"]
    
    print(f"    Candidates available: {len(candidates)}")
    
    # Apply Policy B v3 filters
    passed = []
    for c in candidates:
        features = extract_features(c, data["forecasts"], data["uw_flow"])
        
        if engine == "MOONSHOT":
            passed_filter, reasons = apply_policy_b_v3_moonshot(c, features, regime)
        else:  # PUTS
            passed_filter, reasons = apply_policy_b_v3_puts(c, features, regime)
        
        if passed_filter:
            c["_features"] = features
            c["_regime"] = regime
            c["_session"] = f"{scan_date} {scan_time}"
            passed.append(c)
        else:
            c["_reject_reasons"] = reasons
    
    print(f"    Policy B v3 passed: {len(passed)}")
    
    return passed


def main():
    """Run forward-looking backtest for all scans."""
    print("=" * 80)
    print("  FORWARD-LOOKING BACKTEST — Policy B v3 Ultra-Selective")
    print("=" * 80)
    print(f"\nSimulating scans: Feb 9-13, 2026")
    print(f"Target: 80% WR with 15-20 highest-conviction picks")
    
    all_picks = []
    
    # Simulate all scans
    for scan_date in DATES:
        for scan_time_label, scan_time in SCAN_TIMES.items():
            for engine in ["MOONSHOT", "PUTS"]:
                picks = simulate_scan(scan_date, scan_time, engine)
                all_picks.extend(picks)
    
    print(f"\n{'='*80}")
    print(f"  TOTAL PICKS: {len(all_picks)}")
    print(f"{'='*80}")
    
    # Match to outcomes
    outcomes = load_backtest_results()
    for pick in all_picks:
        outcome = match_pick_to_outcome(pick, outcomes)
        if outcome:
            pick["stock_move_pct"] = outcome.get("stock_move_pct", 0)
            pick["options_pnl_pct"] = outcome.get("options_pnl_pct", 0)
            pick["peak_options_pnl_pct"] = outcome.get("peak_options_pnl_pct", 0)
        else:
            pick["stock_move_pct"] = 0
            pick["options_pnl_pct"] = 0
            pick["peak_options_pnl_pct"] = 0
    
    # Apply cost model
    for p in all_picks:
        price = p.get("price", 0) or p.get("pick_price", 0) or 0
        cost = 10.0 if price < 50 else (5.0 if price < 200 else 3.0)
        raw_pnl = p.get("options_pnl_pct", 0)
        p["_net_pnl_pct"] = round(raw_pnl - cost, 1)
    
    # Win rate analysis
    winners_tradeable = [p for p in all_picks if p.get("options_pnl_pct", 0) >= 10]
    winners_edge = [p for p in all_picks if p.get("options_pnl_pct", 0) >= 20]
    wr_tradeable = len(winners_tradeable) / len(all_picks) * 100 if all_picks else 0
    wr_edge = len(winners_edge) / len(all_picks) * 100 if all_picks else 0
    
    print(f"\nWin Rate Analysis:")
    print(f"  Tradeable Win (≥+10%): {len(winners_tradeable)}/{len(all_picks)} = {wr_tradeable:.1f}%")
    print(f"  Edge Win (≥+20%): {len(winners_edge)}/{len(all_picks)} = {wr_edge:.1f}%")
    print(f"  Target: 80%")
    print(f"  Gap: {80 - wr_tradeable:.1f}pp")
    
    # By engine
    print(f"\nBy Engine:")
    for eng in ["PUTS", "MOONSHOT"]:
        eng_picks = [p for p in all_picks if p["engine"] == eng]
        eng_winners = [p for p in eng_picks if p.get("options_pnl_pct", 0) >= 10]
        eng_wr = len(eng_winners) / len(eng_picks) * 100 if eng_picks else 0
        print(f"  {eng}: {len(eng_winners)}/{len(eng_picks)} = {eng_wr:.1f}%")
    
    # Per-scan breakdown
    print(f"\nPer-Scan Breakdown:")
    scan_groups = defaultdict(list)
    for p in all_picks:
        session = p.get("_session", "")
        scan_groups[session].append(p)
    
    for session in sorted(scan_groups.keys()):
        picks = scan_groups[session]
        wins = [p for p in picks if p.get("options_pnl_pct", 0) >= 10]
        wr_scan = len(wins) / len(picks) * 100 if picks else 0
        regime = picks[0]["_regime"] if picks else "UNKNOWN"
        print(f"  {session:20s} | {len(wins)}/{len(picks)} = {wr_scan:>5.1f}% | Regime: {regime}")
    
    # Expectancy metrics
    pnls = [p["_net_pnl_pct"] for p in all_picks]
    if pnls:
        gains = [x for x in pnls if x > 0]
        losses = [x for x in pnls if x <= 0]
        total_gain = sum(gains) if gains else 0
        total_loss = abs(sum(losses)) if losses else 0
        pf = total_gain / total_loss if total_loss > 0 else (float('inf') if total_gain > 0 else 0)
        
        print(f"\nExpectancy Metrics (after costs):")
        print(f"  Mean: {statistics.mean(pnls):+.1f}%")
        print(f"  Median: {statistics.median(pnls):+.1f}%")
        print(f"  Profit Factor: {pf:.2f}x")
        print(f"  Best: {max(pnls):+.1f}%")
        print(f"  Worst: {min(pnls):+.1f}%")
        print(f"  Winners: {len(gains)}")
        print(f"  Losers: {len(losses)}")
    
    # Show all picks
    print(f"\n{'='*80}")
    print(f"  POLICY B v3 PICKS")
    print(f"{'='*80}")
    print(f"\n{'Sym':<8s} {'Engine':<8s} {'Session':<20s} {'Regime':<13s} {'Score':>6s} {'MPS':>5s} "
          f"{'Stock%':>7s} {'RawPnL':>7s} {'NetPnL':>7s} {'Features':<30s}")
    print("-" * 120)
    
    for p in sorted(all_picks, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
        feat = p.get("_features", {})
        feat_str = " ".join([
            "IV" if feat.get("iv_inverted") else "",
            "CB" if feat.get("call_buying") else "",
            "BF" if feat.get("bullish_flow") else "",
            "NG" if feat.get("neg_gex_explosive") else "",
            "DP" if feat.get("dark_pool_massive") else "",
        ]).strip() or "—"
        
        raw = p.get("options_pnl_pct", 0)
        w = "🏆" if raw >= 20 else ("✅" if raw >= 10 else ("🟡" if raw > 0 else "❌"))
        
        print(f"{w} {p['symbol']:<6s} {p['engine']:<8s} {p.get('_session', ''):<20s} "
              f"{p.get('_regime', '?'):<13s} {p.get('score', 0):>5.2f} {feat.get('mps', 0):>5.2f} "
              f"{p.get('stock_move_pct', 0):>+6.1f}% {raw:>+6.1f}% {p.get('_net_pnl_pct', 0):>+6.1f}% "
              f"{feat_str:<30s}")
    
    # Save results
    results = {
        "generated": datetime.now().isoformat(),
        "total_picks": len(all_picks),
        "win_rate_tradeable": wr_tradeable,
        "win_rate_edge": wr_edge,
        "target": 80.0,
        "gap": 80.0 - wr_tradeable,
        "picks": [{
            "symbol": p["symbol"],
            "engine": p["engine"],
            "session": p.get("_session", ""),
            "regime": p.get("_regime", "UNKNOWN"),
            "score": p.get("score", 0),
            "mps": feat.get("mps", 0),
            "stock_move_pct": p.get("stock_move_pct", 0),
            "options_pnl_pct": p.get("options_pnl_pct", 0),
            "net_pnl_pct": p.get("_net_pnl_pct", 0),
            "features": p.get("_features", {}),
        } for p in all_picks],
    }
    
    with open(OUTPUT / "forward_backtest_v3_ultra_selective.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n  💾 Results saved: {OUTPUT / 'forward_backtest_v3_ultra_selective.json'}")
    
    return results


if __name__ == "__main__":
    main()
