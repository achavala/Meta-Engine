#!/usr/bin/env python3
"""
Forensic 30-Day Options Mover Analysis (Feb 1–27, 2026)
═══════════════════════════════════════════════════════════════════════════════

Uses REAL data only:
  - Polygon API daily bars for all 125 static universe tickers
  - TradeNova collected data: darkpool, UW flow, OI changes, GEX, IV,
    predictive signals, persistence tracker, institutional radar, insider
  - Meta Engine cross_analysis outputs

Finds:
  1. Every stock that moved >=3% in a single day (options >=1x potential)
  2. Pre-move behavior: 5-day, 2-day, 1-day signals from collected data
  3. Pattern analysis: what signals were present BEFORE big moves

NO changes to any existing code. Read-only analysis.
"""

import json
import os
import sys
import requests
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

META_DIR = Path(__file__).parent.parent
TN_DATA = Path.home() / "TradeNova" / "data"

sys.path.insert(0, str(META_DIR))
sys.path.insert(0, str(Path.home() / "PutsEngine"))

from dotenv import load_dotenv
load_dotenv(META_DIR / ".env")
load_dotenv(Path.home() / "PutsEngine" / ".env", override=False)
load_dotenv(Path.home() / "TradeNova" / ".env", override=False)

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")

# Static universe
try:
    from putsengine.config import EngineConfig
    UNIVERSE = sorted(EngineConfig.get_all_tickers())
except Exception:
    UNIVERSE = []

START_DATE = "2026-02-01"
END_DATE = "2026-02-27"
MIN_MOVE_PCT = 3.0  # >=3% stock move = >=1x options potential (ATM weekly)


def fetch_daily_bars(symbol: str) -> list:
    """Fetch daily OHLCV bars from Polygon for Feb 1–27."""
    if not POLYGON_KEY:
        return []
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{START_DATE}/{END_DATE}"
    try:
        r = requests.get(url, params={
            "adjusted": "true", "sort": "asc", "limit": 50, "apiKey": POLYGON_KEY
        }, timeout=15)
        if r.status_code == 200:
            return r.json().get("results", [])
    except Exception:
        pass
    return []


def analyze_moves(bars: list) -> list:
    """Find all days with >= MIN_MOVE_PCT intraday or gap move."""
    moves = []
    for i, bar in enumerate(bars):
        ts = bar.get("t", 0) / 1000 if bar.get("t", 0) > 1e10 else bar.get("t", 0)
        try:
            d = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        except Exception:
            continue
        o = bar.get("o", 0)
        h = bar.get("h", 0)
        l = bar.get("l", 0)
        c = bar.get("c", 0)
        v = bar.get("v", 0)
        if o <= 0:
            continue
        day_change_pct = ((c - o) / o) * 100
        high_pct = ((h - o) / o) * 100
        low_pct = ((o - l) / o) * 100
        gap_pct = 0
        if i > 0:
            prev_c = bars[i-1].get("c", 0)
            if prev_c > 0:
                gap_pct = ((o - prev_c) / prev_c) * 100

        max_move = max(abs(day_change_pct), abs(high_pct), abs(low_pct))
        if max_move >= MIN_MOVE_PCT:
            direction = "UP" if day_change_pct > 0 else "DOWN"
            moves.append({
                "date": d,
                "open": o, "high": h, "low": l, "close": c,
                "volume": v,
                "day_change_pct": round(day_change_pct, 2),
                "high_from_open_pct": round(high_pct, 2),
                "low_from_open_pct": round(low_pct, 2),
                "gap_pct": round(gap_pct, 2),
                "max_intraday_move": round(max_move, 2),
                "direction": direction,
                "bar_idx": i,
            })
    return moves


def load_darkpool_data() -> dict:
    """Load darkpool_cache.json → {symbol: {prints, total_value, block_count...}}"""
    try:
        return json.load(open(TN_DATA / "darkpool_cache.json"))
    except Exception:
        return {}


def load_uw_flow() -> dict:
    """Load UW flow cache → per-ticker flow data."""
    try:
        d = json.load(open(TN_DATA / "uw_flow_cache.json"))
        return d.get("flow_data", d)
    except Exception:
        return {}


def load_uw_oi() -> dict:
    """Load OI change cache → per-ticker OI data."""
    try:
        d = json.load(open(TN_DATA / "uw_oi_change_cache.json"))
        return d.get("data", d)
    except Exception:
        return {}


def load_uw_gex() -> dict:
    """Load GEX cache → per-ticker GEX data."""
    try:
        d = json.load(open(TN_DATA / "uw_gex_cache.json"))
        return d.get("data", d)
    except Exception:
        return {}


def load_uw_iv() -> dict:
    """Load IV term structure cache."""
    try:
        d = json.load(open(TN_DATA / "uw_iv_term_cache.json"))
        return d.get("data", d) if isinstance(d, dict) else {}
    except Exception:
        return {}


def load_uw_skew() -> dict:
    """Load skew cache."""
    try:
        d = json.load(open(TN_DATA / "uw_skew_cache.json"))
        return d.get("data", d) if isinstance(d, dict) else {}
    except Exception:
        return {}


def load_persistence() -> dict:
    """Load persistence tracker → {symbol: days_persistent}."""
    try:
        d = json.load(open(TN_DATA / "persistence_tracker.json"))
        return d.get("candidates", d) if isinstance(d, dict) else {}
    except Exception:
        return {}


def load_predictive_signals() -> dict:
    """Load predictive_signals.json (multi-day history)."""
    try:
        return json.load(open(TN_DATA / "predictive_signals.json"))
    except Exception:
        return {}


def load_institutional_radar() -> dict:
    """Load institutional radar daily."""
    try:
        return json.load(open(TN_DATA / "institutional_radar_daily.json"))
    except Exception:
        return {}


def load_insider_cache() -> dict:
    """Load insider/finviz data."""
    try:
        return json.load(open(TN_DATA / "finviz_insider_cache.json"))
    except Exception:
        return {}


def load_cross_analysis_for_date(d: str) -> dict:
    """Load Meta cross_analysis for a date."""
    p = META_DIR / "output" / f"cross_analysis_{d.replace('-','')}.json"
    try:
        if p.exists():
            return json.load(open(p))
    except Exception:
        pass
    return {}


def get_pre_move_bars(all_bars: list, move_bar_idx: int, days_back: int) -> list:
    """Get bars N days before the move."""
    start = max(0, move_bar_idx - days_back)
    return all_bars[start:move_bar_idx]


def compute_pre_move_features(pre_bars: list) -> dict:
    """Compute features from pre-move price bars."""
    if not pre_bars:
        return {}
    closes = [b.get("c", 0) for b in pre_bars]
    volumes = [b.get("v", 0) for b in pre_bars]
    if not closes or closes[0] <= 0:
        return {}
    total_return = ((closes[-1] - closes[0]) / closes[0]) * 100
    avg_vol = sum(volumes) / len(volumes) if volumes else 0
    max_vol = max(volumes) if volumes else 0
    vol_trend = (volumes[-1] / avg_vol) if avg_vol > 0 else 1.0
    daily_ranges = []
    for b in pre_bars:
        if b.get("o", 0) > 0:
            daily_ranges.append(((b["h"] - b["l"]) / b["o"]) * 100)
    avg_range = sum(daily_ranges) / len(daily_ranges) if daily_ranges else 0
    return {
        "pre_return_pct": round(total_return, 2),
        "pre_avg_volume": int(avg_vol),
        "pre_max_volume": int(max_vol),
        "pre_vol_trend": round(vol_trend, 2),
        "pre_avg_range_pct": round(avg_range, 2),
        "pre_bars_count": len(pre_bars),
    }


def main():
    if not UNIVERSE:
        print("ERROR: Could not load static universe from PutsEngine")
        return
    if not POLYGON_KEY:
        print("ERROR: No POLYGON_API_KEY found")
        return

    print("=" * 80)
    print("FORENSIC 30-DAY OPTIONS MOVER ANALYSIS (Feb 1–27, 2026)")
    print(f"Static Universe: {len(UNIVERSE)} tickers | Min Move: {MIN_MOVE_PCT}%")
    print("=" * 80)

    # Load all TradeNova data
    print("\nLoading TradeNova collected data...")
    dp_data = load_darkpool_data()
    uw_flow = load_uw_flow()
    uw_oi = load_uw_oi()
    uw_gex = load_uw_gex()
    uw_iv = load_uw_iv()
    uw_skew = load_uw_skew()
    persistence = load_persistence()
    pred_signals = load_predictive_signals()
    inst_radar = load_institutional_radar()
    insider_data = load_insider_cache()
    print(f"  Darkpool: {len(dp_data)} tickers")
    print(f"  UW Flow: {type(uw_flow).__name__} ({len(uw_flow) if isinstance(uw_flow, (dict,list)) else '?'})")
    print(f"  UW OI: {type(uw_oi).__name__}")
    print(f"  UW GEX: {type(uw_gex).__name__}")
    print(f"  Persistence: {len(persistence) if isinstance(persistence, dict) else '?'}")
    print(f"  Predictive: {type(pred_signals).__name__}")
    print(f"  Institutional: {type(inst_radar).__name__}")

    # Fetch price data and find big movers
    print(f"\nFetching Polygon daily bars for {len(UNIVERSE)} tickers...")
    all_movers = []
    ticker_bars = {}
    batch_size = 5
    for idx, sym in enumerate(UNIVERSE):
        bars = fetch_daily_bars(sym)
        if bars:
            ticker_bars[sym] = bars
            moves = analyze_moves(bars)
            for m in moves:
                m["symbol"] = sym
                all_movers.append(m)
        if (idx + 1) % 20 == 0:
            print(f"  ... {idx+1}/{len(UNIVERSE)} tickers fetched")

    print(f"\nTotal big moves (>={MIN_MOVE_PCT}%): {len(all_movers)} across {len(set(m['symbol'] for m in all_movers))} tickers")

    # Sort by absolute move size
    all_movers.sort(key=lambda x: -abs(x["max_intraday_move"]))

    # ── ENRICHMENT: Add pre-move features + TradeNova signals ──
    print("\nEnriching with pre-move behavior and TradeNova signals...")
    for m in all_movers:
        sym = m["symbol"]
        bars = ticker_bars.get(sym, [])
        bi = m["bar_idx"]

        # Pre-move price features
        m["pre_5d"] = compute_pre_move_features(get_pre_move_bars(bars, bi, 5))
        m["pre_2d"] = compute_pre_move_features(get_pre_move_bars(bars, bi, 2))
        m["pre_1d"] = compute_pre_move_features(get_pre_move_bars(bars, bi, 1))

        # Darkpool
        dp = dp_data.get(sym, {})
        m["dp_total_value"] = dp.get("total_value", 0)
        m["dp_block_count"] = dp.get("block_count", 0) or dp.get("dark_block_count", 0)
        m["dp_prints"] = dp.get("print_count", 0)

        # UW OI
        oi_entry = {}
        if isinstance(uw_oi, dict):
            oi_entry = uw_oi.get(sym, {})
            if isinstance(oi_entry, list) and oi_entry:
                oi_entry = oi_entry[0] if isinstance(oi_entry[0], dict) else {}
        m["uw_call_oi_change"] = oi_entry.get("call_oi_change_pct", 0) if isinstance(oi_entry, dict) else 0
        m["uw_put_oi_change"] = oi_entry.get("put_oi_change_pct", 0) if isinstance(oi_entry, dict) else 0

        # GEX
        gex_entry = {}
        if isinstance(uw_gex, dict):
            gex_entry = uw_gex.get(sym, {})
            if isinstance(gex_entry, list) and gex_entry:
                gex_entry = gex_entry[0] if isinstance(gex_entry[0], dict) else {}
        m["gex_value"] = gex_entry.get("gex", 0) if isinstance(gex_entry, dict) else 0

        # Persistence
        if isinstance(persistence, dict):
            p_entry = persistence.get(sym, {})
            m["persist_days"] = p_entry.get("days", 0) if isinstance(p_entry, dict) else 0
        else:
            m["persist_days"] = 0

        # Was it in Meta cross_analysis that day?
        cross = load_cross_analysis_for_date(m["date"])
        in_meta_puts = any(p.get("symbol") == sym for p in cross.get("puts_through_moonshot", []))
        in_meta_calls = any(p.get("symbol") == sym for p in cross.get("moonshot_through_puts", []))
        m["in_meta"] = in_meta_puts or in_meta_calls
        m["meta_side"] = "PUT" if in_meta_puts else ("CALL" if in_meta_calls else "NONE")

    # ═════════════════════════════════════════════════════════════
    # OUTPUT
    # ═════════════════════════════════════════════════════════════

    print("\n" + "=" * 80)
    print(f"ALL {MIN_MOVE_PCT}%+ MOVERS: {len(all_movers)} events")
    print("=" * 80)

    # Group by size tiers
    tier_5x = [m for m in all_movers if abs(m["max_intraday_move"]) >= 8]
    tier_2x = [m for m in all_movers if 5 <= abs(m["max_intraday_move"]) < 8]
    tier_1x = [m for m in all_movers if 3 <= abs(m["max_intraday_move"]) < 5]

    print(f"\n  5x+ potential (>=8% move):  {len(tier_5x)} events")
    print(f"  2x+ potential (5-8% move):  {len(tier_2x)} events")
    print(f"  1x+ potential (3-5% move):  {len(tier_1x)} events")

    # Print all 5x+ movers with full detail
    print("\n" + "=" * 80)
    print("TIER 1: 5x+ OPTIONS POTENTIAL (>=8% intraday move)")
    print("=" * 80)
    for m in tier_5x:
        _print_mover(m)

    print("\n" + "=" * 80)
    print("TIER 2: 2x+ OPTIONS POTENTIAL (5-8% intraday move)")
    print("=" * 80)
    for m in tier_2x[:30]:
        _print_mover(m)

    print("\n" + "=" * 80)
    print("TIER 3: 1x+ OPTIONS POTENTIAL (3-5% intraday move) — top 30")
    print("=" * 80)
    for m in tier_1x[:30]:
        _print_mover(m)

    # ═════════════════════════════════════════════════════════════
    # PATTERN ANALYSIS
    # ═════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("PATTERN ANALYSIS: What do 5x+ movers have in common BEFORE the move?")
    print("=" * 80)

    if tier_5x:
        _analyze_patterns(tier_5x, "5x+")
    if tier_2x:
        _analyze_patterns(tier_2x, "2x+")

    # Meta capture rate
    print("\n" + "=" * 80)
    print("META ENGINE CAPTURE RATE")
    print("=" * 80)
    for tier_name, tier_data in [("5x+", tier_5x), ("2x+", tier_2x), ("1x+", tier_1x)]:
        total = len(tier_data)
        captured = sum(1 for m in tier_data if m["in_meta"])
        rate = (captured / total * 100) if total > 0 else 0
        print(f"  {tier_name}: {captured}/{total} = {rate:.0f}% captured by Meta cross_analysis")
        missed = [m for m in tier_data if not m["in_meta"]]
        if missed:
            syms = sorted(set(m["symbol"] for m in missed))
            print(f"    Missed symbols: {syms[:20]}")

    # DP presence
    print("\n" + "=" * 80)
    print("DARKPOOL PRESENCE AMONG BIG MOVERS")
    print("=" * 80)
    for tier_name, tier_data in [("5x+", tier_5x), ("2x+", tier_2x)]:
        has_dp = sum(1 for m in tier_data if m["dp_total_value"] > 0)
        total = len(tier_data)
        big_dp = sum(1 for m in tier_data if m["dp_total_value"] > 50_000_000)
        print(f"  {tier_name}: {has_dp}/{total} had darkpool activity, {big_dp}/{total} had DP > $50M")

    # Save full results
    out_path = META_DIR / "output" / "forensic_30day_movers_feb2026.json"
    with open(out_path, "w") as f:
        json.dump({
            "generated": datetime.now().isoformat(),
            "universe_size": len(UNIVERSE),
            "total_movers": len(all_movers),
            "tier_5x_count": len(tier_5x),
            "tier_2x_count": len(tier_2x),
            "tier_1x_count": len(tier_1x),
            "movers": all_movers,
        }, f, indent=2, default=str)
    print(f"\n📁 Full data saved: {out_path}")


def _print_mover(m: dict):
    sym = m["symbol"]
    d = m["date"]
    chg = m["day_change_pct"]
    hi = m["high_from_open_pct"]
    lo = m["low_from_open_pct"]
    gap = m["gap_pct"]
    vol = m["volume"]
    direction = m["direction"]
    in_meta = "✅META" if m["in_meta"] else "❌MISS"
    meta_side = m["meta_side"]
    dp_val = m["dp_total_value"]
    dp_blk = m["dp_block_count"]
    persist = m["persist_days"]

    print(f"\n  {sym:8s} {d} {direction:4s} Day={chg:+6.1f}% Hi={hi:+5.1f}% Lo={lo:+5.1f}% "
          f"Gap={gap:+5.1f}% Vol={vol/1e6:.1f}M [{in_meta} {meta_side}]")

    if dp_val > 0:
        print(f"    DP: ${dp_val/1e6:.0f}M ({dp_blk} blocks)")
    if persist > 0:
        print(f"    Persistence: {persist} days")

    # Pre-move behavior
    for label, key in [("5d", "pre_5d"), ("2d", "pre_2d"), ("1d", "pre_1d")]:
        pre = m.get(key, {})
        if pre:
            ret = pre.get("pre_return_pct", 0)
            vt = pre.get("pre_vol_trend", 0)
            rng = pre.get("pre_avg_range_pct", 0)
            print(f"    Pre-{label}: return={ret:+.1f}% vol_trend={vt:.1f}x avg_range={rng:.1f}%")


def _analyze_patterns(movers: list, tier_name: str):
    n = len(movers)
    if n == 0:
        return

    # Pre-5d return distribution
    pre5_returns = [m.get("pre_5d", {}).get("pre_return_pct", 0) for m in movers if m.get("pre_5d")]
    pre2_returns = [m.get("pre_2d", {}).get("pre_return_pct", 0) for m in movers if m.get("pre_2d")]
    pre1_returns = [m.get("pre_1d", {}).get("pre_return_pct", 0) for m in movers if m.get("pre_1d")]

    # Volume trend
    vol_trends = [m.get("pre_2d", {}).get("pre_vol_trend", 0) for m in movers if m.get("pre_2d")]
    avg_ranges = [m.get("pre_2d", {}).get("pre_avg_range_pct", 0) for m in movers if m.get("pre_2d")]

    # Gap behavior
    gap_pcts = [m["gap_pct"] for m in movers]

    # Darkpool
    dp_vals = [m["dp_total_value"] for m in movers]
    dp_present = sum(1 for v in dp_vals if v > 0)
    dp_whale = sum(1 for v in dp_vals if v > 100_000_000)

    # Persistence
    persist_counts = [m["persist_days"] for m in movers]
    persist_present = sum(1 for p in persist_counts if p > 0)

    # Direction
    up = sum(1 for m in movers if m["direction"] == "UP")
    down = n - up

    def _avg(lst):
        return sum(lst) / len(lst) if lst else 0
    def _median(lst):
        s = sorted(lst)
        n = len(s)
        return s[n//2] if n else 0

    print(f"\n  === {tier_name} MOVERS ({n} events) ===")
    print(f"  Direction: {up} UP ({up/n*100:.0f}%) | {down} DOWN ({down/n*100:.0f}%)")
    print(f"\n  PRE-MOVE PRICE BEHAVIOR:")
    print(f"    5-day pre-return:  avg={_avg(pre5_returns):+.1f}%  median={_median(pre5_returns):+.1f}%")
    print(f"    2-day pre-return:  avg={_avg(pre2_returns):+.1f}%  median={_median(pre2_returns):+.1f}%")
    print(f"    1-day pre-return:  avg={_avg(pre1_returns):+.1f}%  median={_median(pre1_returns):+.1f}%")
    print(f"    Gap at open:       avg={_avg(gap_pcts):+.1f}%  median={_median(gap_pcts):+.1f}%")
    print(f"\n  PRE-MOVE VOLUME:")
    print(f"    2-day vol trend:   avg={_avg(vol_trends):.2f}x  median={_median(vol_trends):.2f}x")
    print(f"    2-day avg range:   avg={_avg(avg_ranges):.1f}%  median={_median(avg_ranges):.1f}%")
    print(f"\n  MICROSTRUCTURE SIGNALS (from TradeNova collected data):")
    print(f"    Darkpool present:  {dp_present}/{n} ({dp_present/n*100:.0f}%)")
    print(f"    Darkpool >$100M:   {dp_whale}/{n} ({dp_whale/n*100:.0f}%)")
    print(f"    Persistence >0d:   {persist_present}/{n} ({persist_present/n*100:.0f}%)")

    # Most common symbols (repeat movers)
    from collections import Counter
    sym_counts = Counter(m["symbol"] for m in movers)
    repeats = [(s, c) for s, c in sym_counts.most_common(20) if c >= 2]
    if repeats:
        print(f"\n  REPEAT MOVERS (appeared 2+ times):")
        for s, c in repeats:
            print(f"    {s}: {c} big-move days")


if __name__ == "__main__":
    main()
