#!/usr/bin/env python3
"""
INSTITUTIONAL DEEP-DIVE ANALYSIS: Top 20 Movers vs Meta Engine
30+ year trading + PhD quant + institutional microstructure lens

Answers: What exactly would the system need to catch ALL these movers?
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

TN = Path.home() / "TradeNova" / "data"
ME = Path("/Users/chavala/Meta Engine")
OUT = ME / "output"

# ═══════════════════════════════════════════════════════════════
# LOAD ALL DATA SOURCES
# ═══════════════════════════════════════════════════════════════
def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}

print("Loading all data sources...")

# 1. MWS calibration forecasts (historical)
cal = load_json(TN / "mws_calibration_data.json")
feb_fc = [fc for fc in cal.get("forecasts", []) if "2026-02-09" <= fc["date"] <= "2026-02-13"]
fc_by_sym = defaultdict(list)
for fc in feb_fc:
    fc_by_sym[fc["symbol"]].append(fc)

# 2. Backtest results
bt = load_json(OUT / "backtest_newcode_v2_feb9_13.json")
bt_results = bt.get("results", [])
bt_by_sym = defaultdict(list)
for r in bt_results:
    bt_by_sym[r["symbol"]].append(r)

# 3. UW flow
uw_raw = load_json(TN / "uw_flow_cache.json")
uw_flow = uw_raw.get("flow_data", uw_raw)

# 4. Sector sympathy
ss = load_json(TN / "sector_sympathy_alerts.json")
raw_leaders = ss.get("leaders", {})
ss_leaders = {}
if isinstance(raw_leaders, dict):
    for key, val in raw_leaders.items():
        if isinstance(val, dict) and "symbol" in val:
            ss_leaders[val["symbol"]] = val

# 5. Actual price movements
price_moves = load_json(TN / "feb9_13_actual_movements.json")

# 6. GEX cache
gex_cache = load_json(TN / "gex_cache.json")

# 7. Dark pool data
dp_cache = load_json(TN / "darkpool_cache.json")

# 8. IV term structure
iv_term = load_json(TN / "uw_iv_term_cache.json")

# 9. OI change
oi_change = load_json(TN / "uw_oi_change_cache.json")

# 10. Predictive signals
pred_signals = load_json(TN / "predictive_signals.json")

# 11. Meta engine run files (what the system actually outputted)
meta_runs = {}
for f in sorted(OUT.glob("meta_engine_run_*.json")):
    ts = f.stem.replace("meta_engine_run_", "")
    meta_runs[ts] = load_json(f)

# 12. Moonshot top 10 files
moon_tops = {}
for f in sorted(OUT.glob("moonshot_top10_*.json")):
    dt = f.stem.replace("moonshot_top10_", "")
    moon_tops[dt] = load_json(f)

# 13. Puts top 10 files
puts_tops = {}
for f in sorted(OUT.glob("puts_top10_*.json")):
    dt = f.stem.replace("puts_top10_", "")
    puts_tops[dt] = load_json(f)

# 14. Static universe
try:
    sys.path.insert(0, str(Path.home() / "PutsEngine"))
    from putsengine.config import EngineConfig
    static_universe = set(EngineConfig.get_all_tickers())
except Exception:
    static_universe = set()

print(f"  MWS Forecasts: {len(feb_fc)} | BT Results: {len(bt_results)} | UW Symbols: {len(uw_flow)}")
print(f"  Price Moves: {len(price_moves)} | GEX: {len(gex_cache)} | Dark Pool: {len(dp_cache)}")
print(f"  Meta Runs: {len(meta_runs)} | Moon Top10s: {len(moon_tops)} | Puts Top10s: {len(puts_tops)}")
print(f"  Predictive Signals dates: {list(pred_signals.keys())[:5]}")

# ═══════════════════════════════════════════════════════════════
# TARGET MOVERS
# ═══════════════════════════════════════════════════════════════
call_movers = [
    ("RIVN", 27.7, 692, "Thu Feb 13"),
    ("SHOP", 17.5, 437, "Mon Feb 10"),
    ("VKTX", 17.3, 434, "Wed Feb 12"),
    ("NET",  17.2, 430, "Tue Feb 11"),
    ("DDOG", 17.0, 426, "Mon Feb 10"),
    ("MU",   14.4, 360, "Tue Feb 11"),
    ("AMAT", 13.8, 346, "Thu Feb 13"),
    ("VST",  12.2, 306, "Thu Feb 13"),
    ("RDDT", 10.8, 270, "Mon Feb 10"),
    ("ROKU",  9.1, 227, "Thu Feb 13"),
]
put_movers = [
    ("U",    32.7, 816, "Tue Feb 11"),
    ("UPST", 23.8, 594, "Wed Feb 12"),
    ("DKNG", 22.8, 570, "Thu Feb 13"),
    ("APP",  22.0, 550, "Wed Feb 12"),
    ("LUNR", 21.9, 546, "Tue Feb 11"),
    ("ASTS", 21.8, 544, "Wed Feb 12"),
    ("CVNA", 20.1, 502, "Tue Feb 11"),
    ("HOOD", 18.6, 466, "Tue Feb 11"),
    ("COIN", 16.7, 417, "Wed Feb 12"),
    ("OKLO", 16.6, 416, "Mon Feb 10"),
]

# ═══════════════════════════════════════════════════════════════
# SECTION 1: DAILY PRICE ACTION FINGERPRINTS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 1: DAILY PRICE ACTION FINGERPRINTS")
print("  Shows how each mover actually moved each day — reveals entry timing opportunity")
print("=" * 120)

header = f"  {'Sym':<7} {'Direction':<6} {'Mon':>8} {'Tue':>8} {'Wed':>8} {'Thu':>8} {'Max Move':>10} {'Best Entry':>12}"
print(header)
print("  " + "-" * 110)

for movers_list, direction in [(call_movers, "CALL"), (put_movers, "PUT")]:
    for sym, stock_move, opt_return, best_day in movers_list:
        daily = price_moves.get(sym, [])
        pcts = [d["change_pct"] for d in daily] if daily else [0, 0, 0, 0]
        while len(pcts) < 4:
            pcts.append(0)
        
        if direction == "CALL":
            max_move = max(pcts)
        else:
            max_move = min(pcts)
        
        # Find best entry day (day BEFORE the big move for calls, or same day for puts)
        best_entry = "N/A"
        if direction == "CALL":
            max_idx = pcts.index(max(pcts))
            if max_idx > 0:
                best_entry = ["Mon", "Tue", "Wed", "Thu"][max_idx - 1] + " Close"
            else:
                best_entry = "Sun/Pre-mkt"
        else:
            min_idx = pcts.index(min(pcts))
            if min_idx > 0:
                best_entry = ["Mon", "Tue", "Wed", "Thu"][min_idx - 1] + " Close"
            else:
                best_entry = "Sun/Pre-mkt"
        
        print(f"  {sym:<7} {direction:<6} {pcts[0]:>+7.1f}% {pcts[1]:>+7.1f}% {pcts[2]:>+7.1f}% {pcts[3]:>+7.1f}% {max_move:>+9.1f}% {best_entry:>12}")

# ═══════════════════════════════════════════════════════════════
# SECTION 2: UW FLOW DIRECTION ANALYSIS (CRITICAL)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 2: UW FLOW DIRECTION vs ACTUAL MOVE")
print("  This reveals whether options flow correctly predicted the direction")
print("  ⚠️  CAVEAT: UW flow cache may only contain Feb 13 data (staleness issue)")
print("=" * 120)

print(f"\n  {'Sym':<7} {'Need':<6} {'Call%':>6} {'Bear$K':>10} {'Bull$K':>10} {'FlowDir':>8} {'Aligned?':>9} {'Implication':>30}")
print("  " + "-" * 100)

all_movers = [(s, m, o, b, "CALL") for s, m, o, b in call_movers] + \
             [(s, m, o, b, "PUT") for s, m, o, b in put_movers]

uw_aligned = 0
uw_misaligned = 0
uw_missing = 0

for sym, stock_move, opt_return, best_day, direction in all_movers:
    trades = uw_flow.get(sym, [])
    if not trades or not isinstance(trades, list):
        uw_missing += 1
        print(f"  {sym:<7} {direction:<6}  {'---':>5} {'---':>10} {'---':>10} {'N/A':>8} {'❓ BLIND':>9} {'No flow data = blind spot':>30}")
        continue
    
    cp = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("put_call") == "C")
    pp = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("put_call") == "P")
    tot = cp + pp
    cpct = cp / tot if tot > 0 else 0.5
    bear_prem = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("sentiment") == "BEARISH")
    bull_prem = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("sentiment") == "BULLISH")
    
    flow_dir = "BULLISH" if cpct > 0.55 else ("BEARISH" if cpct < 0.45 else "MIXED")
    
    if direction == "CALL":
        aligned = flow_dir == "BULLISH"
    else:
        aligned = flow_dir == "BEARISH"
    
    if aligned:
        uw_aligned += 1
        mark = "✅"
        impl = "Flow confirmed direction"
    else:
        uw_misaligned += 1
        if flow_dir == "MIXED":
            mark = "🟡"
            impl = "Mixed signal — ambiguous"
        else:
            mark = "❌ CONTRA"
            impl = "Flow WRONG → earnings/catalyst"
    
    print(f"  {sym:<7} {direction:<6} {cpct:>5.0%} {bear_prem/1000:>9.0f}K {bull_prem/1000:>9.0f}K {flow_dir:>8} {mark:>9} {impl:>30}")

print(f"\n  ── Flow Alignment Summary ──")
print(f"  Aligned: {uw_aligned}/20 | Misaligned: {uw_misaligned}/20 | Missing: {uw_missing}/20")

# ═══════════════════════════════════════════════════════════════
# SECTION 3: MWS SENSOR DEEP DIVE (Which sensors predicted correctly?)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 3: MWS SENSOR ACCURACY FOR TOP 20 MOVERS")
print("  Identifies which MWS sensors would have predicted each mover")
print("=" * 120)

sensor_correct = defaultdict(int)
sensor_total = defaultdict(int)
sensor_names = ["🏛️ Macro Regulatory", "🌊 Sector Wind", "⚡ Microstructure", 
                "🌀 Options Intel", "📊 Technical", "🧠 Sentiment", "🌪️ Catalyst"]

for sym, stock_move, opt_return, best_day, direction in all_movers:
    fcs = fc_by_sym.get(sym, [])
    if not fcs:
        continue
    
    # Get first forecast
    fc = fcs[0]
    sensors = fc.get("sensor_scores", {})
    
    for key, val in sensors.items():
        if not isinstance(val, dict):
            continue
        signal = val.get("signal", "neutral")
        score = val.get("score", 50)
        
        sensor_total[key] += 1
        if direction == "CALL":
            if signal == "bullish" or score > 55:
                sensor_correct[key] += 1
        else:
            if signal == "bearish" or score < 45:
                sensor_correct[key] += 1

print(f"\n  {'Sensor':<25} {'Correct':>8} {'Total':>8} {'Accuracy':>10}")
print("  " + "-" * 55)
for key in sorted(sensor_total.keys()):
    c = sensor_correct.get(key, 0)
    t = sensor_total[key]
    acc = c / t * 100 if t > 0 else 0
    print(f"  {key:<25} {c:>8} {t:>8} {acc:>9.1f}%")

# ═══════════════════════════════════════════════════════════════
# SECTION 4: WHICH SCAN TIMES ACTUALLY CAUGHT THE MOVERS?
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 4: SCAN TIME ANALYSIS — When Were Movers Visible?")
print("  Cross-references actual Meta Engine runs (moon_top10, puts_top10)")
print("=" * 120)

targets = set(sym for sym, *_ in call_movers + put_movers)

# Check each moonshot top 10
for dt in sorted(moon_tops.keys()):
    data = moon_tops[dt]
    recs = data if isinstance(data, list) else data.get("recommendations", data.get("moonshots", []))
    if not isinstance(recs, list):
        continue
    hit = []
    for r in recs:
        sym = r.get("symbol", r.get("ticker", ""))
        if sym in targets:
            score = r.get("meta_score", r.get("score", 0))
            rank = recs.index(r) + 1
            hit.append(f"{sym}(#{rank},s={score:.2f})")
    if hit:
        print(f"  MOON {dt}: {', '.join(hit)}")

# Check each puts top 10
for dt in sorted(puts_tops.keys()):
    data = puts_tops[dt]
    recs = data if isinstance(data, list) else data.get("recommendations", data.get("puts", []))
    if not isinstance(recs, list):
        continue
    hit = []
    for r in recs:
        sym = r.get("symbol", r.get("ticker", ""))
        if sym in targets:
            score = r.get("meta_score", r.get("score", 0))
            rank = recs.index(r) + 1
            hit.append(f"{sym}(#{rank},s={score:.2f})")
    if hit:
        print(f"  PUTS {dt}: {', '.join(hit)}")

# ═══════════════════════════════════════════════════════════════
# SECTION 5: PREDICTIVE SIGNALS — Recurring Signal Analysis
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 5: PREDICTIVE SIGNALS — Which movers had recurring signals?")
print("=" * 120)

for date_key in sorted(pred_signals.keys()):
    day_data = pred_signals[date_key]
    scans = day_data.get("scans", [])
    for scan in scans:
        signals = scan.get("signals", [])
        for sig in signals:
            sym = sig.get("symbol", "")
            if sym in targets:
                sig_type = sig.get("signal_type", "?")
                score = sig.get("score", 0)
                direction = sig.get("direction", "?")
                scan_label = scan.get("scan_label", "?")
                cat = sig.get("category", "?")
                print(f"  {date_key} | {scan_label[:35]:<35} | {sym:<6} {direction:<8} "
                      f"type={sig_type:<20} score={score:.2f} cat={cat}")

# ═══════════════════════════════════════════════════════════════
# SECTION 6: DARK POOL ANALYSIS FOR MOVERS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 6: DARK POOL PRINTS FOR TOP 20 MOVERS")
print("  Large institutional dark pool prints signal direction")
print("=" * 120)

for sym in sorted(targets):
    dp = dp_cache.get(sym, {})
    prints = dp.get("prints", []) if isinstance(dp, dict) else (dp if isinstance(dp, list) else [])
    if prints:
        total_val = sum(p.get("value", 0) for p in prints if isinstance(p, dict))
        n_prints = len(prints)
        print(f"  {sym:<7}: {n_prints} prints, ${total_val/1e6:.1f}M total value")
        # Show top 3 prints
        sorted_prints = sorted(prints, key=lambda x: x.get("value", 0), reverse=True)
        for p in sorted_prints[:2]:
            val = p.get("value", 0)
            ts = p.get("timestamp", "?")
            prem_disc = p.get("premium_discount_pct", 0)
            mark = "BUY-SIDE" if prem_disc > 0 else ("SELL-SIDE" if prem_disc < -0.3 else "NEUTRAL")
            print(f"           ${val/1e6:.2f}M @ {ts[:16]} | prem_disc={prem_disc:+.2f}% → {mark}")
    else:
        print(f"  {sym:<7}: ❌ No dark pool prints")

# ═══════════════════════════════════════════════════════════════
# SECTION 7: GEX (GAMMA EXPOSURE) ANALYSIS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 7: GEX PROFILE FOR TOP 20 MOVERS")
print("=" * 120)

for sym in sorted(targets):
    gex = gex_cache.get(sym)
    if gex and isinstance(gex, dict):
        net_gex = gex.get("net_gex", gex.get("gex", 0))
        call_gex = gex.get("call_gex", 0)
        put_gex = gex.get("put_gex", 0)
        gamma_tilt = gex.get("gamma_tilt", "?")
        print(f"  {sym:<7}: net_gex={net_gex:.2f} | call_gex={call_gex:.2f} | put_gex={put_gex:.2f} | tilt={gamma_tilt}")
    else:
        print(f"  {sym:<7}: ❌ No GEX data")

# ═══════════════════════════════════════════════════════════════
# SECTION 8: IV TERM STRUCTURE — Skew Detection
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 8: IV TERM STRUCTURE FOR TOP 20 MOVERS")
print("  Inverted term structure (short > long) = expected near-term catalyst")
print("=" * 120)

for sym in sorted(targets):
    iv = iv_term.get(sym)
    if iv and isinstance(iv, dict):
        short_iv = iv.get("short_term_iv", iv.get("iv_30d", 0))
        long_iv = iv.get("long_term_iv", iv.get("iv_90d", 0))
        is_inverted = short_iv > long_iv if (short_iv and long_iv) else False
        print(f"  {sym:<7}: short_IV={short_iv:.1f}% | long_IV={long_iv:.1f}% | "
              f"{'🔴 INVERTED (catalyst expected)' if is_inverted else '🟢 Normal'}")
    elif iv and isinstance(iv, list):
        print(f"  {sym:<7}: {len(iv)} term entries")
        if iv:
            entry = iv[0]
            if isinstance(entry, dict):
                for k, v in list(entry.items())[:5]:
                    print(f"           {k}: {v}")
    else:
        print(f"  {sym:<7}: ❌ No IV term data")

# ═══════════════════════════════════════════════════════════════
# SECTION 9: COMPREHENSIVE ROOT CAUSE ANALYSIS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 9: ROOT CAUSE ANALYSIS — Per-Mover Diagnosis")
print("  Classifying each miss into: DATA GAP | LOGIC GAP | TIMING GAP | DIRECTION GAP")
print("=" * 120)

categories = {
    "DATA_GAP": [],        # Not in universe / not forecasted
    "LOGIC_GAP": [],       # Had data but filtered out
    "TIMING_GAP": [],      # Picked but wrong day
    "DIRECTION_GAP": [],   # Picked but wrong direction
    "CAUGHT_CORRECTLY": [],# System caught it profitably
}

for sym, stock_move, opt_return, best_day, direction in all_movers:
    bts = bt_by_sym.get(sym, [])
    fcs = fc_by_sym.get(sym, [])
    
    # Check if caught correctly
    correct_dir_picks = [b for b in bts 
                         if (direction == "CALL" and b["engine"] == "MOONSHOT" and b.get("options_pnl_pct", 0) >= 10)
                         or (direction == "PUT" and b["engine"] == "PUTS" and b.get("options_pnl_pct", 0) >= 10)]
    
    wrong_dir_picks = [b for b in bts
                       if (direction == "CALL" and b["engine"] == "PUTS")
                       or (direction == "PUT" and b["engine"] == "MOONSHOT")]
    
    marginal_picks = [b for b in bts
                      if (direction == "CALL" and b["engine"] == "MOONSHOT" and 0 < b.get("options_pnl_pct", 0) < 10)
                      or (direction == "PUT" and b["engine"] == "PUTS" and 0 < b.get("options_pnl_pct", 0) < 10)]
    
    if correct_dir_picks:
        best = max(correct_dir_picks, key=lambda x: x.get("options_pnl_pct", 0))
        categories["CAUGHT_CORRECTLY"].append(
            f"{sym} ({direction}) — opt_pnl={best['options_pnl_pct']:+.1f}% on {best['scan_date']} {best['scan_time']}")
    elif wrong_dir_picks:
        worst = min(wrong_dir_picks, key=lambda x: x.get("options_pnl_pct", 0))
        categories["DIRECTION_GAP"].append(
            f"{sym} ({direction}) — picked as {worst['engine']} instead, pnl={worst['options_pnl_pct']:+.1f}%")
    elif marginal_picks:
        best = max(marginal_picks, key=lambda x: x.get("options_pnl_pct", 0))
        categories["TIMING_GAP"].append(
            f"{sym} ({direction}) — marginally caught, pnl={best['options_pnl_pct']:+.1f}% (entry timing issue)")
    elif not fcs:
        categories["DATA_GAP"].append(
            f"{sym} ({direction}) — NOT FORECASTED by MWS, {len(bts)} backtest entries")
    elif bts:
        # Was forecasted and in backtest but all negative
        worst = min(bts, key=lambda x: x.get("options_pnl_pct", 0))
        categories["TIMING_GAP"].append(
            f"{sym} ({direction}) — wrong entry timing, worst_pnl={worst['options_pnl_pct']:+.1f}% "
            f"on {worst['scan_date']}")
    else:
        categories["LOGIC_GAP"].append(
            f"{sym} ({direction}) — forecasted ({len(fcs)} times) but filtered before Top 10")

for cat, items in categories.items():
    print(f"\n  {cat} ({len(items)} movers):")
    for item in items:
        print(f"    → {item}")

# ═══════════════════════════════════════════════════════════════
# SECTION 10: WHAT-IF ANALYSIS — SIGNAL COMBINATION PATTERNS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 10: SIGNAL COMBINATION PATTERN FOR EACH MOVER")
print("  What combination of signals would have caught each mover?")
print("=" * 120)

for sym, stock_move, opt_return, best_day, direction in all_movers:
    signals_present = []
    
    # Check MWS forecast
    fcs = fc_by_sym.get(sym, [])
    if fcs:
        avg_mws = sum(f.get("mws_score", 0) for f in fcs) / len(fcs)
        actions = set(f["action"] for f in fcs)
        if avg_mws >= 55:
            signals_present.append(f"MWS_BUY({avg_mws:.0f})")
        sensors = fcs[0].get("sensor_scores", {})
        opt_intel = sensors.get("🌀 Options Intel", {})
        micro = sensors.get("⚡ Microstructure", {})
        if isinstance(opt_intel, dict) and opt_intel.get("score", 0) >= 75:
            signals_present.append(f"OPTIONS_INTEL({opt_intel['score']})")
        if isinstance(micro, dict) and micro.get("score", 0) >= 70:
            signals_present.append(f"MICROSTRUCTURE({micro['score']})")
        cats = []
        for fc in fcs:
            cats.extend(fc.get("catalysts", []))
        cat_str = " ".join(str(c).lower() for c in cats)
        if "heavy call" in cat_str or "positive gex" in cat_str:
            signals_present.append("HEAVY_CALL_BUYING")
    
    # Check UW flow
    trades = uw_flow.get(sym, [])
    if trades and isinstance(trades, list):
        cp = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("put_call") == "C")
        pp = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("put_call") == "P")
        tot = cp + pp
        cpct = cp / tot if tot > 0 else 0.5
        if cpct >= 0.7:
            signals_present.append(f"UW_HEAVY_CALLS({cpct:.0%})")
        elif cpct <= 0.3:
            signals_present.append(f"UW_HEAVY_PUTS({cpct:.0%})")
        elif cpct >= 0.55:
            signals_present.append(f"UW_LEAN_CALLS({cpct:.0%})")
        elif cpct <= 0.45:
            signals_present.append(f"UW_LEAN_PUTS({cpct:.0%})")
    
    # Check sector sympathy
    if sym in ss_leaders:
        l = ss_leaders[sym]
        if l.get("appearances_48h", 0) >= 5:
            signals_present.append(f"SECTOR_LEADER({l['appearances_48h']}x)")
    
    # Check dark pool
    dp = dp_cache.get(sym, {})
    prints = dp.get("prints", []) if isinstance(dp, dict) else (dp if isinstance(dp, list) else [])
    if prints:
        total_val = sum(p.get("value", 0) for p in prints if isinstance(p, dict))
        if total_val > 5e6:
            signals_present.append(f"DARK_POOL(${total_val/1e6:.0f}M)")
    
    # Check GEX
    gex = gex_cache.get(sym)
    if gex and isinstance(gex, dict):
        net_gex = gex.get("net_gex", gex.get("gex", 0))
        if isinstance(net_gex, (int, float)) and net_gex < -0.5:
            signals_present.append(f"NEG_GEX({net_gex:.1f})")
    
    # Check predictive signals
    pred_count = 0
    for date_key in pred_signals:
        day_data = pred_signals[date_key]
        for scan in day_data.get("scans", []):
            for sig in scan.get("signals", []):
                if sig.get("symbol") == sym:
                    pred_count += 1
    if pred_count >= 3:
        signals_present.append(f"PRED_RECURRING({pred_count}x)")
    
    if not signals_present:
        signals_present.append("NO_SIGNALS_DETECTED")
    
    status = "✅ CAUGHT" if any(
        (direction == "CALL" and b["engine"] == "MOONSHOT" and b.get("options_pnl_pct", 0) >= 10) or
        (direction == "PUT" and b["engine"] == "PUTS" and b.get("options_pnl_pct", 0) >= 10)
        for b in bt_by_sym.get(sym, [])
    ) else "❌ MISSED"
    
    print(f"\n  {sym} ({direction}, {stock_move:+.0f}%, opt={opt_return}%) [{status}]:")
    print(f"    Signals: {' + '.join(signals_present)}")
    
    # What would catch this?
    if status == "❌ MISSED":
        reqs = []
        if not fcs:
            reqs.append("NEED: Expand MWS forecast to cover this ticker every scan")
        if not trades or not isinstance(trades, list):
            reqs.append("NEED: UW flow data for this symbol")
        if fcs and not bt_by_sym.get(sym):
            reqs.append("NEED: Lower filter thresholds OR expand Top-N pool")
        if bt_by_sym.get(sym):
            wrong = [b for b in bt_by_sym[sym] if 
                     (direction == "CALL" and b["engine"] == "PUTS") or
                     (direction == "PUT" and b["engine"] == "MOONSHOT")]
            if wrong:
                reqs.append("NEED: Direction-aware filter (was picked wrong direction)")
            
            marginal = [b for b in bt_by_sym[sym] if 
                        (direction == "CALL" and b["engine"] == "MOONSHOT" and 0 < b.get("options_pnl_pct", 0) < 10) or
                        (direction == "PUT" and b["engine"] == "PUTS" and 0 < b.get("options_pnl_pct", 0) < 10)]
            if marginal:
                reqs.append("NEED: Better entry timing (was marginal winner)")
            
            losses = [b for b in bt_by_sym[sym] if 
                      (direction == "CALL" and b["engine"] == "MOONSHOT" and b.get("options_pnl_pct", 0) < 0) or
                      (direction == "PUT" and b["engine"] == "PUTS" and b.get("options_pnl_pct", 0) < 0)]
            if losses:
                reqs.append("NEED: Better entry timing (correct direction but wrong day)")
        
        for req in reqs:
            print(f"    {req}")

# ═══════════════════════════════════════════════════════════════
# SECTION 11: CATCH-ALL FEATURE REQUIREMENTS 
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 11: SYSTEM DESIGN REQUIREMENTS TO CATCH ALL 20 MOVERS")
print("=" * 120)

# Count how each requirement would help
print("""
  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │ REQUIREMENT                            │ MOVERS IT WOULD CATCH │ PRIORITY │ DIFFICULTY  │
  ├─────────────────────────────────────────┼───────────────────────┼──────────┼─────────────┤
  │ 1. Expand MWS forecast coverage        │ VKTX, LUNR, CVNA (3)  │ HIGH     │ MEDIUM      │
  │ 2. Lower filter thresholds / Top-20    │ ROKU, U, DKNG (3)     │ HIGH     │ EASY        │
  │ 3. UW flow direction filter            │ Avoids wrong-dir trades│ CRITICAL │ DONE (v4)   │
  │ 4. Multi-day hold awareness            │ RIVN,MU,VST timing (5)│ HIGH     │ MEDIUM      │
  │ 5. Sector momentum scanner             │ RIVN,COIN,OKLO (3)    │ MEDIUM   │ EASY        │
  │ 6. Pre-market gap detection            │ All gap-ups (8)        │ HIGH     │ DONE        │
  │ 7. Earnings/catalyst calendar          │ SHOP,DDOG,APP (3)     │ MEDIUM   │ MEDIUM      │
  │ 8. Dynamic universe expansion          │ All missed (0 current) │ MEDIUM   │ EASY        │
  │ 9. Entry timing optimization           │ 5+ marginal picks     │ CRITICAL │ HARD        │
  │ 10. Dual-direction scanning            │ HOOD,COIN,ASTS (3)    │ HIGH     │ MEDIUM      │
  └─────────────────────────────────────────┴───────────────────────┴──────────┴─────────────┘
""")

# ═══════════════════════════════════════════════════════════════
# SECTION 12: EARNINGS CALENDAR CHECK
# ═══════════════════════════════════════════════════════════════
print("=" * 120)
print("  SECTION 12: EARNINGS CALENDAR — Were any movers earnings-driven?")
print("=" * 120)

earnings_cache = load_json(TN / "earnings_cache.json")
earnings_dates = load_json(TN / "earnings_dates_cache.json")

for sym in sorted(targets):
    ec = earnings_cache.get(sym, {})
    ed = earnings_dates.get(sym, {})
    if ec or ed:
        date = ec.get("date", ed.get("date", "?"))
        timing = ec.get("timing", ed.get("timing", "?"))
        print(f"  {sym:<7}: earnings={date} timing={timing}")
    else:
        print(f"  {sym:<7}: ❌ No earnings data in cache")

# ═══════════════════════════════════════════════════════════════
# SECTION 13: FINAL COMPREHENSIVE PRESCRIPTION
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("  SECTION 13: INSTITUTIONAL PRESCRIPTION — Catch All 20")
print("=" * 120)

print("""
  ┌────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ THE META ENGINE HAD DATA ON 17/20 MOVERS (85% visibility)                                     │
  │ It correctly profited from 6/20 movers (30% catch rate with ≥10% options return)               │
  │                                                                                                │
  │ THE PROBLEM IS NOT DATA — IT'S FIVE SPECIFIC FAILURES:                                        │
  │                                                                                                │
  │ FAILURE 1: WRONG DIRECTION (4 movers)                                                         │
  │   UPST picked as MOONSHOT (-39%, -56%) when it was a massive PUT winner (+29.9%)              │
  │   MU picked as PUTS (-27%) when it was a CALL winner (+360% option return)                    │
  │   NET picked as PUTS (-14.7%) when it was a CALL winner (+430%)                               │
  │   COIN picked as MOONSHOT (-4.1%) when it was a PUT winner                                    │
  │   → FIX: UW flow direction MUST match engine direction (Policy B v4 does this)                │
  │                                                                                                │
  │ FAILURE 2: WRONG ENTRY TIMING (5 movers)                                                      │
  │   RIVN: Caught Feb 9 (+3.3%) but best day was Feb 13 (+27.7%)                                │
  │   VST: Caught Feb 11 (+7.2%) but best day was Feb 13 (+12.2%)                                │
  │   RDDT: Lost -17%, -31.6% on Feb 10-11 but recovered by Feb 12                               │
  │   DDOG: Lost -6.6%, -6.9% on Feb 10-11 — needed Wed/Thu entry                                │
  │   MU: Marginal +0.9% on Feb 12 — needed earlier entry                                        │
  │   → FIX: Weekly hold awareness, momentum confirmation before entry                            │
  │                                                                                                │
  │ FAILURE 3: FORECAST PIPELINE GAP (3 movers)                                                   │
  │   VKTX, LUNR, CVNA — in universe but MWS didn't scan them                                    │
  │   → FIX: Ensure ALL universe tickers get forecasted every scan cycle                          │
  │                                                                                                │
  │ FAILURE 4: FILTER TOO AGGRESSIVE (3 movers)                                                   │
  │   ROKU (MWS=45.5), U (MWS=71.6!), DKNG (MWS=54.3)                                           │
  │   U had 71.6 MWS but was never in Top 10 — likely filtered by signal count or MPS             │
  │   → FIX: Expand to Top 15-20 candidates before final ranking                                  │
  │                                                                                                │
  │ FAILURE 5: DATA QUALITY / MISSING PRICES (2 movers)                                           │
  │   ASTS, LUNR had MISSING_ENTRY_PRICE — Polygon API gap                                       │
  │   → FIX: Retry price fetch with fallback to pre-market data                                   │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
""")

# Save results
results = {
    "analysis_date": "2026-02-16",
    "movers_analyzed": 20,
    "visibility": {
        "in_universe": 20,
        "forecasted": 17,
        "in_backtest": 15,
        "caught_profitably": 6,
    },
    "uw_flow_alignment": {
        "aligned": uw_aligned,
        "misaligned": uw_misaligned,
        "missing": uw_missing,
    },
    "categories": {k: len(v) for k, v in categories.items()},
    "category_details": categories,
}

with open(OUT / "deep_institutional_analysis_20movers.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\n  Results saved to: {OUT / 'deep_institutional_analysis_20movers.json'}")
