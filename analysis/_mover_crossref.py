#!/usr/bin/env python3
"""
Cross-reference Top 20 Movers (Feb 9-13, 2026) against ALL Meta Engine data sources.
Identify exactly where each mover was visible, invisible, or filtered out.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

TN = Path.home() / "TradeNova" / "data"
ME = Path("/Users/chavala/Meta Engine")

# ═══ LOAD ALL DATA SOURCES ═══
print("Loading data sources...")

# 1. MWS calibration forecasts (historical)
with open(TN / "mws_calibration_data.json") as f:
    cal = json.load(f)
feb_fc = [fc for fc in cal["forecasts"] if "2026-02-09" <= fc["date"] <= "2026-02-13"]
fc_by_sym = defaultdict(list)
for fc in feb_fc:
    fc_by_sym[fc["symbol"]].append(fc)

# 2. Backtest results
with open(ME / "output" / "backtest_newcode_v2_feb9_13.json") as f:
    bt = json.load(f)
bt_results = bt["results"]
bt_by_sym = defaultdict(list)
for r in bt_results:
    bt_by_sym[r["symbol"]].append(r)

# 3. UW flow
with open(TN / "uw_flow_cache.json") as f:
    uw_raw = json.load(f)
uw_flow = uw_raw.get("flow_data", uw_raw)

# 4. Sector sympathy (leaders is dict of key→record, alerts is dict of sym→record)
with open(TN / "sector_sympathy_alerts.json") as f:
    ss = json.load(f)
raw_leaders = ss.get("leaders", {})
ss_leaders = {}
if isinstance(raw_leaders, dict):
    for key, val in raw_leaders.items():
        if isinstance(val, dict) and "symbol" in val:
            ss_leaders[val["symbol"]] = val
raw_alerts = ss.get("alerts", {})
ss_by_sym = {}
if isinstance(raw_alerts, dict):
    ss_by_sym = {k: v for k, v in raw_alerts.items() if isinstance(v, dict)}
ss_alerts_count = len(ss_by_sym)

# 5. Current forecast
with open(TN / "tomorrows_forecast.json") as f:
    curr_fc = json.load(f)
curr_fc_by_sym = {fc["symbol"]: fc for fc in curr_fc.get("forecasts", [])}

# 6. Static universe (104 tickers from PutsEngine)
try:
    sys.path.insert(0, str(Path.home() / "PutsEngine"))
    from putsengine.config import EngineConfig
    static_universe = set(EngineConfig.get_all_tickers())
except Exception:
    static_universe = set()

# 7. Final recommendations
try:
    with open(TN / "final_recommendations.json") as f:
        frec = json.load(f)
    final_recs = {r["symbol"]: r for r in frec.get("recommendations", [])}
except Exception:
    final_recs = {}

# 8. Final recommendations history
try:
    with open(TN / "final_recommendations_history.json") as f:
        hist = json.load(f)
    hist_recs = defaultdict(list)
    for entry in hist if isinstance(hist, list) else hist.get("history", []):
        for r in entry.get("recommendations", []):
            hist_recs[r.get("symbol", "")].append(r)
except Exception:
    hist_recs = {}

# 9. EWS data
try:
    with open(TN / "ews_cache.json") as f:
        ews = json.load(f)
    ews_by_sym = {e.get("symbol", ""): e for e in ews} if isinstance(ews, list) else ews
except Exception:
    ews_by_sym = {}

# 10. Predictive signals
try:
    with open(TN / "data" / "predictive_signals.json") as f:
        pred = json.load(f)
except Exception:
    try:
        with open(TN / "predictive_signals.json") as f:
            pred = json.load(f)
    except Exception:
        pred = {}

print(f"  MWS Forecasts: {len(feb_fc)} (Feb 9-13)")
print(f"  Backtest Results: {len(bt_results)}")
print(f"  UW Flow: {len(uw_flow)} symbols")
print(f"  Sector Sympathy Leaders: {len(ss_leaders)}")
print(f"  Sector Sympathy Alerts: {ss_alerts_count}")
print(f"  Static Universe: {len(static_universe)} tickers")
print(f"  Current Forecast: {len(curr_fc_by_sym)} symbols")
print(f"  Final Recs: {len(final_recs)} symbols")
print(f"  EWS Data: {len(ews_by_sym)} symbols")

# ═══ TARGET MOVERS ═══
call_movers = [
    ("RIVN", "+27.7%", "+692%", "7.9x", "Thu Feb 13"),
    ("SHOP", "+17.5%", "+437%", "5.4x", "Mon Feb 10"),
    ("VKTX", "+17.3%", "+434%", "5.3x", "Wed Feb 12"),
    ("NET",  "+17.2%", "+430%", "5.3x", "Tue Feb 11"),
    ("DDOG", "+17.0%", "+426%", "5.3x", "Mon Feb 10"),
    ("MU",   "+14.4%", "+360%", "4.6x", "Tue Feb 11"),
    ("AMAT", "+13.8%", "+346%", "4.5x", "Thu Feb 13"),
    ("VST",  "+12.2%", "+306%", "4.1x", "Thu Feb 13"),
    ("RDDT", "+10.8%", "+270%", "3.7x", "Mon Feb 10"),
    ("ROKU", "+9.1%",  "+227%", "3.3x", "Thu Feb 13"),
]
put_movers = [
    ("U",    "-32.7%", "+816%", "9.2x", "Tue Feb 11"),
    ("UPST", "-23.8%", "+594%", "6.9x", "Wed Feb 12"),
    ("DKNG", "-22.8%", "+570%", "6.7x", "Thu Feb 13"),
    ("APP",  "-22.0%", "+550%", "6.5x", "Wed Feb 12"),
    ("LUNR", "-21.9%", "+546%", "6.5x", "Tue Feb 11"),
    ("ASTS", "-21.8%", "+544%", "6.4x", "Wed Feb 12"),
    ("CVNA", "-20.1%", "+502%", "6.0x", "Tue Feb 11"),
    ("HOOD", "-18.6%", "+466%", "5.7x", "Tue Feb 11"),
    ("COIN", "-16.7%", "+417%", "5.2x", "Wed Feb 12"),
    ("OKLO", "-16.6%", "+416%", "5.2x", "Mon Feb 10"),
]

print("\n" + "=" * 110)
print("  CROSS-REFERENCE: Top 20 Movers vs ALL Meta Engine Data Sources")
print("  Week of Feb 9-13, 2026")
print("=" * 110)

# Summary trackers
visibility_matrix = {}

for direction_label, movers, direction in [
    ("TOP CALL MOVERS (Moonshot Candidates)", call_movers, "CALL"),
    ("TOP PUT MOVERS (Puts Candidates)", put_movers, "PUT"),
]:
    print(f"\n{'='*110}")
    print(f"  {direction_label}")
    print(f"{'='*110}")

    for sym, stock_move, opt_return, multiple, best_day in movers:
        print(f"\n  {'─'*105}")
        print(f"  {sym} | Stock: {stock_move} | Options: {opt_return} ({multiple}) | Best: {best_day}")
        print(f"  {'─'*105}")

        vis = {
            "universe": False,
            "mws_forecast": False,
            "backtest_picked": False,
            "uw_flow": False,
            "sector_sympathy": False,
            "current_forecast": False,
        }

        # Check 1: In static universe?
        in_universe = sym in static_universe
        vis["universe"] = in_universe
        status = "✅ IN UNIVERSE" if in_universe else "❌ NOT IN 104-TICKER UNIVERSE — INVISIBLE TO SCANNER"
        print(f"    1️⃣  Static Universe:    {status}")

        # Check 2: In MWS forecasts for Feb 9-13?
        fcs = fc_by_sym.get(sym, [])
        vis["mws_forecast"] = len(fcs) > 0
        if fcs:
            dates = sorted(set(f["date"] for f in fcs))
            hours = sorted(set(f.get("hour", "?") for f in fcs))
            actions = sorted(set(f["action"] for f in fcs))
            avg_score = sum(f.get("mws_score", 0) for f in fcs) / len(fcs)
            regime_hostile = sum(1 for f in fcs if f.get("regime_hostile"))
            sector_pass = sum(1 for f in fcs if f.get("sector_wind_pass"))
            print(f"    2️⃣  MWS Forecast:      ✅ {len(fcs)} forecasts | dates={dates}")
            print(f"       actions={actions} | avg_MWS={avg_score:.1f} | "
                  f"regime_hostile={regime_hostile}/{len(fcs)} | sector_pass={sector_pass}/{len(fcs)}")

            # Sensor details from first forecast
            sensors = fcs[0].get("sensor_scores", {})
            if sensors:
                parts = []
                for k, v in sensors.items():
                    if isinstance(v, dict):
                        parts.append(f"{k[:12]}={v.get('signal','?')}({v.get('score',0)})")
                print(f"       sensors: {' | '.join(parts)}")

            # Check actual outcomes
            outcomes = [(f.get("actual_1d_move_pct", "?"), f.get("outcome_1d", "?"), f["date"]) for f in fcs[:3]]
            for move, outcome, dt in outcomes:
                print(f"       {dt}: 1d_move={move}, outcome={outcome}")
        else:
            print(f"    2️⃣  MWS Forecast:      ❌ NOT FORECASTED (not in MWS scan)")

        # Check 3: In backtest results?
        bts = bt_by_sym.get(sym, [])
        if bts:
            vis["backtest_picked"] = True
            for b in bts:
                pnl = b.get("options_pnl_pct", 0)
                eng = b["engine"]
                dq = b.get("data_quality", "?")
                score = b.get("score", 0)
                mps = b.get("mps", 0)
                sigs = b.get("signals", [])
                sig_cnt = b.get("signal_count", 0)
                gates = b.get("gates", {})
                passed_pb = b.get("passed_policy_b", "?")
                icon = "🏆" if pnl >= 20 else ("✅" if pnl >= 10 else ("🟡" if pnl > 0 else "❌"))
                print(f"    3️⃣  Backtest Pick:     {icon} {eng} {b['scan_date']} {b['scan_time']} | "
                      f"score={score:.3f} mps={mps:.3f} opt_pnl={pnl:+.1f}% "
                      f"sig_cnt={sig_cnt} dq={dq} passed_pb={passed_pb}")
                if not passed_pb:
                    # Show gate failures
                    fails = [k for k, v in gates.items() if v == "FAIL"] if isinstance(gates, dict) else []
                    if fails:
                        print(f"       gate_failures: {fails}")
        else:
            print(f"    3️⃣  Backtest Pick:     ❌ NOT IN ANY BACKTEST (not scanned or filtered out)")

        # Check 4: UW flow?
        trades = uw_flow.get(sym, [])
        if trades and isinstance(trades, list):
            vis["uw_flow"] = True
            cp = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("put_call") == "C")
            pp = sum(t.get("premium", 0) for t in trades if isinstance(t, dict) and t.get("put_call") == "P")
            tot = cp + pp
            cpct = cp / tot if tot > 0 else 0.5
            sweeps = sum(1 for t in trades if isinstance(t, dict) and t.get("is_sweep"))
            blocks = sum(1 for t in trades if isinstance(t, dict) and t.get("is_block"))
            unusual = sum(1 for t in trades if isinstance(t, dict) and t.get("is_unusual"))
            total_prem = tot / 1000
            sentiments = defaultdict(int)
            for t in trades:
                if isinstance(t, dict):
                    sentiments[t.get("sentiment", "UNKNOWN")] += 1
            print(f"    4️⃣  UW Flow:           ✅ {len(trades)} trades | ${total_prem:.0f}K premium | "
                  f"call_pct={cpct:.0%}")
            print(f"       sweeps={sweeps} blocks={blocks} unusual={unusual} | "
                  f"sentiment={dict(sentiments)}")
        else:
            print(f"    4️⃣  UW Flow:           ❌ NO FLOW DATA")

        # Check 5: Sector sympathy?
        if sym in ss_leaders:
            vis["sector_sympathy"] = True
            l = ss_leaders[sym]
            print(f"    5️⃣  Sector Sympathy:   ✅ LEADER (appearances={l.get('appearances_48h', 0)}, "
                  f"score={l.get('avg_score', 0):.2f}, sector={l.get('sector_name', '?')})")
        elif sym in ss_by_sym:
            vis["sector_sympathy"] = True
            alert = ss_by_sym[sym]
            leader = alert.get("leader_symbol", "?")
            sector = alert.get("sector_name", "?")
            print(f"    5️⃣  Sector Sympathy:   ✅ alert | leader={leader} | sector={sector}")
        else:
            print(f"    5️⃣  Sector Sympathy:   ❌ NONE")

        # Check 6: Current forecast?
        cfc = curr_fc_by_sym.get(sym)
        if cfc:
            vis["current_forecast"] = True
            cats = cfc.get("catalysts", [])
            cat_str = " | ".join(str(c)[:50] for c in cats[:3]) if cats else "none"
            print(f"    6️⃣  Current Forecast:  ✅ action={cfc.get('action')} "
                  f"MWS={cfc.get('mws_score',0):.1f}")
            print(f"       catalysts: [{cat_str}]")
        else:
            print(f"    6️⃣  Current Forecast:  ❌ NOT IN TODAY'S FORECAST")

        # Check 7: EWS data
        ews_entry = ews_by_sym.get(sym)
        if ews_entry and isinstance(ews_entry, dict):
            ipi = ews_entry.get("ipi", 0) or ews_entry.get("institutional_pressure_index", 0)
            print(f"    7️⃣  EWS Data:          ✅ IPI={ipi}")
        else:
            print(f"    7️⃣  EWS Data:          ❌ NONE")

        # Diagnosis
        visible_count = sum(1 for v in vis.values() if v)
        print(f"\n    🔎 VISIBILITY SCORE: {visible_count}/6 data sources")
        if not vis["universe"]:
            print(f"    ⚠️  ROOT CAUSE: Not in static universe → completely invisible to scanner")
        elif not vis["mws_forecast"]:
            print(f"    ⚠️  ROOT CAUSE: In universe but not forecasted by MWS → missed by forecast pipeline")
        elif vis["backtest_picked"]:
            # Was picked but may have been filtered
            bts_ok = [b for b in bts if b.get("data_quality") == "OK"]
            if bts_ok:
                best = max(bts_ok, key=lambda x: x.get("options_pnl_pct", 0))
                pnl = best.get("options_pnl_pct", 0)
                if pnl >= 10:
                    print(f"    ✅ SYSTEM HAD THIS — opt_pnl={pnl:+.1f}% (was in scan pool)")
                else:
                    print(f"    🟡 SYSTEM HAD THIS but low pnl={pnl:+.1f}% (entry timing issue?)")
            else:
                print(f"    🟡 SYSTEM HAD THIS but data_quality != OK (price data gap)")
        else:
            print(f"    ⚠️  In universe + forecasted but NOT in backtest picks → filtered out early")

        visibility_matrix[sym] = vis

# ═══ SUMMARY MATRIX ═══
print(f"\n\n{'='*110}")
print(f"  VISIBILITY SUMMARY MATRIX")
print(f"{'='*110}")
all_syms = [s for s, *_ in call_movers] + [s for s, *_ in put_movers]
print(f"\n  {'Symbol':<8s} {'Dir':<5s} {'Univ':>5s} {'MWS':>5s} {'BT':>5s} {'UW':>5s} {'Sect':>5s} {'CurFC':>5s} {'Score':>5s}")
print(f"  {'-'*55}")
for sym_info, direction_l in [(call_movers, "CALL"), (put_movers, "PUT")]:
    for sym, *_ in sym_info:
        v = visibility_matrix[sym]
        score = sum(1 for val in v.values() if val)
        vals = [
            "✅" if v["universe"] else "❌",
            "✅" if v["mws_forecast"] else "❌",
            "✅" if v["backtest_picked"] else "❌",
            "✅" if v["uw_flow"] else "❌",
            "✅" if v["sector_sympathy"] else "❌",
            "✅" if v["current_forecast"] else "❌",
        ]
        print(f"  {sym:<8s} {direction_l:<5s} {'  '.join(vals)}  {score}/6")

# ═══ GAP ANALYSIS ═══
print(f"\n\n{'='*110}")
print(f"  GAP ANALYSIS: Why Movers Were Missed")
print(f"{'='*110}")

not_in_universe = [s for s in all_syms if not visibility_matrix[s]["universe"]]
in_universe_not_forecast = [s for s in all_syms if visibility_matrix[s]["universe"] and not visibility_matrix[s]["mws_forecast"]]
forecast_not_picked = [s for s in all_syms if visibility_matrix[s]["mws_forecast"] and not visibility_matrix[s]["backtest_picked"]]
picked = [s for s in all_syms if visibility_matrix[s]["backtest_picked"]]

print(f"\n  GAP 1: NOT IN UNIVERSE ({len(not_in_universe)} movers)")
print(f"  These tickers are completely invisible — the scanner never sees them.")
for s in not_in_universe:
    print(f"    ❌ {s}")

print(f"\n  GAP 2: IN UNIVERSE BUT NOT FORECASTED ({len(in_universe_not_forecast)} movers)")
print(f"  These are in the scan pool but MWS didn't generate a forecast.")
for s in in_universe_not_forecast:
    print(f"    ❌ {s}")

print(f"\n  GAP 3: FORECASTED BUT NOT IN BACKTEST PICKS ({len(forecast_not_picked)} movers)")
print(f"  These had forecasts but were filtered out before reaching the Top 10.")
for s in forecast_not_picked:
    fcs = fc_by_sym.get(s, [])
    avg_score = sum(f.get("mws_score", 0) for f in fcs) / len(fcs) if fcs else 0
    print(f"    ❌ {s} (avg MWS={avg_score:.1f})")

print(f"\n  GAP 4: IN BACKTEST PICKS ({len(picked)} movers)")
print(f"  These were scanned and evaluated — check if they were winners or losers.")
for s in picked:
    bts = bt_by_sym.get(s, [])
    for b in bts:
        pnl = b.get("options_pnl_pct", 0)
        eng = b["engine"]
        icon = "🏆" if pnl >= 20 else ("✅" if pnl >= 10 else ("🟡" if pnl > 0 else "❌"))
        print(f"    {icon} {s} {eng} {b['scan_date']} opt_pnl={pnl:+.1f}%")

# ═══ WHAT'S NEEDED ═══
print(f"\n\n{'='*110}")
print(f"  SYSTEM REQUIREMENTS TO CATCH ALL 20 MOVERS")
print(f"{'='*110}")
print(f"""
  Current System Coverage: {len(picked)}/20 movers in backtest picks ({len(picked)/20*100:.0f}%)
  
  REQUIREMENT 1: EXPAND UNIVERSE
    Current: {len(static_universe)} tickers (static list)
    Missing movers NOT in universe: {not_in_universe}
    These are high-beta, mid/small-cap stocks that move the most.
    Need: Dynamic universe expansion OR separate "movers scanner" pipeline.

  REQUIREMENT 2: FORECAST COVERAGE
    In universe but no forecast: {in_universe_not_forecast}
    MWS forecasts {len(set(fc["symbol"] for fc in feb_fc))} unique symbols during Feb 9-13.
    Need: Ensure ALL universe tickers get a forecast each scan.

  REQUIREMENT 3: FILTER TUNING
    Forecasted but filtered: {forecast_not_picked}
    These had data but scored below Top 10 cutoff.
    Need: Expand candidate pool or adjust ranking.

  REQUIREMENT 4: WEEKLY OPTIONS TIMING
    The TOP returns assume Monday-open entry with weekly options.
    The system scans at 9:35 AM and 3:15 PM — good for same-day entries
    but the "best day" for many movers was Tue-Thu.
    Need: Multi-day hold awareness for weekly options plays.
""")
