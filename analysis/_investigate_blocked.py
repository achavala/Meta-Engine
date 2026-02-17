#!/usr/bin/env python3
"""Investigate why specific high-value picks were blocked."""

import json
from pathlib import Path

OUTPUT = Path("/Users/chavala/Meta Engine/output")
TN_DATA = Path("/Users/chavala/TradeNova/data")

def main():
    # 1. Why was Feb 12 PM moonshot pool empty?
    with open(TN_DATA / "trinity_interval_scans.json") as f:
        trinity = json.load(f)

    feb12 = trinity.get("2026-02-12", {})
    scans = feb12.get("scans", [])
    print("=== Feb 12 scans (check PM coverage) ===")
    for i, s in enumerate(scans):
        st = s.get("scan_time", "")
        label = s.get("scan_label", "")
        n_moon = len(s.get("moonshot", []))
        n_cat = len(s.get("catalyst", []))
        n_coil = len(s.get("coiled_spring", []))
        n_top = len(s.get("top_10", []))
        print(f"  Scan {i}: {st[:19]} ({label}) moon={n_moon} cat={n_cat} coil={n_coil} top10={n_top}")

    # 2. Check Feb 12 PM data from backtest
    with open(OUTPUT / "backtest_newcode_v2_feb9_13.json") as f:
        bt = json.load(f)
    
    for scan_time_code in ["0935", "1515"]:
        for eng in ["MOONSHOT", "PUTS"]:
            recs = [r for r in bt["results"] 
                    if r["scan_date"] == "2026-02-12" 
                    and r["scan_time"] == scan_time_code 
                    and r["engine"] == eng]
            ok_recs = [r for r in recs if r.get("data_quality") == "OK"]
            print(f"\n  Feb 12 {scan_time_code} {eng}: {len(recs)} total, {len(ok_recs)} OK")

    # 3. Check UW flow for blocked winners
    with open(TN_DATA / "uw_flow_cache.json") as f:
        uw = json.load(f)
    flow_data = uw.get("flow_data", uw)
    
    print("\n\n=== UW FLOW FOR BLOCKED PUTS WINNERS ===")
    syms_to_check = ["APP", "CLS", "CHWY", "TCOM", "TEM", "AMAT", "SCHW", "APLD", "MOH"]
    
    for sym in syms_to_check:
        flow = flow_data.get(sym, [])
        if flow and isinstance(flow, list):
            call_prem = sum(
                t.get("premium", 0) 
                for t in flow 
                if isinstance(t, dict) and t.get("put_call") == "C"
            )
            put_prem = sum(
                t.get("premium", 0) 
                for t in flow 
                if isinstance(t, dict) and t.get("put_call") == "P"
            )
            total = call_prem + put_prem
            call_pct = call_prem / total if total > 0 else 0.5
            blocked = "BLOCKED" if call_pct > 0.55 else "PASS"
            print(f"  {sym:7s}: call${call_prem:>10,.0f}  put${put_prem:>10,.0f}  "
                  f"call_pct={call_pct:.1%}  → {blocked}")
        else:
            print(f"  {sym:7s}: NO UW FLOW DATA → default 50% → PASS (not blocked)")
    
    # 4. Check what CONVICTION scores the blocked puts had
    print("\n\n=== BLOCKED PUTS — WHY CONVICTION TOO LOW? ===")
    
    # Simulate conviction for blocked puts
    ok_puts = [r for r in bt["results"] 
               if r["engine"] == "PUTS" and r.get("data_quality") == "OK"]
    
    # Feb 12 AM puts — only 1 was picked (UPST), APP was blocked
    feb12_am_puts = [r for r in ok_puts if r["scan_date"] == "2026-02-12" and r["scan_time"] == "0935"]
    print(f"\n  Feb 12 AM PUTS ({len(feb12_am_puts)} candidates):")
    for r in sorted(feb12_am_puts, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
        sym = r["symbol"]
        flow = flow_data.get(sym, [])
        if flow and isinstance(flow, list):
            cp = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "C")
            pp = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "P")
            tot = cp + pp
            cpct = cp / tot if tot > 0 else 0.5
        else:
            cpct = 0.5
        
        blocked_by_cpct = cpct > 0.55
        opt_pnl = r.get("options_pnl_pct", 0)
        icon = "🏆" if opt_pnl >= 20 else ("✅" if opt_pnl >= 10 else ("🟡" if opt_pnl > 0 else "❌"))
        gate = "BLOCKED(call_pct)" if blocked_by_cpct else "PASS"
        
        print(f"    {icon} {sym:7s} opt={opt_pnl:+6.1f}% stock={r.get('stock_move_pct',0):+5.1f}% "
              f"score={r.get('score',0):.2f} sig={r.get('signal_count',0)} "
              f"call_pct={cpct:.1%} → {gate}")
    
    # Feb 11 PM puts — AFRM and MRVL picked, CHWY/TCOM blocked
    feb11_pm_puts = [r for r in ok_puts if r["scan_date"] == "2026-02-11" and r["scan_time"] == "1515"]
    print(f"\n  Feb 11 PM PUTS ({len(feb11_pm_puts)} candidates):")
    for r in sorted(feb11_pm_puts, key=lambda x: x.get("options_pnl_pct", 0), reverse=True):
        sym = r["symbol"]
        flow = flow_data.get(sym, [])
        if flow and isinstance(flow, list):
            cp = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "C")
            pp = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "P")
            tot = cp + pp
            cpct = cp / tot if tot > 0 else 0.5
        else:
            cpct = 0.5
        
        blocked_by_cpct = cpct > 0.55
        opt_pnl = r.get("options_pnl_pct", 0)
        icon = "🏆" if opt_pnl >= 20 else ("✅" if opt_pnl >= 10 else ("🟡" if opt_pnl > 0 else "❌"))
        gate = "BLOCKED(call_pct)" if blocked_by_cpct else "PASS"
        
        print(f"    {icon} {sym:7s} opt={opt_pnl:+6.1f}% stock={r.get('stock_move_pct',0):+5.1f}% "
              f"score={r.get('score',0):.2f} sig={r.get('signal_count',0)} "
              f"call_pct={cpct:.1%} → {gate}")


if __name__ == "__main__":
    main()
