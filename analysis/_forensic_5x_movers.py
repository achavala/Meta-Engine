#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════════════════
  INSTITUTIONAL FORENSIC ANALYSIS: ≥5x MOVERS (Feb 9-13, 2026)
══════════════════════════════════════════════════════════════════════════════
  
  Goal: Trace every ≥5x mover through the entire signal pipeline to answer:
    1. What signals existed 1-2 days BEFORE the big move?
    2. Why did Trinity detect them but Final Rec missed ALL of them?
    3. What changes would have caught them?
    4. Design the system to never miss these again.

  Lens: 30+ yrs trading + PhD quant + institutional microstructure
══════════════════════════════════════════════════════════════════════════════
"""

import json
import statistics
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional

ROOT = Path("/Users/chavala/Meta Engine")
OUTPUT = ROOT / "output"
TN_DATA = Path("/Users/chavala/TradeNova/data")

# ══════════════════════════════════════════════════════════════════════════════
# ALL ≥5x MOVERS FROM USER'S TABLE
# ══════════════════════════════════════════════════════════════════════════════

CALL_MOVERS = [
    {"sym": "SHOP", "sector": "Software/Cloud", "entry": 112.11, "high": 145.16, "max_up": 29.5, "mult": 18.2, "best_day": "2026-02-11", "trinity_days": ["Tue","Wed","Thu","Fri"]},
    {"sym": "RIVN", "sector": "EV/Auto", "entry": 14.92, "high": 18.48, "max_up": 23.9, "mult": 15.3, "best_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "CIFR", "sector": "Crypto", "entry": 14.70, "high": 17.96, "max_up": 22.2, "mult": 14.3, "best_day": "2026-02-10", "trinity_days": ["Mon","Tue"]},
    {"sym": "WULF", "sector": "Crypto", "entry": 14.58, "high": 17.70, "max_up": 21.4, "mult": 13.8, "best_day": "2026-02-10", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "DDOG", "sector": "Software/Cloud", "entry": 113.00, "high": 137.00, "max_up": 21.2, "mult": 13.7, "best_day": "2026-02-10", "trinity_days": ["Tue","Wed","Thu","Fri"]},
    {"sym": "NET", "sector": "Software/Cloud", "entry": 173.22, "high": 209.75, "max_up": 21.1, "mult": 13.7, "best_day": "2026-02-11", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "IBRX", "sector": "Biotech", "entry": 6.08, "high": 7.30, "max_up": 20.1, "mult": 13.0, "best_day": "2026-02-13", "trinity_days": []},
    {"sym": "VKTX", "sector": "Biotech", "entry": 28.66, "high": 34.38, "max_up": 20.0, "mult": 13.0, "best_day": "2026-02-12", "trinity_days": []},
    {"sym": "U", "sector": "Streaming/Social", "entry": 25.36, "high": 30.36, "max_up": 19.7, "mult": 12.8, "best_day": "2026-02-11", "trinity_days": ["Mon","Tue"]},
    {"sym": "AMAT", "sector": "Semis", "entry": 322.52, "high": 376.32, "max_up": 16.7, "mult": 11.0, "best_day": "2026-02-13", "trinity_days": ["Tue","Wed","Thu","Fri"]},
    {"sym": "UPST", "sector": "Fintech", "entry": 38.40, "high": 44.55, "max_up": 16.0, "mult": 10.6, "best_day": "2026-02-10", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "APP", "sector": "AI/Tech", "entry": 423.99, "high": 487.29, "max_up": 14.9, "mult": 9.9, "best_day": "2026-02-09", "trinity_days": []},
    {"sym": "VST", "sector": "Energy/Nuclear", "entry": 150.50, "high": 171.66, "max_up": 14.1, "mult": 9.2, "best_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "RBLX", "sector": "Streaming/Social", "entry": 67.52, "high": 76.58, "max_up": 13.4, "mult": 8.8, "best_day": "2026-02-10", "trinity_days": ["Mon"]},
    {"sym": "ROKU", "sector": "Streaming/Social", "entry": 86.00, "high": 97.50, "max_up": 13.4, "mult": 8.8, "best_day": "2026-02-13", "trinity_days": ["Mon","Fri"]},
    {"sym": "BYND", "sector": "Consumer/Spec", "entry": 0.72, "high": 0.82, "max_up": 13.1, "mult": 8.6, "best_day": "2026-02-10", "trinity_days": []},
    {"sym": "LUNR", "sector": "Space/Defense", "entry": 18.02, "high": 20.31, "max_up": 12.7, "mult": 8.3, "best_day": "2026-02-09", "trinity_days": ["Mon"]},
    {"sym": "NVAX", "sector": "Biotech", "entry": 8.27, "high": 9.28, "max_up": 12.2, "mult": 7.9, "best_day": "2026-02-11", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "MU", "sector": "Semis", "entry": 391.57, "high": 438.77, "max_up": 12.1, "mult": 7.8, "best_day": "2026-02-12", "trinity_days": ["Wed","Thu","Fri"]},
    {"sym": "MDB", "sector": "Software/Cloud", "entry": 347.05, "high": 388.00, "max_up": 11.8, "mult": 7.7, "best_day": "2026-02-10", "trinity_days": ["Tue","Wed","Thu","Fri"]},
    {"sym": "CEG", "sector": "Energy/Nuclear", "entry": 264.00, "high": 294.36, "max_up": 11.5, "mult": 7.5, "best_day": "2026-02-13", "trinity_days": ["Tue","Wed","Thu","Fri"]},
    {"sym": "SNOW", "sector": "Software/Cloud", "entry": 169.80, "high": 188.70, "max_up": 11.1, "mult": 7.2, "best_day": "2026-02-11", "trinity_days": ["Wed","Thu","Fri"]},
    {"sym": "MRNA", "sector": "Biotech", "entry": 41.03, "high": 45.50, "max_up": 10.9, "mult": 7.0, "best_day": "2026-02-10", "trinity_days": ["Wed"]},
    {"sym": "TLN", "sector": "Energy/Nuclear", "entry": 343.72, "high": 381.00, "max_up": 10.8, "mult": 7.0, "best_day": "2026-02-13", "trinity_days": ["Thu","Fri"]},
    {"sym": "CRWV", "sector": "Space/Defense", "entry": 90.89, "high": 100.69, "max_up": 10.8, "mult": 6.9, "best_day": "2026-02-13", "trinity_days": ["Wed","Thu","Fri"]},
    {"sym": "BITF", "sector": "Crypto", "entry": 2.16, "high": 2.38, "max_up": 10.2, "mult": 6.5, "best_day": "2026-02-10", "trinity_days": []},
    {"sym": "SAVA", "sector": "Biotech", "entry": 2.02, "high": 2.22, "max_up": 9.9, "mult": 6.3, "best_day": "2026-02-10", "trinity_days": []},
    {"sym": "TSM", "sector": "Semis", "entry": 350.00, "high": 382.39, "max_up": 9.3, "mult": 5.9, "best_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "CRSP", "sector": "Biotech", "entry": 49.52, "high": 54.10, "max_up": 9.2, "mult": 5.9, "best_day": "2026-02-13", "trinity_days": ["Fri"]},
    {"sym": "AFRM", "sector": "Fintech", "entry": 57.15, "high": 62.14, "max_up": 8.7, "mult": 5.5, "best_day": "2026-02-10", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "CRWD", "sector": "Software/Cloud", "entry": 398.99, "high": 432.85, "max_up": 8.5, "mult": 5.3, "best_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
]

PUT_MOVERS = [
    {"sym": "U", "sector": "Streaming/Social", "entry": 25.36, "low": 18.54, "max_down": -26.9, "mult": 16.9, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue"]},
    {"sym": "UPST", "sector": "Fintech", "entry": 38.40, "low": 29.29, "max_down": -23.7, "mult": 15.2, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "HIMS", "sector": "Biotech", "entry": 32.95, "low": 26.38, "max_down": -19.9, "mult": 12.9, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "DKNG", "sector": "Consumer/Spec", "entry": 26.56, "low": 21.01, "max_down": -20.9, "mult": 13.5, "worst_day": "2026-02-13", "trinity_days": []},
    {"sym": "ASTS", "sector": "Space/Defense", "entry": 101.64, "low": 79.89, "max_down": -21.4, "mult": 13.9, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "COIN", "sector": "Crypto", "entry": 161.38, "low": 130.50, "max_down": -19.1, "mult": 12.5, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "CVNA", "sector": "Consumer/Spec", "entry": 402.40, "low": 328.88, "max_down": -18.3, "mult": 11.8, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "AMC", "sector": "Meme", "entry": 1.46, "low": 1.21, "max_down": -17.1, "mult": 11.3, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "FUBO", "sector": "Streaming/Social", "entry": 1.60, "low": 1.31, "max_down": -18.1, "mult": 11.9, "worst_day": "2026-02-12", "trinity_days": ["Tue","Wed"]},
    {"sym": "HOOD", "sector": "Fintech", "entry": 85.15, "low": 70.25, "max_down": -17.5, "mult": 11.5, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Fri"]},
    {"sym": "RGTI", "sector": "Quantum", "entry": 17.71, "low": 14.74, "max_down": -16.8, "mult": 11.1, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "APP", "sector": "AI/Tech", "entry": 423.99, "low": 356.00, "max_down": -16.0, "mult": 10.6, "worst_day": "2026-02-13", "trinity_days": []},
    {"sym": "LUNR", "sector": "Space/Defense", "entry": 18.02, "low": 15.20, "max_down": -15.6, "mult": 10.4, "worst_day": "2026-02-12", "trinity_days": ["Mon"]},
    {"sym": "DNA", "sector": "Biotech", "entry": 10.25, "low": 8.65, "max_down": -15.6, "mult": 10.4, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "INOD", "sector": "Industrials", "entry": 48.98, "low": 41.34, "max_down": -15.6, "mult": 10.4, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "AFRM", "sector": "Fintech", "entry": 57.15, "low": 48.55, "max_down": -15.0, "mult": 10.0, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "ENPH", "sector": "Energy/Nuclear", "entry": 50.32, "low": 43.00, "max_down": -14.5, "mult": 9.6, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "RKLB", "sector": "Space/Defense", "entry": 74.00, "low": 63.87, "max_down": -13.7, "mult": 9.0, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "PLUG", "sector": "Industrials", "entry": 2.09, "low": 1.81, "max_down": -13.4, "mult": 8.8, "worst_day": "2026-02-12", "trinity_days": ["Mon","Wed"]},
    {"sym": "MARA", "sector": "Crypto", "entry": 8.24, "low": 7.14, "max_down": -13.3, "mult": 8.7, "worst_day": "2026-02-12", "trinity_days": ["Thu","Fri"]},
    {"sym": "IONQ", "sector": "Quantum", "entry": 35.50, "low": 30.85, "max_down": -13.1, "mult": 8.6, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "TDOC", "sector": "Consumer/Spec", "entry": 5.06, "low": 4.40, "max_down": -13.0, "mult": 8.5, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue"]},
    {"sym": "OKLO", "sector": "Energy/Nuclear", "entry": 71.75, "low": 62.56, "max_down": -12.8, "mult": 8.4, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue"]},
    {"sym": "SOFI", "sector": "Fintech", "entry": 21.70, "low": 19.01, "max_down": -12.4, "mult": 8.1, "worst_day": "2026-02-13", "trinity_days": ["Wed","Thu","Fri"]},
    {"sym": "QBTS", "sector": "Quantum", "entry": 20.99, "low": 18.45, "max_down": -12.1, "mult": 7.9, "worst_day": "2026-02-12", "trinity_days": ["Mon"]},
    {"sym": "SMCI", "sector": "Semis", "entry": 34.29, "low": 30.22, "max_down": -11.9, "mult": 7.7, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "RDDT", "sector": "Meme", "entry": 143.92, "low": 127.70, "max_down": -11.3, "mult": 7.3, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "INTC", "sector": "Semis", "entry": 50.65, "low": 44.97, "max_down": -11.2, "mult": 7.2, "worst_day": "2026-02-13", "trinity_days": []},
    {"sym": "SNAP", "sector": "Streaming/Social", "entry": 5.31, "low": 4.72, "max_down": -11.1, "mult": 7.2, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue"]},
    {"sym": "MRNA", "sector": "Biotech", "entry": 41.03, "low": 36.66, "max_down": -10.7, "mult": 6.9, "worst_day": "2026-02-11", "trinity_days": ["Wed"]},
    {"sym": "PTON", "sector": "Consumer/Spec", "entry": 4.62, "low": 4.13, "max_down": -10.6, "mult": 6.8, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "SPCE", "sector": "Space/Defense", "entry": 2.74, "low": 2.45, "max_down": -10.6, "mult": 6.8, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "NTLA", "sector": "Biotech", "entry": 12.49, "low": 11.21, "max_down": -10.2, "mult": 6.6, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "BIDU", "sector": "China ADR", "entry": 145.45, "low": 131.50, "max_down": -9.6, "mult": 6.1, "worst_day": "2026-02-13", "trinity_days": []},
    {"sym": "CLSK", "sector": "Crypto", "entry": 10.14, "low": 9.17, "max_down": -9.6, "mult": 6.1, "worst_day": "2026-02-12", "trinity_days": []},
    {"sym": "LCID", "sector": "EV/Auto", "entry": 10.82, "low": 9.80, "max_down": -9.4, "mult": 6.0, "worst_day": "2026-02-12", "trinity_days": ["Mon","Wed"]},
    {"sym": "MSTR", "sector": "Crypto", "entry": 132.87, "low": 120.64, "max_down": -9.2, "mult": 5.8, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "PLTR", "sector": "AI/Tech", "entry": 138.67, "low": 126.23, "max_down": -9.0, "mult": 5.7, "worst_day": "2026-02-13", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "RBLX", "sector": "Streaming/Social", "entry": 67.52, "low": 61.60, "max_down": -8.8, "mult": 5.5, "worst_day": "2026-02-12", "trinity_days": ["Mon"]},
    {"sym": "ROKU", "sector": "Streaming/Social", "entry": 86.00, "low": 78.53, "max_down": -8.7, "mult": 5.5, "worst_day": "2026-02-12", "trinity_days": ["Mon","Fri"]},
    {"sym": "NFLX", "sector": "Mega/Tech", "entry": 82.07, "low": 75.23, "max_down": -8.3, "mult": 5.2, "worst_day": "2026-02-12", "trinity_days": ["Mon","Tue","Wed","Thu","Fri"]},
    {"sym": "BABA", "sector": "China ADR", "entry": 162.32, "low": 149.00, "max_down": -8.2, "mult": 5.1, "worst_day": "2026-02-13", "trinity_days": ["Wed","Thu","Fri"]},
]


def load_trinity():
    with open(TN_DATA / "trinity_interval_scans.json") as f:
        return json.load(f)


def load_forecasts():
    with open(TN_DATA / "tomorrows_forecast.json") as f:
        data = json.load(f)
    return {fc["symbol"]: fc for fc in data.get("forecasts", []) if fc.get("symbol")}


def load_uw_flow():
    with open(TN_DATA / "uw_flow_cache.json") as f:
        raw = json.load(f)
    if "flow_data" in raw and isinstance(raw["flow_data"], dict):
        return raw["flow_data"]
    return raw


def load_cross_analyses():
    results = []
    for cf in sorted(OUTPUT.glob("cross_analysis_*.json")):
        if "latest" in cf.name:
            continue
        with open(cf) as f:
            data = json.load(f)
        results.append(data)
    return results


def load_final_recs():
    path = TN_DATA / "final_recommendations.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def load_final_recs_history():
    path = TN_DATA / "final_recommendations_history.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


def load_eod_picks():
    path = TN_DATA / "eod_interval_picks.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def load_darkpool():
    path = TN_DATA / "darkpool_cache.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def trace_symbol_through_pipeline(sym: str, trinity: Dict, forecasts: Dict,
                                   uw_flow: Dict, cross_analyses: List,
                                   eod_picks: Dict) -> Dict:
    """Trace a single symbol through EVERY stage of the pipeline."""
    result = {
        "symbol": sym,
        "trinity_appearances": [],
        "forecast_data": None,
        "uw_flow_data": None,
        "cross_analysis_appearances": [],
        "eod_appearances": [],
        "pipeline_drop_point": None,
    }

    # 1) Trinity appearances
    for d in ["2026-02-09", "2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13"]:
        day_data = trinity.get(d, {})
        scans = day_data.get("scans", [])
        for s in scans:
            ts = s.get("scan_time_str", "")
            for eng in ["moonshot", "catalyst", "coiled_spring", "top_10"]:
                picks = s.get(eng, [])
                for p in picks:
                    if p.get("symbol") == sym:
                        result["trinity_appearances"].append({
                            "date": d,
                            "time": ts,
                            "engine": eng,
                            "score": p.get("score", 0),
                            "signals": str(p.get("signals", ""))[:120],
                            "action": p.get("action", ""),
                            "win_probability": p.get("win_probability", 0),
                            "entry_price": p.get("entry_price", 0),
                            "expected": p.get("expected", ""),
                        })

    # 2) Forecast data
    if sym in forecasts:
        fc = forecasts[sym]
        result["forecast_data"] = {
            "bullish_prob": fc.get("bullish_probability", 0),
            "bearish_prob": fc.get("bearish_probability", 0),
            "catalysts": fc.get("catalysts", []),
            "direction": fc.get("direction", ""),
            "confidence": fc.get("confidence", 0),
        }

    # 3) UW flow data
    if sym in uw_flow:
        flow = uw_flow[sym]
        if isinstance(flow, list) and flow:
            call_prem = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "C")
            put_prem = sum(t.get("premium", 0) for t in flow if isinstance(t, dict) and t.get("put_call") == "P")
            total = call_prem + put_prem
            result["uw_flow_data"] = {
                "num_trades": len(flow),
                "call_premium": call_prem,
                "put_premium": put_prem,
                "call_pct": round(call_prem / total, 3) if total > 0 else 0.5,
                "total_premium": total,
            }

    # 4) Cross analysis appearances
    for ca in cross_analyses:
        ts = ca.get("timestamp", "")[:10]
        for key in ["moonshot_through_puts", "puts_through_moonshot"]:
            for pick in ca.get(key, []):
                if pick.get("symbol") == sym:
                    result["cross_analysis_appearances"].append({
                        "date": ts,
                        "list": key,
                        "rank": ca.get(key, []).index(pick) + 1,
                        "score": pick.get("score", 0),
                        "engine": pick.get("engine", ""),
                    })

    # 5) EOD picks
    if isinstance(eod_picks, dict):
        for d, day_data in eod_picks.items():
            if isinstance(day_data, dict):
                for pick in day_data.get("picks", []):
                    if pick.get("symbol") == sym:
                        result["eod_appearances"].append({
                            "date": d,
                            "score": pick.get("score", 0),
                            "engine": pick.get("engine", ""),
                        })

    # Determine drop point
    if not result["trinity_appearances"]:
        result["pipeline_drop_point"] = "NEVER_IN_TRINITY"
    elif not result["cross_analysis_appearances"]:
        result["pipeline_drop_point"] = "DROPPED_AT_CROSS_ANALYSIS"
    else:
        result["pipeline_drop_point"] = "DROPPED_AT_FINAL_REC"

    return result


def main():
    print("=" * 120)
    print("  INSTITUTIONAL FORENSIC ANALYSIS: ≥5x MOVERS (Feb 9-13, 2026)")
    print("  Goal: Why did the system miss ALL 73 ≥5x movers?")
    print("=" * 120)

    # Load all data
    trinity = load_trinity()
    forecasts = load_forecasts()
    uw_flow = load_uw_flow()
    cross_analyses = load_cross_analyses()
    eod_picks = load_eod_picks()

    print(f"\n  Data loaded:")
    print(f"    Trinity: {sum(len(trinity.get(d, {}).get('scans', [])) for d in ['2026-02-09','2026-02-10','2026-02-11','2026-02-12','2026-02-13'])} scans")
    print(f"    Forecasts: {len(forecasts)} symbols")
    print(f"    UW Flow: {len(uw_flow)} symbols")
    print(f"    Cross Analyses: {len(cross_analyses)} files")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 1: CALL MOVERS — FULL PIPELINE TRACE
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 1: ≥5x CALL MOVERS (31 tickers) — Full Pipeline Trace")
    print(f"{'═'*120}")

    call_traces = []
    for mover in CALL_MOVERS:
        trace = trace_symbol_through_pipeline(
            mover["sym"], trinity, forecasts, uw_flow, cross_analyses, eod_picks
        )
        trace["mover_data"] = mover
        call_traces.append(trace)

    # Summary table
    print(f"\n  {'Sym':<7s} {'Sector':<16s} {'MaxUp':>6s} {'~Mult':>6s} {'BestDay':<12s} "
          f"{'Trinity#':>8s} {'CrossAn#':>8s} {'Forecast':>8s} {'UW_Flow':>8s} {'DROP POINT':<30s}")
    print(f"  {'─'*115}")

    for t in call_traces:
        m = t["mover_data"]
        print(f"  {m['sym']:<7s} {m['sector']:<16s} +{m['max_up']:>5.1f}% {m['mult']:>5.1f}x {m['best_day']:<12s} "
              f"{len(t['trinity_appearances']):>8d} {len(t['cross_analysis_appearances']):>8d} "
              f"{'✅' if t['forecast_data'] else '❌':>8s} "
              f"{'✅' if t['uw_flow_data'] else '❌':>8s} "
              f"{t['pipeline_drop_point']:<30s}")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 2: PUT MOVERS — FULL PIPELINE TRACE
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 2: ≥5x PUT MOVERS (42 tickers) — Full Pipeline Trace")
    print(f"{'═'*120}")

    put_traces = []
    for mover in PUT_MOVERS:
        trace = trace_symbol_through_pipeline(
            mover["sym"], trinity, forecasts, uw_flow, cross_analyses, eod_picks
        )
        trace["mover_data"] = mover
        put_traces.append(trace)

    print(f"\n  {'Sym':<7s} {'Sector':<16s} {'MaxDn':>6s} {'~Mult':>6s} {'WorstDay':<12s} "
          f"{'Trinity#':>8s} {'CrossAn#':>8s} {'Forecast':>8s} {'UW_Flow':>8s} {'DROP POINT':<30s}")
    print(f"  {'─'*115}")

    for t in put_traces:
        m = t["mover_data"]
        print(f"  {m['sym']:<7s} {m['sector']:<16s} {m['max_down']:>+5.1f}% {m['mult']:>5.1f}x {m['worst_day']:<12s} "
              f"{len(t['trinity_appearances']):>8d} {len(t['cross_analysis_appearances']):>8d} "
              f"{'✅' if t['forecast_data'] else '❌':>8s} "
              f"{'✅' if t['uw_flow_data'] else '❌':>8s} "
              f"{t['pipeline_drop_point']:<30s}")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 3: DROP POINT ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    all_traces = call_traces + put_traces
    print(f"\n{'═'*120}")
    print(f"  SECTION 3: DROP POINT ANALYSIS — Where did movers fall out?")
    print(f"{'═'*120}")

    drop_counts = defaultdict(int)
    for t in all_traces:
        drop_counts[t["pipeline_drop_point"]] += 1

    total = len(all_traces)
    for dp, cnt in sorted(drop_counts.items(), key=lambda x: -x[1]):
        pct = cnt / total * 100
        print(f"  {dp:<35s}: {cnt:>3d}/{total} ({pct:.1f}%)")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 4: WHAT SIGNALS EXISTED 1-2 DAYS BEFORE THE BIG MOVE?
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 4: EARLY SIGNALS — What existed 1-2 days BEFORE the big move?")
    print(f"{'═'*120}")

    DATE_MAP = {
        "2026-02-09": "Mon",
        "2026-02-10": "Tue",
        "2026-02-11": "Wed",
        "2026-02-12": "Thu",
        "2026-02-13": "Fri",
    }
    
    early_signal_count = 0
    no_early_signal = 0

    for t in sorted(all_traces, key=lambda x: -abs(x["mover_data"].get("max_up", 0) or x["mover_data"].get("max_down", 0))):
        m = t["mover_data"]
        sym = m["sym"]
        best_day = m.get("best_day", m.get("worst_day", ""))
        is_call = "max_up" in m
        move = m.get("max_up", 0) or abs(m.get("max_down", 0))
        direction = "CALL" if is_call else "PUT"

        # Find early signals (before best_day)
        early_signals = []
        for app in t["trinity_appearances"]:
            if app["date"] < best_day:
                early_signals.append(app)

        if early_signals:
            early_signal_count += 1
        else:
            no_early_signal += 1

        if early_signals:
            # Show only top 10 most interesting
            if move >= 15 or len(early_signals) >= 3:
                print(f"\n  {'🔥' if move >= 20 else '⚡'} {sym} ({direction}) — Move: {'+' if is_call else ''}{m.get('max_up', m.get('max_down', 0))}% (~{m['mult']}x) — Best: {best_day}")
                print(f"     Early signals ({len(early_signals)} appearances before move day):")
                for es in early_signals[:6]:
                    day_name = DATE_MAP.get(es["date"], es["date"])
                    print(f"       {day_name} {es['time']:12s} | {es['engine']:12s} | Score={es['score']:.3f} | "
                          f"WP={es['win_probability']:.0%} | {es['action']:5s}")
                    if es["signals"]:
                        print(f"         Signals: {es['signals'][:100]}")
                
                # Show forecast if available
                if t["forecast_data"]:
                    fc = t["forecast_data"]
                    bp = fc['bullish_prob']
                    bp_s = f"{bp:.0%}" if isinstance(bp, (int, float)) else str(bp)
                    bep = fc['bearish_prob']
                    bep_s = f"{bep:.0%}" if isinstance(bep, (int, float)) else str(bep)
                    conf = fc['confidence']
                    conf_s = f"{conf:.0%}" if isinstance(conf, (int, float)) else str(conf)
                    print(f"     📊 Forecast: Bull={bp_s} Bear={bep_s} Dir={fc['direction']} Conf={conf_s}")
                    if fc["catalysts"]:
                        cat_str = ", ".join(str(c) for c in fc["catalysts"][:3])
                        print(f"         Catalysts: {cat_str}")
                
                # Show UW flow if available
                if t["uw_flow_data"]:
                    uf = t["uw_flow_data"]
                    print(f"     📈 UW Flow: {uf['num_trades']} trades | Call={uf['call_pct']:.0%} Put={1-uf['call_pct']:.0%} | Total=${uf['total_premium']:,.0f}")

    print(f"\n  Summary: {early_signal_count}/{len(all_traces)} movers had early signals ({early_signal_count/len(all_traces)*100:.0f}%)")
    print(f"           {no_early_signal}/{len(all_traces)} movers had NO early signals ({no_early_signal/len(all_traces)*100:.0f}%)")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 5: CROSS ANALYSIS — What was picked INSTEAD?
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 5: What was recommended INSTEAD of these movers?")
    print(f"{'═'*120}")

    mover_syms = set(m["sym"] for m in CALL_MOVERS + PUT_MOVERS)

    for ca in cross_analyses:
        ts = ca.get("timestamp", "")[:16]
        print(f"\n  📅 Cross Analysis: {ts}")

        for key in ["moonshot_through_puts", "puts_through_moonshot"]:
            picks = ca.get(key, [])
            direction = "CALL" if "moonshot" in key.split("_")[0] else "PUT"
            print(f"    {key} ({direction}):")
            for i, p in enumerate(picks, 1):
                sym = p.get("symbol", "?")
                score = p.get("score", 0)
                is_mover = "🔥" if sym in mover_syms else "  "
                print(f"      {is_mover} #{i:2d} {sym:<7s} Score={score:.3f} | {p.get('engine', '')}")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 6: SIGNAL FINGERPRINTING — What did winners have in common?
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 6: SIGNAL FINGERPRINTING — Common patterns in ≥5x movers")
    print(f"{'═'*120}")

    # Count sectors
    call_sectors = defaultdict(int)
    put_sectors = defaultdict(int)
    for m in CALL_MOVERS:
        call_sectors[m["sector"]] += 1
    for m in PUT_MOVERS:
        put_sectors[m["sector"]] += 1

    print(f"\n  CALL MOVERS by Sector:")
    for sec, cnt in sorted(call_sectors.items(), key=lambda x: -x[1]):
        print(f"    {sec:<20s}: {cnt}")

    print(f"\n  PUT MOVERS by Sector:")
    for sec, cnt in sorted(put_sectors.items(), key=lambda x: -x[1]):
        print(f"    {sec:<20s}: {cnt}")

    # Signal analysis
    signal_keywords = defaultdict(int)
    score_dist = []
    wp_dist = []
    
    for t in all_traces:
        for app in t["trinity_appearances"]:
            score_dist.append(app["score"])
            wp_dist.append(app["win_probability"])
            sig_str = app["signals"].lower()
            for kw in ["support", "breakout", "momentum", "oversold", "compression",
                       "heavy call", "positive gex", "dark pool", "accumulation",
                       "iv_inverted", "neg_gex", "sector", "sympathy", "recurrence",
                       "bounce", "gap", "volume", "earnings"]:
                if kw in sig_str:
                    signal_keywords[kw] += 1

    if signal_keywords:
        print(f"\n  Top Signal Keywords in ≥5x Movers:")
        for kw, cnt in sorted(signal_keywords.items(), key=lambda x: -x[1])[:15]:
            print(f"    {kw:<25s}: {cnt}")

    if score_dist:
        print(f"\n  Score Distribution (Trinity appearances):")
        print(f"    Mean:   {statistics.mean(score_dist):.3f}")
        print(f"    Median: {statistics.median(score_dist):.3f}")
        print(f"    Min:    {min(score_dist):.3f}")
        print(f"    Max:    {max(score_dist):.3f}")

    if wp_dist:
        wp_nonzero = [w for w in wp_dist if w > 0]
        if wp_nonzero:
            print(f"\n  Win Probability Distribution:")
            print(f"    Mean:   {statistics.mean(wp_nonzero):.1%}")
            print(f"    Median: {statistics.median(wp_nonzero):.1%}")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 7: CRITICAL FINDINGS & ROOT CAUSE
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 7: CRITICAL FINDINGS & ROOT CAUSE ANALYSIS")
    print(f"{'═'*120}")

    # Count movers that were in Trinity but not in cross analysis
    in_trinity = [t for t in all_traces if t["trinity_appearances"]]
    in_cross = [t for t in all_traces if t["cross_analysis_appearances"]]
    has_forecast = [t for t in all_traces if t["forecast_data"]]
    has_flow = [t for t in all_traces if t["uw_flow_data"]]
    
    print(f"\n  Pipeline Funnel:")
    print(f"    Total ≥5x movers:                  {len(all_traces)}")
    print(f"    Had Trinity signals:                {len(in_trinity)} ({len(in_trinity)/len(all_traces)*100:.0f}%)")
    print(f"    Had Forecast data:                  {len(has_forecast)} ({len(has_forecast)/len(all_traces)*100:.0f}%)")
    print(f"    Had UW Flow data:                   {len(has_flow)} ({len(has_flow)/len(all_traces)*100:.0f}%)")
    print(f"    Made Cross Analysis Top 10:         {len(in_cross)} ({len(in_cross)/len(all_traces)*100:.0f}%)")
    print(f"    Made Final Recommendation:          0 (0%)")

    # Analyze what cross analysis picked instead
    ca_picked_syms = set()
    ca_mover_syms = set()
    for ca in cross_analyses:
        for key in ["moonshot_through_puts", "puts_through_moonshot"]:
            for p in ca.get(key, []):
                sym = p.get("symbol", "")
                ca_picked_syms.add(sym)
                if sym in mover_syms:
                    ca_mover_syms.add(sym)

    print(f"\n  Cross Analysis picked {len(ca_picked_syms)} unique symbols total")
    print(f"  Of those, {len(ca_mover_syms)} were ≥5x movers: {sorted(ca_mover_syms)}")
    print(f"  ≥5x movers NOT in any cross analysis: {len(mover_syms - ca_mover_syms)}")
    missed_movers = sorted(mover_syms - ca_mover_syms)
    print(f"    {missed_movers[:20]}")
    if len(missed_movers) > 20:
        print(f"    ... and {len(missed_movers) - 20} more")

    # What cross analysis picked that were NOT movers
    non_movers_picked = ca_picked_syms - mover_syms
    print(f"\n  Non-mover symbols in Cross Analysis: {len(non_movers_picked)}")
    print(f"    {sorted(non_movers_picked)[:15]}")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 8: ACTIONABLE RECOMMENDATIONS
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'═'*120}")
    print(f"  SECTION 8: ACTIONABLE RECOMMENDATIONS")
    print(f"{'═'*120}")

    print("""
  ROOT CAUSES (ordered by impact):

  1. 🔴 MEGA-CAP BIAS IN CROSS ANALYSIS: The cross_analysis picks UNH, AMZN, 
     GOOGL, NVDA, META — large-cap "safe" names that don't move 5x. The volatile
     mid/small-caps (RIVN, CIFR, WULF, U, HIMS) that produce 5x returns are 
     systematically filtered out.

  2. 🔴 SCORE CEILING EFFECT: Trinity scores cluster at 0.80-1.00 for BOTH movers
     and non-movers. The scoring doesn't discriminate between a 5x mover and a 
     mega-cap that moves 2%.

  3. 🔴 NO VOLATILITY/BETA WEIGHTING: A 10% move in RIVN ($14.92) produces ~15x
     options return. The same score in NVDA ($800+) produces ~3x. The system 
     treats them equally.

  4. 🔴 NO SECTOR MOMENTUM DETECTION: When 5+ crypto/software stocks are all in 
     Trinity with bullish signals, that's a SECTOR WAVE. The system doesn't 
     amplify sector clustering.

  5. 🟡 PUTS ENGINE MISMATCH: The puts engine doesn't recognize "high-beta stocks
     in bear regimes" as prime short candidates. HIMS, ASTS, COIN, HOOD all had
     massive downside but weren't surfaced.

  6. 🟡 NO MULTI-DAY SIGNAL PERSISTENCE TRACKING: RIVN appeared Mon-Fri, UPST 
     Mon-Fri, NET Mon-Fri. Persistent signals = highest conviction. The system
     doesn't track this.

  7. 🟡 FORECAST NOT INTEGRATED INTO RANKING: Many movers had bullish forecasts
     with "Heavy call buying" catalysts but this wasn't weighted in final ranking.

  SYSTEM DESIGN CHANGES NEEDED:

  A) ADD VOLATILITY-WEIGHTED SCORING
     - Score × Implied_Vol × Beta = "Move-Adjusted Score"
     - RIVN (beta ~3.5, IV ~100%) should rank 10x higher than UNH (beta 0.6, IV 25%)

  B) ADD SECTOR CLUSTERING DETECTOR
     - When 3+ symbols in same sector appear in Trinity → boost all by +0.15
     - When 5+ appear → boost by +0.30 ("sector wave")

  C) ADD MULTI-DAY PERSISTENCE SCORE
     - Day 1 appearance: base score
     - Day 2+ appearance: +0.10 per day ("persistent signal")
     - 5-day persistence: +0.50 maximum boost

  D) ADD OPTIONS MULTIPLIER TO RANKING
     - Estimated options return = stock_move × leverage_factor
     - leverage_factor = f(IV, price, DTE, delta)
     - Rank by expected OPTIONS return, not stock return

  E) EXPAND UNIVERSE COVERAGE
     - 6 movers (IBRX, VKTX, BYND, BITF, SAVA, APP) were NEVER in Trinity
     - Need: wider scanner universe, especially biotech/micro-cap
     - Add Polygon screener for unusual volume + price action

  F) REDESIGN CROSS ANALYSIS RANKING
     - Current: score-based → picks mega-caps
     - New: (score × volatility × sector_boost × persistence) → picks movers
     - Add separate "5x Potential" list alongside existing Top 10
    """)

    # Save full results
    results = {
        "generated": str(Path(__file__).name),
        "total_call_movers": len(CALL_MOVERS),
        "total_put_movers": len(PUT_MOVERS),
        "call_traces": [{
            "symbol": t["mover_data"]["sym"],
            "max_up": t["mover_data"].get("max_up", 0),
            "mult": t["mover_data"]["mult"],
            "trinity_count": len(t["trinity_appearances"]),
            "cross_analysis_count": len(t["cross_analysis_appearances"]),
            "has_forecast": bool(t["forecast_data"]),
            "has_uw_flow": bool(t["uw_flow_data"]),
            "drop_point": t["pipeline_drop_point"],
            "early_signals": [a for a in t["trinity_appearances"] if a["date"] < t["mover_data"].get("best_day", "9999")],
        } for t in call_traces],
        "put_traces": [{
            "symbol": t["mover_data"]["sym"],
            "max_down": t["mover_data"].get("max_down", 0),
            "mult": t["mover_data"]["mult"],
            "trinity_count": len(t["trinity_appearances"]),
            "cross_analysis_count": len(t["cross_analysis_appearances"]),
            "has_forecast": bool(t["forecast_data"]),
            "has_uw_flow": bool(t["uw_flow_data"]),
            "drop_point": t["pipeline_drop_point"],
        } for t in put_traces],
    }

    out_file = OUTPUT / "forensic_5x_movers_analysis.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  💾 Results saved: {out_file}")


if __name__ == "__main__":
    main()
