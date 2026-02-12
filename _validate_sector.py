"""Validate Sector Momentum Boost: Does SNDK improve?"""
import sys, json, logging
sys.path.insert(0, ".")
logging.basicConfig(level=logging.INFO, format="%(message)s")

import importlib
import engine_adapters.moonshot_adapter as ma
importlib.reload(ma)

# Get full pool
results = ma._fallback_from_cached_moonshots(top_n=300)
pool = {r["symbol"]: (i+1, r) for i, r in enumerate(results)}

# Key movers
movers = {
    "MU": "+5.97%", "TSM": "+4.05%", "WDC": "+3.96%",
    "ON": "+4.35%", "SNDK": "+8.84%", "ALAB": "+6.20%",
    "LRCX": "+3.10%", "ASML": "+2.80%", "AMD": "+2.50%",
    "AVGO": "+3.40%", "CRDO": "+4.10%", "QCOM": "+1.80%",
}

print("\n" + "=" * 80)
print("TOP 15 MOONSHOT PICKS (with Sector Momentum Boost)")
print("=" * 80)
for i, r in enumerate(results[:15], 1):
    sym = r["symbol"]
    base = r.get("_base_score", 0)
    orm = r.get("_orm_score", 0)
    boost = r.get("_post_orm_boost", 0)
    sec_boost = r.get("_sector_boost", 0)
    sec_heat = r.get("_sector_heat", 0)
    sector = r.get("_sector", "")
    move = movers.get(sym, "")
    mark = f" ← {move}" if move else ""
    sec_tag = f" [🔥{sector}:{sec_heat}p +{sec_boost:.3f}]" if sec_boost else ""
    print(f"  #{i:2d} {sym:6s} final={r['score']:.3f} "
          f"(base={base:.3f} orm={orm:.3f} boost=+{boost:.3f}){sec_tag}{mark}")

# Semiconductor sector specifically
print("\n" + "=" * 80)
print("SEMICONDUCTOR SECTOR PERFORMANCE")
print("=" * 80)
semi_syms = ["NVDA", "AMD", "INTC", "MU", "AVGO", "QCOM", "TSM", "ASML", "AMAT",
             "LRCX", "KLAC", "MRVL", "ON", "SWKS", "STX", "WDC", "CRDO", "ALAB",
             "RMBS", "CLS", "ARM", "WOLF", "TXN", "SNDK", "LITE", "COHR", "UMAC"]

semi_ranked = []
for sym in semi_syms:
    if sym in pool:
        rank, r = pool[sym]
        semi_ranked.append((rank, sym, r))

semi_ranked.sort()
print(f"  {len(semi_ranked)} semiconductor stocks in pool:")
for rank, sym, r in semi_ranked[:20]:
    base = r.get("_base_score", 0)
    orm = r.get("_orm_score", 0)
    boost = r.get("_post_orm_boost", 0)
    sec_boost = r.get("_sector_boost", 0)
    conv = r.get("_convergence_sources", 0)
    move = movers.get(sym, "")
    in_top10 = "TOP 10" if rank <= 10 else f"#{rank:3d}"
    mark = f" ← {move}" if move else ""
    print(f"    {in_top10:>8s} {sym:6s} final={r['score']:.3f} "
          f"base={base:.3f} orm={orm:.3f} sec=+{sec_boost:.3f} "
          f"total_boost=+{boost:.3f}{mark}")

# SNDK before/after
print("\n" + "=" * 80)
print("SNDK IMPROVEMENT ANALYSIS")
print("=" * 80)
if "SNDK" in pool:
    rank, r = pool["SNDK"]
    base = r.get("_base_score", 0)
    orm = r.get("_orm_score", 0)
    conv_bonus = r.get("_convergence_bonus", 0)
    sec_boost = r.get("_sector_boost", 0)
    total_boost = r.get("_post_orm_boost", 0)
    blend = base * 0.55 + orm * 0.45
    print(f"  SNDK Rank: #{rank}")
    print(f"  Base Score: {base:.3f} (early_setup + compression)")
    print(f"  ORM Score:  {orm:.3f}")
    print(f"  ORM Blend:  {blend:.3f} (base×0.55 + orm×0.45)")
    print(f"  Convergence: +{conv_bonus:.3f} (3 sources)")
    print(f"  Sector Boost: +{sec_boost:.3f} (semiconductor momentum)")
    print(f"  Total Boost: +{total_boost:.3f}")
    print(f"  Final Score: {r['score']:.3f}")
    print(f"  Improvement: #52 → #{rank}")

# Check that non-semiconductor picks are stable
print("\n" + "=" * 80)
print("NON-SEMICONDUCTOR STABILITY CHECK")
print("=" * 80)
non_semi = [(i+1, r) for i, r in enumerate(results[:20]) if r.get("_sector", "") != "semiconductors"]
for rank, r in non_semi[:10]:
    sym = r["symbol"]
    sec = r.get("_sector", "none")
    sec_boost = r.get("_sector_boost", 0)
    print(f"  #{rank:2d} {sym:6s} final={r['score']:.3f} sector={sec} sec_boost=+{sec_boost:.3f}")
