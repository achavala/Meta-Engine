"""
DEEP-DIVE Forensic Analysis: Feb 1–27, 2026
═══════════════════════════════════════════════════════════════
For each 5x+/10x+ mover: pulls EVERY signal from EVERY data source.
Pure data analysis — no code changes.
"""

import json, os, sys, time, requests
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict, Counter

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")
load_dotenv(Path.home() / "PutsEngine" / ".env", override=False)
load_dotenv(Path.home() / "TradeNova" / ".env", override=False)

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
TN = Path.home() / "TradeNova" / "data"
META_OUT = Path(__file__).parent.parent / "output"

UNIVERSE = sorted(['AAPL','AFRM','AMAT','AMC','AMD','AMKR','AMZN','APH','APP','ARM','ASML','ASTS',
'AVGO','BABA','BIDU','BILI','BITF','BYND','CEG','CIFR','CLSK','COHR','COIN','CRCL',
'CRDO','CRM','CRSP','CRWD','CRWV','CVNA','DDOG','DJT','DKNG','DNA','ENPH','FIG',
'FSLR','FUBO','FUTU','GME','GOOGL','HIMS','HOOD','HROW','HUT','IBRX','INOD','INTC',
'IONQ','IOVA','IREN','KLAC','LCID','LEU','LI','LITE','LLY','LRCX','LUNR','MARA',
'MDB','MDGL','META','MRNA','MRVL','MSFT','MSTR','MU','NBIS','NET','NFLX','NIO',
'NTLA','NTNX','NVAX','NVDA','OKLO','ORCL','PANW','PATH','PDD','PLTR','PLUG','PTON',
'PYPL','QBTS','QCOM','RBLX','RDDT','RGTI','RIOT','RIVN','RKLB','RKT','ROKU','SAVA',
'SEDG','SHOP','SMCI','SNAP','SNDK','SNOW','SOFI','SPCE','SQ','STX','TDOC','TIGR',
'TLN','TSLA','TSM','TTD','U','UBER','UNH','UPST','UUUU','VKTX','VST','WDC','WULF',
'XPEV','ZETA','ZM','ZS'])


def lj(path):
    try:
        with open(path) as f: return json.load(f)
    except: return {}


def fetch_bars(sym):
    if not POLYGON_KEY: return []
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/2026-01-25/2026-02-27",
            params={"adjusted":"true","sort":"asc","limit":50,"apiKey":POLYGON_KEY}, timeout=10)
        if r.status_code == 200: return r.json().get("results",[])
        if r.status_code == 429: time.sleep(12); return fetch_bars(sym)
    except: pass
    return []


def main():
    # ── Load ALL signal sources ────────────────────────────────────
    dp_data = lj(TN / "darkpool_cache.json")
    uw_flow = lj(TN / "uw_flow_cache.json").get("flow_data", {})
    uw_oi = lj(TN / "uw_oi_change_cache.json").get("data", {})
    uw_gex = lj(TN / "uw_gex_cache.json").get("data", {})
    persistence = lj(TN / "persistence_tracker.json").get("candidates", {})
    pred_sigs = lj(TN / "predictive_signals.json")
    inst_radar = lj(TN / "institutional_radar_daily.json").get("ticker_signals", {})
    finviz = lj(TN / "finviz_insider_cache.json")
    early_mom = lj(TN / "early_momentum_candidates.json").get("candidates", [])

    early_mom_syms = {c["symbol"]: c for c in early_mom if isinstance(c, dict) and c.get("symbol")}

    # Build predictive signal per-ticker index
    pred_by_ticker = defaultdict(list)
    for dt_str, day_data in pred_sigs.items():
        if not isinstance(day_data, dict): continue
        scans = day_data.get("scans", [])
        for scan in scans:
            for sig in scan.get("signals", []):
                sym = sig.get("symbol", "")
                if sym:
                    pred_by_ticker[sym].append({
                        "date": dt_str,
                        "scan": scan.get("scan_label", ""),
                        "type": sig.get("signal_type", ""),
                        "category": sig.get("category", ""),
                        "score": sig.get("score", 0),
                    })

    # Meta picks
    meta_picks = {}
    for cf in sorted(META_OUT.glob("cross_analysis_2026*.json")):
        try:
            ca = json.load(open(cf))
            ds = cf.stem.split("_")[-1]
            dt = date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
            puts = [p.get("symbol","") for p in ca.get("puts_through_moonshot",[])]
            calls = [c.get("symbol","") for c in ca.get("moonshot_through_puts",[])]
            meta_picks[dt] = set(puts + calls)
        except: pass

    print("=" * 90)
    print("DEEP-DIVE FORENSIC: Every Signal for Every 5x+/10x+ Mover (Feb 1–27, 2026)")
    print("=" * 90)
    print(f"Data: {len(dp_data)} DP, {len(uw_flow)} UW, {len(uw_oi)} OI, {len(uw_gex)} GEX,")
    print(f"  {len(persistence)} persist, {len(pred_by_ticker)} pred, {len(inst_radar)} radar,")
    print(f"  {len(finviz)} finviz, {len(early_mom_syms)} early_mom, {len(meta_picks)} meta dates")
    print()

    # ── Fetch bars & find movers ───────────────────────────────────
    print("Fetching Polygon bars for 125 tickers...")
    all_bars = {}
    for i, sym in enumerate(UNIVERSE):
        bars = fetch_bars(sym)
        if bars: all_bars[sym] = bars
        if (i+1) % 25 == 0: print(f"  ...{i+1}/125")
        if (i+1) % 5 == 0: time.sleep(0.25)
    print(f"  Got bars for {len(all_bars)} tickers\n")

    movers = []
    for sym, bars in all_bars.items():
        bbd = {}
        for b in bars:
            try:
                dt = date.fromtimestamp(b["t"]/1000)
                bbd[dt] = b
            except: continue
        sd = sorted(bbd.keys())
        for idx, dt in enumerate(sd):
            if dt < date(2026,2,1) or dt > date(2026,2,27): continue
            b = bbd[dt]
            o,c,h,l,v = b.get("o",0),b.get("c",0),b.get("h",0),b.get("l",0),b.get("v",0)
            if o <= 0: continue
            cm = abs((c-o)/o)*100
            ir_ = ((h-l)/o)*100
            if cm < 8.0 and ir_ < 12.0: continue  # Only 5x+ and 10x+

            direction = "UP" if c > o else "DOWN"
            tier = "10x+" if (cm >= 15 or ir_ >= 20) else "5x+"

            # Pre-move bars
            pre = []
            for lb in range(1,6):
                li = idx - lb
                if li >= 0:
                    pb = bbd[sd[li]]
                    po = pb.get("o",0)
                    if po > 0:
                        pre.append({
                            "d": sd[li], "ret": ((pb["c"]-po)/po)*100,
                            "rng": ((pb["h"]-pb["l"])/po)*100, "v": pb.get("v",0)
                        })

            movers.append({
                "sym": sym, "date": dt, "dir": direction, "tier": tier,
                "o": o, "c": c, "h": h, "l": l, "vol": v,
                "cm": round(cm,2), "ir": round(ir_,2),
                "max_up": round(((h-o)/o)*100,2), "max_dn": round(((o-l)/o)*100,2),
                "pre": pre,
            })

    movers.sort(key=lambda x: -x["cm"])
    print(f"Total 5x+/10x+ mover events: {len(movers)}")
    print(f"  10x+: {sum(1 for m in movers if m['tier']=='10x+')}")
    print(f"  5x+:  {sum(1 for m in movers if m['tier']=='5x+')}")
    print()

    # ── PER-TICKER DEEP DIVES ──────────────────────────────────────
    # Group movers by ticker for full profiles
    by_ticker = defaultdict(list)
    for m in movers:
        by_ticker[m["sym"]].append(m)

    # Sort tickers by total number of big-move days
    ranked = sorted(by_ticker.items(), key=lambda x: (-len(x[1]), -max(m["cm"] for m in x[1])))

    print("=" * 90)
    print("SECTION 1: PER-TICKER FULL SIGNAL PROFILES (Top 30 Serial Movers)")
    print("=" * 90)

    for rank, (sym, sym_movers) in enumerate(ranked[:30], 1):
        sm = sorted(sym_movers, key=lambda x: x["date"])
        best = max(sym_movers, key=lambda x: x["cm"])

        # Darkpool
        dp = dp_data.get(sym, {})
        dp_val = float(dp.get("total_value",0) or 0) if isinstance(dp,dict) else 0
        dp_blocks = int(dp.get("block_count",0) or dp.get("dark_block_count",0) or 0) if isinstance(dp,dict) else 0
        dp_prints = int(dp.get("print_count",0) or 0) if isinstance(dp,dict) else 0

        # UW Flow
        flows = uw_flow.get(sym, [])
        call_prem = sum(float(f.get("premium",0) or 0) for f in flows if isinstance(f,dict) and (f.get("put_call","")or"").upper()=="C")
        put_prem = sum(float(f.get("premium",0) or 0) for f in flows if isinstance(f,dict) and (f.get("put_call","")or"").upper()=="P")
        sweeps = sum(1 for f in flows if isinstance(f,dict) and f.get("is_sweep"))
        blocks = sum(1 for f in flows if isinstance(f,dict) and f.get("is_block"))
        unusual = sum(1 for f in flows if isinstance(f,dict) and f.get("is_unusual"))

        # UW OI
        oi = uw_oi.get(sym, {})
        oi_call = int(oi.get("call_oi_change",0) or 0) if isinstance(oi,dict) else 0
        oi_put = int(oi.get("put_oi_change",0) or 0) if isinstance(oi,dict) else 0
        oi_total = int(oi.get("total_oi_change",0) or 0) if isinstance(oi,dict) else 0

        # GEX
        gx = uw_gex.get(sym, {})
        net_gex = float(gx.get("net_gex",0) or 0) if isinstance(gx,dict) else 0
        net_delta = float(gx.get("net_delta",0) or 0) if isinstance(gx,dict) else 0

        # Persistence
        pt = persistence.get(sym, {})
        pt_count = int(pt.get("total_appearances_24h",0) or 0) if isinstance(pt,dict) else 0

        # Predictive signals
        ps_list = pred_by_ticker.get(sym, [])

        # Institutional radar
        ir = inst_radar.get(sym, {})
        ir_signals = ir.get("signals", []) if isinstance(ir, dict) else []
        ir_conv = ir.get("conviction", "") if isinstance(ir, dict) else ""
        ir_details = ir.get("details", {}) if isinstance(ir, dict) else {}

        # Finviz insider
        fv = finviz.get(sym, {})
        fv_buys = int(fv.get("total_buys",0) or 0) if isinstance(fv,dict) else 0
        fv_sells = int(fv.get("total_sells",0) or 0) if isinstance(fv,dict) else 0
        fv_net = float(fv.get("net_value",0) or 0) if isinstance(fv,dict) else 0
        fv_sent = (fv.get("net_sentiment","") or "") if isinstance(fv,dict) else ""
        fv_inst_own = float(fv.get("institutional_ownership_pct",0) or 0) if isinstance(fv,dict) else 0

        # Early momentum
        em = early_mom_syms.get(sym, {})
        em_score = float(em.get("emc_score",0) or 0) if em else 0

        # Meta capture
        meta_dates = [m["date"] for m in sm if m["date"] in meta_picks and sym in meta_picks[m["date"]]]

        # v3 signals
        has_serial = any(m["pre"] and abs(m["pre"][0]["ret"]) >= 5.0 for m in sm)
        has_dp_whale = dp_val >= 100_000_000
        avg_pre2d_range = 0
        pre2d_ranges = []
        for m in sm:
            if len(m["pre"]) >= 2:
                pre2d_ranges.append(sum(p["rng"] for p in m["pre"][:2])/2)
        has_high_atr = pre2d_ranges and max(pre2d_ranges) >= 5.0

        print(f"\n{'━'*90}")
        print(f"  #{rank} {sym} — {len(sm)} big days (5x+/10x+), best={best['cm']:+.1f}% on {best['date']}")
        print(f"{'━'*90}")

        # Move timeline
        dates_str = ", ".join(f"{m['date'].strftime('%m/%d')}({m['cm']:+.0f}%{m['dir'][0]})" for m in sm)
        print(f"  MOVES: {dates_str}")

        # Darkpool
        dp_label = "🐋 WHALE" if dp_val >= 100_000_000 else ("📊 Significant" if dp_val >= 50_000_000 else "📉 Low")
        print(f"  DARKPOOL: {dp_label} ${dp_val/1e6:.0f}M | blocks={dp_blocks} prints={dp_prints}")

        # UW Flow
        cp_ratio = call_prem/put_prem if put_prem > 0 else float('inf') if call_prem > 0 else 0
        cp_str = f"{cp_ratio:.1f}" if cp_ratio != float('inf') else "∞"
        print(f"  UW FLOW: call_prem=${call_prem/1e6:.2f}M put_prem=${put_prem/1e6:.2f}M C/P={cp_str} | sweeps={sweeps} blocks={blocks} unusual={unusual}")

        # UW OI
        oi_dir = "CALL-heavy" if oi_call > oi_put*1.5 else ("PUT-heavy" if oi_put > oi_call*1.5 else "balanced")
        print(f"  UW OI: total_change={oi_total:+,d} call={oi_call:+,d} put={oi_put:+,d} → {oi_dir}")

        # GEX
        gex_regime = "POSITIVE" if net_gex > 0 else "NEGATIVE"
        delta_dir = "LONG" if net_delta > 0 else "SHORT"
        print(f"  GEX: net={net_gex:,.0f} ({gex_regime}) delta={net_delta:,.0f} ({delta_dir})")

        # Institutional Radar
        if ir_signals:
            print(f"  INST RADAR: {', '.join(ir_signals)} | conviction={ir_conv}")
            for k,v in ir_details.items():
                if isinstance(v, (int,float)) and v != 0:
                    if isinstance(v, float) and abs(v) > 1000:
                        print(f"    {k}: ${v/1e6:.1f}M" if abs(v) > 1_000_000 else f"    {k}: {v:,.0f}")
                    else:
                        print(f"    {k}: {v}")

        # Persistence
        if pt_count > 0:
            print(f"  PERSISTENCE: {pt_count} appearances in 24h")

        # Predictive Signals
        if ps_list:
            print(f"  PREDICTIVE: {len(ps_list)} signals across {len(set(p['date'] for p in ps_list))} days")
            for p in ps_list[:3]:
                print(f"    {p['date']} [{p['scan'][:30]}] {p['type']} ({p['category']}) score={p['score']}")

        # Finviz Insider
        if fv_buys or fv_sells:
            print(f"  INSIDER: buys={fv_buys} sells={fv_sells} net=${fv_net/1e6:.2f}M sent={fv_sent} inst_own={fv_inst_own:.1f}%")

        # Early Momentum
        if em_score > 0:
            print(f"  EARLY MOM: score={em_score:.2f} persist={em.get('persist_score',0):.2f} convergence={em.get('convergence_score',0):.2f}")

        # Meta capture
        if meta_dates:
            print(f"  META CAPTURED: {len(meta_dates)}/{len(sm)} big-move days ({', '.join(str(d) for d in meta_dates)})")
        else:
            print(f"  META: ❌ NOT captured on any big-move day")

        # v3 verdict
        v3_sigs = []
        if has_serial: v3_sigs.append("SERIAL✓")
        if has_dp_whale: v3_sigs.append(f"DP_WHALE(${dp_val/1e6:.0f}M)")
        if has_high_atr: v3_sigs.append(f"HIGH_ATR({max(pre2d_ranges):.1f}%)")
        print(f"  v3 VERDICT: {' | '.join(v3_sigs) if v3_sigs else '⚠️ WOULD BE MISSED'}")

        # Pre-move pattern for best day
        if best["pre"]:
            print(f"  PRE-MOVE (before {best['date']}, {best['cm']:+.1f}% {best['dir']}):")
            for i, p in enumerate(best["pre"]):
                label = ["1d","2d","3d","4d","5d"][i]
                print(f"    {label} before ({p['d']}): ret={p['ret']:+.1f}% range={p['rng']:.1f}% vol={p['v']:,.0f}")

    # ── SECTION 2: MISSED MOVERS — WHY? ────────────────────────────
    print("\n" + "=" * 90)
    print("SECTION 2: STILL-MISSED MOVERS (no v3 signal) — ROOT CAUSE")
    print("=" * 90)

    for m in movers:
        pre2d_rng = 0
        if len(m["pre"]) >= 2:
            pre2d_rng = sum(p["rng"] for p in m["pre"][:2])/2
        serial = m["pre"] and abs(m["pre"][0]["ret"]) >= 5.0
        dp = dp_data.get(m["sym"], {})
        dp_v = float(dp.get("total_value",0) or 0) if isinstance(dp,dict) else 0
        in_meta = m["date"] in meta_picks and m["sym"] in meta_picks[m["date"]]

        if not serial and dp_v < 100_000_000 and pre2d_rng < 5.0 and not in_meta:
            ir = inst_radar.get(m["sym"], {})
            ir_sigs = ir.get("signals",[]) if isinstance(ir,dict) else []
            fv = finviz.get(m["sym"], {})
            fv_sent = fv.get("net_sentiment","") if isinstance(fv,dict) else ""
            ps = pred_by_ticker.get(m["sym"], [])
            oi = uw_oi.get(m["sym"], {})
            oi_call = int(oi.get("call_oi_change",0) or 0) if isinstance(oi,dict) else 0
            oi_put = int(oi.get("put_oi_change",0) or 0) if isinstance(oi,dict) else 0

            print(f"\n  {m['date']} {m['sym']:6s} {m['dir']:4s} close={m['cm']:+.1f}% intra={m['ir']:.1f}%")
            print(f"    DP: ${dp_v/1e6:.0f}M | pre-2d range: {pre2d_rng:.1f}%")
            print(f"    Radar: {ir_sigs if ir_sigs else 'none'}")
            print(f"    Insider: {fv_sent if fv_sent else 'none'}")
            print(f"    Predictive: {len(ps)} signals")
            print(f"    OI change: call={oi_call:+,d} put={oi_put:+,d}")
            if m["pre"]:
                p = m["pre"][0]
                print(f"    Prev day: ret={p['ret']:+.1f}% range={p['rng']:.1f}%")

    # ── SECTION 3: PATTERN SYNTHESIS ───────────────────────────────
    print("\n" + "=" * 90)
    print("SECTION 3: PATTERN SYNTHESIS (30-yr Trading + PhD Quant Lens)")
    print("=" * 90)

    # Pattern 1: Darkpool concentration
    print("\n  PATTERN 1: DARKPOOL CONCENTRATION")
    dp_whale_movers = [(sym, ms) for sym, ms in by_ticker.items()
                       if isinstance(dp_data.get(sym,{}), dict)
                       and float(dp_data[sym].get("total_value",0) or 0) >= 100_000_000]
    dp_whale_movers.sort(key=lambda x: -float(dp_data[x[0]].get("total_value",0) or 0))
    for sym, ms in dp_whale_movers[:15]:
        dv = float(dp_data[sym].get("total_value",0))/1e6
        print(f"    {sym:6s}: ${dv:,.0f}M DP, {len(ms)} big days, avg move={sum(m['cm'] for m in ms)/len(ms):.1f}%")

    # Pattern 2: OI skew before big moves
    print("\n  PATTERN 2: OI CHANGE SKEW (call vs put OI)")
    for sym, ms in ranked[:20]:
        oi = uw_oi.get(sym, {})
        if not isinstance(oi, dict): continue
        oc = int(oi.get("call_oi_change",0) or 0)
        op = int(oi.get("put_oi_change",0) or 0)
        if oc == 0 and op == 0: continue
        ratio = oc/op if op != 0 else float('inf')
        dominant_dir = [m["dir"] for m in ms]
        up_pct = sum(1 for d in dominant_dir if d=="UP")/len(dominant_dir)*100
        print(f"    {sym:6s}: call_oi={oc:+6,d} put_oi={op:+6,d} ratio={ratio:.2f} | moves {up_pct:.0f}% UP")

    # Pattern 3: Institutional radar signals
    print("\n  PATTERN 3: INSTITUTIONAL RADAR SIGNAL FREQUENCY")
    radar_movers = [(sym, ms) for sym, ms in by_ticker.items() if sym in inst_radar]
    radar_movers.sort(key=lambda x: -len(x[1]))
    for sym, ms in radar_movers[:20]:
        ir = inst_radar[sym]
        sigs = ir.get("signals",[])
        conv = ir.get("conviction","")
        print(f"    {sym:6s}: {len(ms)} big days | radar={sigs} conv={conv}")

    # Pattern 4: Predictive signal hit rate
    print("\n  PATTERN 4: PREDICTIVE SIGNAL HIT RATE")
    pred_movers = [(sym, ms) for sym, ms in by_ticker.items() if sym in pred_by_ticker]
    pred_movers.sort(key=lambda x: -len(pred_by_ticker[x[0]]))
    for sym, ms in pred_movers[:15]:
        ps = pred_by_ticker[sym]
        types = Counter(p["type"] for p in ps)
        print(f"    {sym:6s}: {len(ms)} big days, {len(ps)} pred signals | {dict(types)}")

    # Pattern 5: GEX regime and move direction
    print("\n  PATTERN 5: GEX REGIME vs MOVE DIRECTION")
    for sym, ms in ranked[:20]:
        gx = uw_gex.get(sym, {})
        if not isinstance(gx, dict): continue
        ng = float(gx.get("net_gex",0) or 0)
        nd = float(gx.get("net_delta",0) or 0)
        gex_str = "POS" if ng > 0 else "NEG"
        up_moves = sum(1 for m in ms if m["dir"]=="UP")
        dn_moves = sum(1 for m in ms if m["dir"]=="DOWN")
        print(f"    {sym:6s}: GEX={gex_str}({ng:+,.0f}) delta={nd:+,.0f} | {up_moves}UP/{dn_moves}DN")

    # ── SUMMARY: What's missing for the remaining 5%? ─────────────
    print("\n" + "=" * 90)
    print("FINAL ASSESSMENT: REMAINING GAPS")
    print("=" * 90)

    missed = []
    for m in movers:
        pre2d_rng = 0
        if len(m["pre"]) >= 2:
            pre2d_rng = sum(p["rng"] for p in m["pre"][:2])/2
        serial = m["pre"] and abs(m["pre"][0]["ret"]) >= 5.0
        dp = dp_data.get(m["sym"], {})
        dp_v = float(dp.get("total_value",0) or 0) if isinstance(dp,dict) else 0
        in_meta = m["date"] in meta_picks and m["sym"] in meta_picks[m["date"]]
        if not serial and dp_v < 100_000_000 and pre2d_rng < 5.0 and not in_meta:
            ir_sigs = inst_radar.get(m["sym"],{}).get("signals",[]) if isinstance(inst_radar.get(m["sym"],{}),dict) else []
            missed.append({**m, "dp_v": dp_v, "pre2d_rng": pre2d_rng, "ir_sigs": ir_sigs})

    total = len(movers)
    caught = total - len(missed)
    print(f"\n  v3 catches {caught}/{total} = {caught/total*100:.1f}% of all 5x+/10x+ movers")
    print(f"  Still missed: {len(missed)} events ({len(missed)/total*100:.1f}%)")

    if missed:
        missed_syms = Counter(m["sym"] for m in missed)
        print(f"\n  Missed tickers: {dict(missed_syms)}")

        # Check if missed tickers have ANY radar signals
        missed_with_radar = [m for m in missed if m.get("ir_sigs")]
        missed_with_pred = [m for m in missed if m["sym"] in pred_by_ticker]
        missed_with_oi = [m for m in missed if m["sym"] in uw_oi and isinstance(uw_oi[m["sym"]],dict) and int(uw_oi[m["sym"]].get("total_oi_change",0) or 0) > 5000]

        print(f"\n  Could we rescue any missed with OTHER signals?")
        print(f"    Have inst_radar signals: {len(missed_with_radar)}/{len(missed)}")
        print(f"    Have predictive signals: {len(missed_with_pred)}/{len(missed)}")
        print(f"    Have large OI change (>5K): {len(missed_with_oi)}/{len(missed)}")

        # Per-ticker analysis for each missed
        for m in missed:
            sym = m["sym"]
            ir = inst_radar.get(sym, {})
            ir_sigs = ir.get("signals",[]) if isinstance(ir,dict) else []
            oi_entry = uw_oi.get(sym, {})
            oi_change = int(oi_entry.get("total_oi_change",0) or 0) if isinstance(oi_entry,dict) else 0
            ps_count = len(pred_by_ticker.get(sym,[]))
            fv_sent = finviz.get(sym,{}).get("net_sentiment","") if isinstance(finviz.get(sym,{}),dict) else ""

            rescue_sigs = []
            if ir_sigs: rescue_sigs.append(f"radar:{','.join(ir_sigs)}")
            if oi_change > 5000: rescue_sigs.append(f"OI:+{oi_change:,d}")
            if ps_count > 0: rescue_sigs.append(f"pred:{ps_count}")
            if fv_sent and fv_sent != "NEUTRAL": rescue_sigs.append(f"insider:{fv_sent}")

            print(f"    {m['date']} {sym:6s} {m['cm']:+.1f}% → {'RESCUABLE: '+' | '.join(rescue_sigs) if rescue_sigs else 'TRUE SURPRISE (no signal)'}")


if __name__ == "__main__":
    main()
