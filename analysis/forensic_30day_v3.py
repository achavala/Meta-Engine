"""
Forensic 30-Day Options Mover Analysis (Feb 1–27, 2026)
with v3 X-Worthy Selector Simulation
═══════════════════════════════════════════════════════════════

Answers: "With the new v3 code (serial movers, darkpool, ATR, gap plays),
would we have caught the 1x/5x movers?"

Uses ONLY real data: Polygon bars + TradeNova caches + Meta cross_analysis.
"""

import json, os, sys, time, requests
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict, Counter

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path.home() / "PutsEngine"))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")
load_dotenv(Path.home() / "PutsEngine" / ".env", override=False)
load_dotenv(Path.home() / "TradeNova" / ".env", override=False)

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
TRADENOVA_DATA = Path.home() / "TradeNova" / "data"
META_OUTPUT = Path(__file__).parent.parent / "output"

UNIVERSE = ['AAPL','AFRM','AMAT','AMC','AMD','AMKR','AMZN','APH','APP','ARM','ASML','ASTS',
'AVGO','BABA','BIDU','BILI','BITF','BYND','CEG','CIFR','CLSK','COHR','COIN','CRCL',
'CRDO','CRM','CRSP','CRWD','CRWV','CVNA','DDOG','DJT','DKNG','DNA','ENPH','FIG',
'FSLR','FUBO','FUTU','GME','GOOGL','HIMS','HOOD','HROW','HUT','IBRX','INOD','INTC',
'IONQ','IOVA','IREN','KLAC','LCID','LEU','LI','LITE','LLY','LRCX','LUNR','MARA',
'MDB','MDGL','META','MRNA','MRVL','MSFT','MSTR','MU','NBIS','NET','NFLX','NIO',
'NTLA','NTNX','NVAX','NVDA','OKLO','ORCL','PANW','PATH','PDD','PLTR','PLUG','PTON',
'PYPL','QBTS','QCOM','RBLX','RDDT','RGTI','RIOT','RIVN','RKLB','RKT','ROKU','SAVA',
'SEDG','SHOP','SMCI','SNAP','SNDK','SNOW','SOFI','SPCE','SQ','STX','TDOC','TIGR',
'TLN','TSLA','TSM','TTD','U','UBER','UNH','UPST','UUUU','VKTX','VST','WDC','WULF',
'XPEV','ZETA','ZM','ZS']

START_DATE = "2026-01-25"  # Extra buffer for pre-move analysis
END_DATE = "2026-02-27"
ANALYSIS_START = date(2026, 2, 1)
ANALYSIS_END = date(2026, 2, 27)


def fetch_bars(symbol, start=START_DATE, end=END_DATE):
    if not POLYGON_KEY:
        return []
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
            params={"adjusted": "true", "sort": "asc", "limit": 50, "apiKey": POLYGON_KEY},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("results", [])
        if r.status_code == 429:
            time.sleep(12)
            return fetch_bars(symbol, start, end)
    except Exception:
        pass
    return []


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def main():
    print("=" * 80)
    print("FORENSIC 30-DAY ANALYSIS: Feb 1–27, 2026")
    print("125-ticker Static Universe × v3 X-Worthy Selector Simulation")
    print("=" * 80)
    print()

    # ── Load all TradeNova signal caches ──────────────────────────────
    dp_data = load_json(TRADENOVA_DATA / "darkpool_cache.json")
    uw_flow = load_json(TRADENOVA_DATA / "uw_flow_cache.json")
    uw_oi = load_json(TRADENOVA_DATA / "uw_oi_change_cache.json")
    uw_gex = load_json(TRADENOVA_DATA / "uw_gex_cache.json")
    persistence = load_json(TRADENOVA_DATA / "persistence_tracker.json")
    predictive = load_json(TRADENOVA_DATA / "predictive_signals.json")
    inst_radar = load_json(TRADENOVA_DATA / "institutional_radar_daily.json")
    finviz = load_json(TRADENOVA_DATA / "finviz_insider_cache.json")

    uw_flow_data = uw_flow.get("flow_data", {}) if isinstance(uw_flow, dict) else {}
    uw_oi_data = uw_oi.get("data", {}) if isinstance(uw_oi, dict) else {}
    uw_gex_data = uw_gex.get("data", {}) if isinstance(uw_gex, dict) else {}
    persist_cands = persistence.get("candidates", {}) if isinstance(persistence, dict) else {}
    pred_data = predictive.get("signals", predictive) if isinstance(predictive, dict) else {}
    inst_data = inst_radar.get("radar", inst_radar) if isinstance(inst_radar, dict) else {}

    print(f"TradeNova data loaded:")
    print(f"  Darkpool: {len(dp_data)} tickers")
    print(f"  UW Flow: {len(uw_flow_data)} tickers")
    print(f"  UW OI Change: {len(uw_oi_data)} tickers")
    print(f"  UW GEX: {len(uw_gex_data)} tickers")
    print(f"  Persistence: {len(persist_cands)} candidates")
    print(f"  Predictive Signals: {len(pred_data)} entries")
    print(f"  Institutional Radar: {len(inst_data)} entries")
    print(f"  Finviz Insider: {len(finviz)} entries")
    print()

    # ── Load all Meta cross_analysis files ────────────────────────────
    meta_picks_by_date = {}
    ca_files = sorted(META_OUTPUT.glob("cross_analysis_2026*.json"))
    for cf in ca_files:
        try:
            ca = json.load(open(cf))
            datestr = cf.stem.split("_")[-1]  # 20260227
            dt = date(int(datestr[:4]), int(datestr[4:6]), int(datestr[6:8]))
            puts = [p.get("symbol","") for p in ca.get("puts_through_moonshot", [])]
            calls = [c.get("symbol","") for c in ca.get("moonshot_through_puts", [])]
            meta_picks_by_date[dt] = set(puts + calls)
        except Exception:
            pass
    print(f"Meta cross_analysis loaded: {len(meta_picks_by_date)} dates")
    print()

    # ── Fetch Polygon bars for all 125 tickers ───────────────────────
    print("Fetching Polygon bars for 125 tickers...")
    all_bars = {}
    for i, sym in enumerate(UNIVERSE):
        bars = fetch_bars(sym)
        if bars:
            all_bars[sym] = bars
        if (i + 1) % 25 == 0:
            print(f"  ... {i+1}/{len(UNIVERSE)} fetched ({len(all_bars)} with data)")
        if (i + 1) % 5 == 0:
            time.sleep(0.25)

    print(f"  Total: {len(all_bars)} tickers with bar data")
    print()

    # ── Identify ALL big movers ──────────────────────────────────────
    movers = []  # List of {sym, date, open, close, high, low, pct_move, intra_range, volume, tier}

    for sym, bars in all_bars.items():
        bar_by_date = {}
        for b in bars:
            try:
                ts = b.get("t", 0) / 1000
                dt = date.fromtimestamp(ts)
                bar_by_date[dt] = b
            except Exception:
                continue

        sorted_dates = sorted(bar_by_date.keys())
        for idx, dt in enumerate(sorted_dates):
            if dt < ANALYSIS_START or dt > ANALYSIS_END:
                continue
            b = bar_by_date[dt]
            o = b.get("o", 0)
            c = b.get("c", 0)
            h = b.get("h", 0)
            l = b.get("l", 0)
            v = b.get("v", 0)
            if o <= 0:
                continue

            close_move = abs((c - o) / o) * 100
            intra_range = ((h - l) / o) * 100
            max_up = ((h - o) / o) * 100
            max_down = ((o - l) / o) * 100
            direction = "UP" if c > o else "DOWN"

            # 1x options ≈ stock moves ≥3%, 5x ≈ ≥8%, 10x ≈ ≥15%
            if close_move >= 3.0 or intra_range >= 5.0:
                tier = "1x+"
                if close_move >= 8.0 or intra_range >= 12.0:
                    tier = "5x+"
                if close_move >= 15.0 or intra_range >= 20.0:
                    tier = "10x+"

                # Pre-move features
                pre_5d = []
                pre_2d = []
                pre_1d = None
                for lookback in range(1, 6):
                    lb_idx = idx - lookback
                    if lb_idx >= 0:
                        lb_dt = sorted_dates[lb_idx]
                        lb_b = bar_by_date[lb_dt]
                        lb_o = lb_b.get("o", 0)
                        lb_c = lb_b.get("c", 0)
                        lb_h = lb_b.get("h", 0)
                        lb_l = lb_b.get("l", 0)
                        lb_v = lb_b.get("v", 0)
                        if lb_o > 0:
                            day_ret = ((lb_c - lb_o) / lb_o) * 100
                            day_range = ((lb_h - lb_l) / lb_o) * 100
                            entry = {"date": lb_dt, "ret": day_ret, "range": day_range, "vol": lb_v}
                            pre_5d.append(entry)
                            if lookback <= 2:
                                pre_2d.append(entry)
                            if lookback == 1:
                                pre_1d = entry

                # Signals from TradeNova
                dp_entry = dp_data.get(sym, {})
                dp_val = float(dp_entry.get("total_value", 0) or 0) if isinstance(dp_entry, dict) else 0
                dp_blocks = int(dp_entry.get("block_count", 0) or dp_entry.get("dark_block_count", 0) or 0) if isinstance(dp_entry, dict) else 0

                uw_entry = uw_flow_data.get(sym, {})
                uw_call_prem = 0
                uw_put_prem = 0
                if isinstance(uw_entry, dict):
                    uw_call_prem = float(uw_entry.get("total_call_premium", 0) or 0)
                    uw_put_prem = float(uw_entry.get("total_put_premium", 0) or 0)
                elif isinstance(uw_entry, list):
                    for flow in uw_entry:
                        if isinstance(flow, dict):
                            if (flow.get("put_call","") or "").upper() == "C":
                                uw_call_prem += float(flow.get("premium",0) or 0)
                            else:
                                uw_put_prem += float(flow.get("premium",0) or 0)

                oi_entry = uw_oi_data.get(sym, {})
                oi_change = 0
                if isinstance(oi_entry, dict):
                    oi_change = float(oi_entry.get("net_oi_change", 0) or oi_entry.get("oi_change", 0) or 0)

                gex_entry = uw_gex_data.get(sym, {})
                gex_val = float(gex_entry.get("gex", 0) or gex_entry.get("net_gex", 0) or 0) if isinstance(gex_entry, dict) else 0

                persist_entry = persist_cands.get(sym, {})
                persist_days = int(persist_entry.get("consecutive_days", 0) or persist_entry.get("days", 0) or 0) if isinstance(persist_entry, dict) else 0

                in_meta = False
                for md in meta_picks_by_date:
                    if md == dt and sym in meta_picks_by_date[md]:
                        in_meta = True
                        break

                # v3 signals: serial mover (check if prev day was also big)
                prev_day_big = False
                if pre_1d and abs(pre_1d["ret"]) >= 5.0:
                    prev_day_big = True

                movers.append({
                    "sym": sym, "date": dt, "direction": direction,
                    "open": o, "close": c, "high": h, "low": l,
                    "close_move_pct": round(close_move, 2),
                    "intra_range_pct": round(intra_range, 2),
                    "max_up_pct": round(max_up, 2),
                    "max_down_pct": round(max_down, 2),
                    "volume": v,
                    "tier": tier,
                    "pre_5d": pre_5d, "pre_2d": pre_2d, "pre_1d": pre_1d,
                    "dp_value": dp_val, "dp_blocks": dp_blocks,
                    "uw_call_prem": uw_call_prem, "uw_put_prem": uw_put_prem,
                    "oi_change": oi_change, "gex": gex_val,
                    "persist_days": persist_days,
                    "in_meta": in_meta,
                    "prev_day_big": prev_day_big,
                })

    # ── Sort & categorize ─────────────────────────────────────────────
    movers.sort(key=lambda x: -x["close_move_pct"])

    tier_10x = [m for m in movers if m["tier"] == "10x+"]
    tier_5x = [m for m in movers if m["tier"] == "5x+"]
    tier_1x = [m for m in movers if m["tier"] == "1x+"]

    print("=" * 80)
    print(f"TOTAL BIG MOVERS: {len(movers)}")
    print(f"  10x+ (≥15% close or ≥20% intra): {len(tier_10x)}")
    print(f"  5x+  (≥8% close or ≥12% intra):  {len(tier_5x)}")
    print(f"  1x+  (≥3% close or ≥5% intra):   {len(tier_1x)}")
    print("=" * 80)
    print()

    # ── v3 CAPTURE SIMULATION ────────────────────────────────────────
    # Would v3 have caught these? Check the 3 new signals:
    # 1. Serial mover (prev day big move)
    # 2. Darkpool whale (>$100M)
    # 3. High ATR (we use pre-2d range as proxy)
    print("=" * 80)
    print("v3 X-WORTHY CAPTURE SIMULATION")
    print("Would the new v3 signals have caught these movers?")
    print("=" * 80)
    print()

    for label, tier_list in [("10x+ MOVERS", tier_10x), ("5x+ MOVERS", tier_5x), ("1x+ MOVERS (sample top 30)", tier_1x[:30])]:
        if not tier_list:
            continue
        print(f"\n{'─'*70}")
        print(f"  {label} ({len(tier_list)} events)")
        print(f"{'─'*70}")

        serial_caught = 0
        dp_caught = 0
        atr_caught = 0
        meta_caught = 0
        any_v3_caught = 0

        for m in tier_list:
            v3_signals = []

            # Signal 1: Serial mover
            if m["prev_day_big"]:
                v3_signals.append("SERIAL")
                serial_caught += 1

            # Signal 2: Darkpool
            if m["dp_value"] >= 100_000_000:
                v3_signals.append(f"DP=${m['dp_value']/1e6:.0f}M")
                dp_caught += 1
            elif m["dp_value"] >= 50_000_000:
                v3_signals.append(f"dp=${m['dp_value']/1e6:.0f}M")

            # Signal 3: ATR (use pre-2d average range)
            avg_pre2d_range = 0
            if m["pre_2d"]:
                avg_pre2d_range = sum(d["range"] for d in m["pre_2d"]) / len(m["pre_2d"])
            if avg_pre2d_range >= 5.0:
                v3_signals.append(f"ATR={avg_pre2d_range:.1f}%")
                atr_caught += 1

            # Meta original capture
            if m["in_meta"]:
                v3_signals.append("META✓")
                meta_caught += 1

            if m["prev_day_big"] or m["dp_value"] >= 100_000_000 or avg_pre2d_range >= 5.0 or m["in_meta"]:
                any_v3_caught += 1

            # UW flow
            uw_ratio = ""
            if m["uw_put_prem"] > 0:
                ratio = m["uw_call_prem"] / m["uw_put_prem"]
                uw_ratio = f"C/P={ratio:.1f}"
            elif m["uw_call_prem"] > 0:
                uw_ratio = "C/P=∞"

            sig_str = " | ".join(v3_signals) if v3_signals else "⚠️ MISSED"

            pre1d_ret = f"{m['pre_1d']['ret']:+.1f}%" if m["pre_1d"] else "N/A"
            pre1d_rng = f"{m['pre_1d']['range']:.1f}%" if m["pre_1d"] else "N/A"

            print(
                f"  {m['date']} {m['sym']:6s} {m['direction']:4s} "
                f"close={m['close_move_pct']:+6.1f}% "
                f"intra={m['intra_range_pct']:5.1f}% "
                f"│ prev={pre1d_ret} rng={pre1d_rng} "
                f"│ {uw_ratio:8s} "
                f"│ {sig_str}"
            )

        n = len(tier_list)
        print(f"\n  CAPTURE RATES for {label}:")
        print(f"    Serial mover (prev day ≥5%):  {serial_caught}/{n} = {serial_caught/n*100:.0f}%")
        print(f"    Darkpool whale (≥$100M):       {dp_caught}/{n} = {dp_caught/n*100:.0f}%")
        print(f"    High ATR (pre-2d ≥5%):         {atr_caught}/{n} = {atr_caught/n*100:.0f}%")
        print(f"    Meta original cross_analysis:   {meta_caught}/{n} = {meta_caught/n*100:.0f}%")
        print(f"    ANY v3 signal (union):          {any_v3_caught}/{n} = {any_v3_caught/n*100:.0f}%")

    # ── REPEAT MOVER ANALYSIS ────────────────────────────────────────
    print()
    print("=" * 80)
    print("SERIAL MOVER ANALYSIS")
    print("Stocks that had multiple big-move days")
    print("=" * 80)

    sym_counter = Counter(m["sym"] for m in movers)
    serial = {s: c for s, c in sym_counter.items() if c >= 3}
    for sym, cnt in sorted(serial.items(), key=lambda x: -x[1]):
        sym_movers = sorted([m for m in movers if m["sym"] == sym], key=lambda x: x["date"])
        dates = [f"{m['date'].strftime('%m/%d')}({m['close_move_pct']:+.0f}%)" for m in sym_movers]
        dp_val = sym_movers[0]["dp_value"]

        consecutive_pairs = 0
        for i in range(len(sym_movers) - 1):
            d1 = sym_movers[i]["date"]
            d2 = sym_movers[i + 1]["date"]
            if (d2 - d1).days <= 2:
                consecutive_pairs += 1

        print(f"  {sym:6s}: {cnt:2d} big days, {consecutive_pairs} back-to-back pairs, "
              f"DP=${dp_val/1e6:.0f}M | {', '.join(dates)}")

    # ── PRE-MOVE BEHAVIOR PATTERNS ───────────────────────────────────
    print()
    print("=" * 80)
    print("PRE-MOVE BEHAVIOR PATTERNS (PhD Quant Lens)")
    print("═" * 80)

    for label, tier_list in [("10x+ MOVERS", tier_10x), ("5x+ MOVERS", tier_5x)]:
        if not tier_list:
            continue
        print(f"\n{'─'*50}")
        print(f"  {label}: Pre-Move Behavior ({len(tier_list)} events)")
        print(f"{'─'*50}")

        pre1d_rets = [m["pre_1d"]["ret"] for m in tier_list if m["pre_1d"]]
        pre1d_rngs = [m["pre_1d"]["range"] for m in tier_list if m["pre_1d"]]
        pre2d_rets = []
        pre2d_rngs = []
        for m in tier_list:
            if m["pre_2d"]:
                pre2d_rets.append(sum(d["ret"] for d in m["pre_2d"]) / len(m["pre_2d"]))
                pre2d_rngs.append(sum(d["range"] for d in m["pre_2d"]) / len(m["pre_2d"]))

        pre5d_rets = []
        pre5d_rngs = []
        for m in tier_list:
            if m["pre_5d"]:
                pre5d_rets.append(sum(d["ret"] for d in m["pre_5d"]) / len(m["pre_5d"]))
                pre5d_rngs.append(sum(d["range"] for d in m["pre_5d"]) / len(m["pre_5d"]))

        def stats(vals):
            if not vals:
                return "N/A"
            s = sorted(vals)
            avg = sum(s) / len(s)
            med = s[len(s)//2]
            return f"avg={avg:+.2f}% med={med:+.2f}% min={s[0]:+.2f}% max={s[-1]:+.2f}%"

        print(f"  1-Day Before Return:  {stats(pre1d_rets)}")
        print(f"  1-Day Before Range:   {stats(pre1d_rngs)}")
        print(f"  2-Day Avg Return:     {stats(pre2d_rets)}")
        print(f"  2-Day Avg Range:      {stats(pre2d_rngs)}")
        print(f"  5-Day Avg Return:     {stats(pre5d_rets)}")
        print(f"  5-Day Avg Range:      {stats(pre5d_rngs)}")

        # Direction split
        up = [m for m in tier_list if m["direction"] == "UP"]
        down = [m for m in tier_list if m["direction"] == "DOWN"]
        print(f"  Direction: {len(up)} UP ({len(up)/len(tier_list)*100:.0f}%) vs {len(down)} DOWN ({len(down)/len(tier_list)*100:.0f}%)")

        # Darkpool stats
        dp_vals = [m["dp_value"] for m in tier_list]
        dp_nonzero = [v for v in dp_vals if v > 0]
        dp_whale = [v for v in dp_vals if v >= 100_000_000]
        print(f"  Darkpool: {len(dp_nonzero)}/{len(tier_list)} had DP, "
              f"{len(dp_whale)} whale (≥$100M)")

        # UW Flow
        high_call = sum(1 for m in tier_list if m["uw_call_prem"] > m["uw_put_prem"] * 2)
        high_put = sum(1 for m in tier_list if m["uw_put_prem"] > m["uw_call_prem"] * 2)
        print(f"  UW Flow: {high_call} heavy call, {high_put} heavy put (2x ratio)")

        # Prev day big
        prev_big = sum(1 for m in tier_list if m["prev_day_big"])
        print(f"  Serial Mover (prev ≥5%): {prev_big}/{len(tier_list)} = {prev_big/len(tier_list)*100:.0f}%")

    # ── GAP ANALYSIS: What v3 signals would flag vs what was missed ──
    print()
    print("=" * 80)
    print("GAP ANALYSIS: v3 SIGNALS vs MISSED MOVERS")
    print("=" * 80)

    all_big = tier_10x + tier_5x
    if all_big:
        caught_by_serial = [m for m in all_big if m["prev_day_big"]]
        caught_by_dp = [m for m in all_big if m["dp_value"] >= 100_000_000]
        caught_by_atr = [m for m in all_big if m["pre_2d"] and sum(d["range"] for d in m["pre_2d"])/len(m["pre_2d"]) >= 5.0]
        caught_by_meta = [m for m in all_big if m["in_meta"]]
        caught_any = [m for m in all_big if m["prev_day_big"] or m["dp_value"] >= 100_000_000 or (m["pre_2d"] and sum(d["range"] for d in m["pre_2d"])/len(m["pre_2d"]) >= 5.0) or m["in_meta"]]

        missed = [m for m in all_big if m not in caught_any]

        print(f"\n  5x+ and 10x+ Combined ({len(all_big)} events):")
        print(f"    Caught by serial mover:  {len(caught_by_serial)} ({len(caught_by_serial)/len(all_big)*100:.0f}%)")
        print(f"    Caught by DP whale:      {len(caught_by_dp)} ({len(caught_by_dp)/len(all_big)*100:.0f}%)")
        print(f"    Caught by high ATR:      {len(caught_by_atr)} ({len(caught_by_atr)/len(all_big)*100:.0f}%)")
        print(f"    Caught by Meta original: {len(caught_by_meta)} ({len(caught_by_meta)/len(all_big)*100:.0f}%)")
        print(f"    Caught by ANY v3 signal: {len(caught_any)} ({len(caught_any)/len(all_big)*100:.0f}%)")
        print(f"    STILL MISSED:            {len(missed)} ({len(missed)/len(all_big)*100:.0f}%)")

        if missed:
            print(f"\n  MISSED 5x+/10x+ events (no v3 signal detected):")
            for m in sorted(missed, key=lambda x: -x["close_move_pct"]):
                pre1d_ret = f"{m['pre_1d']['ret']:+.1f}%" if m["pre_1d"] else "N/A"
                print(f"    {m['date']} {m['sym']:6s} {m['direction']:4s} "
                      f"close={m['close_move_pct']:+.1f}% "
                      f"DP=${m['dp_value']/1e6:.0f}M prev={pre1d_ret}")

    # ── UW FLOW DEEP DIVE (direction alignment) ─────────────────────
    print()
    print("=" * 80)
    print("UW FLOW DIRECTION ALIGNMENT ANALYSIS")
    print("Did options flow predict direction BEFORE the move?")
    print("=" * 80)

    for label, tier_list in [("5x+/10x+", tier_10x + tier_5x), ("1x+ (top 50)", sorted(tier_1x, key=lambda x: -x["close_move_pct"])[:50])]:
        if not tier_list:
            continue
        aligned = 0
        misaligned = 0
        no_flow = 0
        for m in tier_list:
            if m["uw_call_prem"] == 0 and m["uw_put_prem"] == 0:
                no_flow += 1
                continue
            if m["direction"] == "UP" and m["uw_call_prem"] > m["uw_put_prem"]:
                aligned += 1
            elif m["direction"] == "DOWN" and m["uw_put_prem"] > m["uw_call_prem"]:
                aligned += 1
            else:
                misaligned += 1

        total_with_flow = aligned + misaligned
        print(f"  {label}: aligned={aligned}/{total_with_flow} ({aligned/max(total_with_flow,1)*100:.0f}%), "
              f"misaligned={misaligned}, no_flow={no_flow}")

    # ── RECOMMENDATIONS ──────────────────────────────────────────────
    print()
    print("=" * 80)
    print("KEY FINDINGS & REMAINING GAPS")
    print("=" * 80)
    print()

    total_big = len(tier_10x) + len(tier_5x)
    if total_big > 0:
        serial_pct = sum(1 for m in (tier_10x + tier_5x) if m["prev_day_big"]) / total_big * 100
        dp_pct = sum(1 for m in (tier_10x + tier_5x) if m["dp_value"] >= 100_000_000) / total_big * 100
        atr_pct = sum(1 for m in (tier_10x + tier_5x) if m["pre_2d"] and sum(d["range"] for d in m["pre_2d"])/len(m["pre_2d"]) >= 5.0) / total_big * 100
        any_pct = sum(1 for m in (tier_10x + tier_5x) if m["prev_day_big"] or m["dp_value"] >= 100_000_000 or (m["pre_2d"] and sum(d["range"] for d in m["pre_2d"])/len(m["pre_2d"]) >= 5.0)) / total_big * 100

        print(f"  v3 Signal Coverage on 5x+/10x+ Movers:")
        print(f"    Serial Mover alone catches:    {serial_pct:.0f}%")
        print(f"    Darkpool Whale alone catches:   {dp_pct:.0f}%")
        print(f"    High ATR alone catches:         {atr_pct:.0f}%")
        print(f"    Combined v3 (union):            {any_pct:.0f}%")
        print()
        print(f"  This means v3 would have flagged {any_pct:.0f}% of all 5x+/10x+ movers")
        print(f"  BEFORE they moved, via at least one of the new signals.")


if __name__ == "__main__":
    main()
