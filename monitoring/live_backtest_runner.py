"""
Live Backtest Runner — Runs during trading hours to track prediction accuracy.

Schedule: Runs at market open (9:30 AM), then every 30 min until 4:00 PM.
At each checkpoint:
  1. Records current Smart Money v3 predictions
  2. Fetches current prices from Polygon
  3. Compares predictions vs reality
  4. Logs everything to output/live_backtest_YYYYMMDD.json

Usage:
    python3 monitoring/live_backtest_runner.py           # One-shot check
    python3 monitoring/live_backtest_runner.py --loop     # Run all day (9:30-4:00)
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("LiveBacktest")

OUTPUT_DIR = Path.home() / "Meta Engine" / "output"
POLYGON_API_KEY = ""


def _get_api_key():
    global POLYGON_API_KEY
    if POLYGON_API_KEY:
        return POLYGON_API_KEY
    try:
        sys.path.insert(0, str(Path.home() / "Meta Engine"))
        from config import MetaConfig
        POLYGON_API_KEY = MetaConfig.POLYGON_API_KEY
    except Exception:
        pass
    return POLYGON_API_KEY


def _fetch_current_prices(tickers):
    """Fetch current prices from Polygon snapshot."""
    import requests
    api_key = _get_api_key()
    if not api_key:
        logger.error("No Polygon API key")
        return {}

    prices = {}
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        syms = ",".join(batch)
        try:
            url = (
                f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
                f"?tickers={syms}&apiKey={api_key}"
            )
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for t in data.get("tickers", []):
                    sym = t.get("ticker", "")
                    day = t.get("day", {})
                    prev = t.get("prevDay", {})
                    prices[sym] = {
                        "current": day.get("c", 0) or t.get("lastTrade", {}).get("p", 0),
                        "prev_close": prev.get("c", 0),
                        "day_change_pct": t.get("todaysChangePerc", 0),
                        "volume": day.get("v", 0),
                    }
        except Exception as e:
            logger.warning(f"Price fetch error: {e}")
        if i + batch_size < len(tickers):
            time.sleep(0.3)
    return prices


def run_checkpoint():
    """Run one checkpoint: scan + price check + compare."""
    now = datetime.now()
    logger.info("=" * 70)
    logger.info(f"CHECKPOINT: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    from engine_adapters.smart_money_scanner import scan_smart_money
    result = scan_smart_money()

    bullish = result["bullish_candidates"][:10]
    bearish = result["bearish_candidates"][:10]

    all_syms = list(set(
        [b["symbol"] for b in bullish] + [b["symbol"] for b in bearish]
    ))
    prices = _fetch_current_prices(all_syms)

    checkpoint = {
        "timestamp": now.isoformat(),
        "top10_calls": [],
        "top10_puts": [],
        "sources": result.get("sources_loaded", []),
    }

    logger.info("\nTOP 10 CALLS:")
    for b in bullish:
        p = prices.get(b["symbol"], {})
        entry = {
            "symbol": b["symbol"],
            "conviction": b["conviction"],
            "day_change_pct": p.get("day_change_pct", 0),
            "current_price": p.get("current", 0),
            "signals": b.get("signals", [])[:3],
        }
        checkpoint["top10_calls"].append(entry)
        mark = "UP" if entry["day_change_pct"] > 1.0 else ("DOWN" if entry["day_change_pct"] < -1.0 else "FLAT")
        logger.info(f"  {b['symbol']:6s} conv={b['conviction']:.2f} | Today: {entry['day_change_pct']:+.1f}% | {mark}")

    logger.info("\nTOP 10 PUTS:")
    for b in bearish:
        p = prices.get(b["symbol"], {})
        entry = {
            "symbol": b["symbol"],
            "conviction": b["conviction"],
            "day_change_pct": p.get("day_change_pct", 0),
            "current_price": p.get("current", 0),
            "signals": b.get("signals", [])[:3],
        }
        checkpoint["top10_puts"].append(entry)
        mark = "DOWN" if entry["day_change_pct"] < -1.0 else ("UP" if entry["day_change_pct"] > 1.0 else "FLAT")
        logger.info(f"  {b['symbol']:6s} conv={b['conviction']:.2f} | Today: {entry['day_change_pct']:+.1f}% | {mark}")

    # Save checkpoint
    date_str = now.strftime("%Y%m%d")
    log_path = OUTPUT_DIR / f"live_backtest_{date_str}.json"
    existing = []
    if log_path.exists():
        try:
            with open(log_path) as f:
                existing = json.load(f)
        except Exception:
            existing = []
    existing.append(checkpoint)
    with open(log_path, "w") as f:
        json.dump(existing, f, indent=2, default=str)

    logger.info(f"\nSaved checkpoint #{len(existing)} to {log_path}")

    # Consistency check against previous checkpoints
    if len(existing) >= 2:
        prev = existing[-2]
        prev_calls = set(c["symbol"] for c in prev.get("top10_calls", []))
        curr_calls = set(c["symbol"] for c in checkpoint["top10_calls"])
        prev_puts = set(c["symbol"] for c in prev.get("top10_puts", []))
        curr_puts = set(c["symbol"] for c in checkpoint["top10_puts"])

        new_calls = curr_calls - prev_calls
        dropped_calls = prev_calls - curr_calls
        new_puts = curr_puts - prev_puts
        dropped_puts = prev_puts - curr_puts

        if new_calls or dropped_calls:
            logger.warning(f"  CALLS CHANGED: +{new_calls or 'none'} -{dropped_calls or 'none'}")
        else:
            logger.info("  CALLS: STABLE (no changes)")

        if new_puts or dropped_puts:
            logger.warning(f"  PUTS CHANGED: +{new_puts or 'none'} -{dropped_puts or 'none'}")
        else:
            logger.info("  PUTS: STABLE (no changes)")

    return checkpoint


def run_loop():
    """Run checkpoints every 30 min during market hours."""
    logger.info("Live Backtest Runner — Starting loop mode")
    logger.info("Will run checkpoints 9:30 AM to 4:00 PM EST every 30 min")

    while True:
        now = datetime.now()
        hour, minute = now.hour, now.minute
        market_time = hour * 60 + minute

        if 570 <= market_time <= 960:  # 9:30 AM to 4:00 PM
            run_checkpoint()
            logger.info("Sleeping 30 minutes until next checkpoint...")
            time.sleep(1800)
        elif market_time < 570:
            wait = (570 - market_time) * 60
            logger.info(f"Pre-market. Waiting {wait // 60} minutes for market open...")
            time.sleep(min(wait, 300))
        else:
            logger.info("Market closed. Generating end-of-day summary...")
            _generate_eod_summary()
            break


def _generate_eod_summary():
    """Generate end-of-day accuracy summary."""
    date_str = datetime.now().strftime("%Y%m%d")
    log_path = OUTPUT_DIR / f"live_backtest_{date_str}.json"
    if not log_path.exists():
        logger.warning("No checkpoints found for today")
        return

    with open(log_path) as f:
        checkpoints = json.load(f)

    if not checkpoints:
        return

    logger.info("\n" + "=" * 70)
    logger.info("END OF DAY SUMMARY")
    logger.info("=" * 70)

    first = checkpoints[0]
    last = checkpoints[-1]

    logger.info(f"Checkpoints: {len(checkpoints)} ({first['timestamp']} to {last['timestamp']})")

    # Track which predictions were consistent across ALL checkpoints
    call_syms = {}
    put_syms = {}
    for cp in checkpoints:
        for c in cp.get("top10_calls", []):
            call_syms.setdefault(c["symbol"], []).append(c.get("day_change_pct", 0))
        for p in cp.get("top10_puts", []):
            put_syms.setdefault(p["symbol"], []).append(p.get("day_change_pct", 0))

    logger.info("\nCALL predictions consistency:")
    for sym, changes in sorted(call_syms.items(), key=lambda x: -len(x[1])):
        final_chg = changes[-1]
        mark = "WIN" if final_chg > 1.0 else ("LOSS" if final_chg < -1.0 else "FLAT")
        logger.info(f"  {sym:6s} appeared {len(changes)}/{len(checkpoints)} times | EOD: {final_chg:+.1f}% {mark}")

    logger.info("\nPUT predictions consistency:")
    for sym, changes in sorted(put_syms.items(), key=lambda x: -len(x[1])):
        final_chg = changes[-1]
        mark = "WIN" if final_chg < -1.0 else ("LOSS" if final_chg > 1.0 else "FLAT")
        logger.info(f"  {sym:6s} appeared {len(changes)}/{len(checkpoints)} times | EOD: {final_chg:+.1f}% {mark}")


if __name__ == "__main__":
    if "--loop" in sys.argv:
        run_loop()
    else:
        run_checkpoint()
