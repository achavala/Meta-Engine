"""
🏛️ META ENGINE — Core Orchestrator
=====================================
The Meta Engine sits on top of PutsEngine and Moonshot Engine,
running 2× daily on every trading day (9:35 AM, 3:15 PM ET) to:

1. Get Top 10 picks from PutsEngine (bearish/distribution signals)
2. Get Top 10 picks from Moonshot Engine (bullish/squeeze signals)
3. Store both Top 10s in output files
4. Cross-analyze: Run PutsEngine Top 10 through Moonshot, and vice versa
5. Generate 3-sentence institutional summary for each pick
6. Generate technical analysis chart with RSI
7. Email the full report with chart attachment
8. Send Telegram alert with summaries
9. Post to X/Twitter as a thread

CRITICAL: This engine does NOT modify PutsEngine or TradeNova in any way.
It imports their modules read-only and uses their API clients.
"""

import sys
import os
import json
import logging
import fcntl
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional

import pytz

# Ensure Meta Engine path is in sys.path
META_DIR = Path(__file__).parent
if str(META_DIR) not in sys.path:
    sys.path.insert(0, str(META_DIR))

from config import MetaConfig

# Lock file to prevent concurrent runs
LOCK_FILE = META_DIR / ".meta_engine.lock"

# Setup logging
LOG_DIR = Path(MetaConfig.LOGS_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOG_DIR / f"meta_engine_{datetime.now().strftime('%Y%m%d')}.log",
            mode="a"
        ),
    ],
)
logger = logging.getLogger("MetaEngine")

# Timezone
EST = pytz.timezone("US/Eastern")

# US market holidays 2026
US_HOLIDAYS_2026 = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
    date(2026, 4, 3), date(2026, 5, 25), date(2026, 7, 3),
    date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
}


def is_trading_day(d: date = None) -> bool:
    """Check if a given date is a US market trading day."""
    if d is None:
        d = datetime.now(EST).date()
    if d.weekday() >= 5:
        return False
    if d in US_HOLIDAYS_2026:
        return False
    return True


LOCK_STALE_MINUTES = 30  # Auto-release lock if older than 30 min


def _acquire_lock() -> Optional[object]:
    """
    Acquire an exclusive file lock to prevent concurrent Meta Engine runs.
    Returns the lock file object if acquired, None if another run is active.
    
    Stale lock detection: if the lock file is > LOCK_STALE_MINUTES old,
    assume the previous holder crashed and force-release the lock.
    """
    # ── Stale lock detection ──────────────────────────────────
    if LOCK_FILE.exists():
        try:
            lock_age_sec = (datetime.now() - datetime.fromtimestamp(
                LOCK_FILE.stat().st_mtime)).total_seconds()
            if lock_age_sec > LOCK_STALE_MINUTES * 60:
                # Read old lock info for diagnostics
                try:
                    old_info = LOCK_FILE.read_text().strip()
                except Exception:
                    old_info = "(unreadable)"
                logger.warning(
                    f"🔓 Stale lock detected ({lock_age_sec/60:.0f} min old). "
                    f"Force-releasing. Old lock: {old_info}"
                )
                # Try to kill the stale process if PID is available
                for line in old_info.split("\n"):
                    if line.startswith("pid="):
                        try:
                            stale_pid = int(line.split("=")[1])
                            os.kill(stale_pid, 0)  # Check if alive
                            os.kill(stale_pid, 9)  # Force kill
                            logger.warning(f"  Killed stale process PID {stale_pid}")
                        except (ProcessLookupError, ValueError):
                            pass  # Already dead
                        except PermissionError:
                            logger.warning(f"  Cannot kill PID (permission denied)")
                LOCK_FILE.unlink(missing_ok=True)
        except Exception as e:
            logger.debug(f"  Stale lock check error: {e}")
    
    # ── Normal lock acquisition ───────────────────────────────
    try:
        lock_fd = open(LOCK_FILE, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Write PID and timestamp for diagnostics
        lock_fd.write(f"pid={os.getpid()}\nstarted={datetime.now(EST).isoformat()}\n")
        lock_fd.flush()
        return lock_fd
    except (IOError, OSError):
        # Another process holds the lock
        try:
            with open(LOCK_FILE) as f:
                lock_info = f.read().strip()
            logger.warning(f"🔒 Another Meta Engine run is active: {lock_info}")
        except Exception:
            logger.warning("🔒 Another Meta Engine run is active (lock file exists)")
        return None


def _release_lock(lock_fd):
    """Release the file lock."""
    if lock_fd:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
            LOCK_FILE.unlink(missing_ok=True)
        except Exception:
            pass


def _read_market_direction() -> Optional[Dict[str, Any]]:
    """
    Read the latest market direction from PutsEngine's market_direction.json.
    
    This file is updated hourly by PutsEngine's MarketPulse engine.
    Contains: regime, direction, confidence, GEX, VIX, best/avoid plays.
    
    Returns:
        Dict with market direction data, or None if unavailable.
    """
    md_path = Path(MetaConfig.PUTSENGINE_PATH) / "logs" / "market_direction.json"
    try:
        if md_path.exists():
            with open(md_path, "r") as f:
                data = json.load(f)
            # Only use if not too stale (< 2 hours old)
            ts_str = data.get("timestamp", "")
            if ts_str:
                from datetime import timezone
                ts = datetime.fromisoformat(ts_str)
                # Ensure timezone-aware comparison
                if ts.tzinfo is None:
                    ts = EST.localize(ts)
                now_et = datetime.now(EST)
                age_minutes = (now_et - ts).total_seconds() / 60
                if age_minutes > 120:
                    logger.warning(f"  Market direction data is {age_minutes:.0f} min old — using anyway")
            return {
                "regime": data.get("regime", "N/A"),
                "regime_score": data.get("regime_score", 0),
                "direction": data.get("direction", "N/A"),
                "confidence": data.get("confidence", "N/A"),
                "confidence_pct": data.get("confidence_pct", 0),
                "spy_signal": data.get("spy_signal", 0),
                "vix_signal": data.get("vix_signal", 0),
                "gex_regime": data.get("gex_regime", "N/A"),
                "gex_value": data.get("gex_value", 0),
                "best_plays": data.get("best_plays", []),
                "avoid_plays": data.get("avoid_plays", []),
                "timestamp": ts_str,
            }
        else:
            logger.warning(f"  Market direction file not found: {md_path}")
            return None
    except Exception as e:
        logger.error(f"  Error reading market direction: {e}")
        return None


def _backfill_prices_from_cross(
    picks: list,
    cross_items: list,
) -> None:
    """
    ALWAYS update prices in the original pick list using the real-time
    market data from the cross-analysis step (which fetched 30-day bars
    from Polygon API).

    This ensures saved files and report tables show the most current
    prices, even when the initial picks came from cached PutsEngine
    files with stale prices (e.g. RBLX $66 cached → $73 real-time).

    Unlike the previous version, this does NOT check for price==0 first.
    Stale non-zero prices are just as dangerous as zero prices for
    trading decisions.
    """
    cross_map = {item["symbol"]: item for item in cross_items}
    updated = 0
    for pick in picks:
        sym = pick["symbol"]
        cx = cross_map.get(sym, {})
        # The cross-analysis market_data uses the latest Polygon 30-day bars
        md_price = cx.get("market_data", {}).get("price", 0)
        if md_price > 0:
            old_price = pick.get("price", 0)
            if old_price != md_price:
                if old_price > 0:
                    pct_diff = ((md_price - old_price) / old_price) * 100
                    if abs(pct_diff) > 1:
                        logger.debug(
                            f"    {sym}: ${old_price:.2f} → ${md_price:.2f} "
                            f"({pct_diff:+.1f}%)"
                        )
                pick["price"] = md_price
                updated += 1
    if updated > 0:
        logger.info(
            f"  ✅ Updated prices for {updated} picks from cross-analysis "
            f"Polygon market data (most current)"
        )


def run_meta_engine(force: bool = False) -> Dict[str, Any]:
    """
    Execute the full Meta Engine pipeline.
    
    Args:
        force: If True, run even on non-trading days
        
    Returns:
        Dict with all results and status
    """
    now = datetime.now(EST)
    logger.info("=" * 70)
    logger.info("🏛️  META ENGINE — STARTING")
    logger.info(f"   Time: {now.strftime('%B %d, %Y %I:%M:%S %p ET')}")
    logger.info("=" * 70)
    
    # Check trading day
    if not force and not is_trading_day():
        logger.info("📅 Not a trading day. Use --force to run anyway.")
        return {"status": "skipped", "reason": "not_trading_day"}
    
    # Acquire lock to prevent concurrent runs
    lock_fd = _acquire_lock()
    if lock_fd is None:
        logger.error("❌ Cannot start — another Meta Engine run is already in progress.")
        return {"status": "skipped", "reason": "concurrent_run_blocked"}
    
    try:
        return _run_pipeline(now, force)
    finally:
        _release_lock(lock_fd)


def _run_pipeline(now: datetime, force: bool = False) -> Dict[str, Any]:
    """Internal: Execute the pipeline after lock is acquired."""
    # Validate configuration
    config_status = MetaConfig.validate()
    logger.info(f"📋 Config: APIs={config_status['apis']} | "
                f"Email={config_status['email']['configured']} | "
                f"Telegram={config_status['telegram']['configured']} | "
                f"X={config_status['x_twitter']['configured']}")
    
    results = {
        "timestamp": now.isoformat(),
        "status": "running",
        "puts_top10": [],
        "moonshot_top10": [],
        "cross_analysis": {},
        "summaries": {},
        "chart_path": None,
        "notifications": {"email": False, "telegram": False, "x_twitter": False},
    }
    
    output_dir = Path(MetaConfig.OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # ================================================================
    # STEP 1: Get Top Puts (Policy B: quality over quantity)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 1: Getting PutsEngine picks (Policy B)...")
    logger.info("=" * 50)
    
    from engine_adapters.puts_adapter import get_top_puts
    puts_top10 = get_top_puts(top_n=MetaConfig.TOP_N_PICKS)
    results["puts_top10"] = puts_top10
    
    # Log quality-over-quantity status
    if len(puts_top10) < 3:
        logger.warning(
            f"  ⚠️ LOW OPPORTUNITY: Only {len(puts_top10)} puts passed Policy B gates. "
            f"Capital preserved — this is expected on quiet days."
        )
    
    # Save to file
    puts_file = output_dir / f"puts_top10_{now.strftime('%Y%m%d')}.json"
    with open(puts_file, "w") as f:
        json.dump({"timestamp": now.isoformat(), "picks": puts_top10}, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {puts_file}")
    
    # ================================================================
    # STEP 2: Get Top Moonshots (Policy B: quality over quantity)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 2: Getting Moonshot picks (Policy B)...")
    logger.info("=" * 50)
    
    from engine_adapters.moonshot_adapter import get_top_moonshots
    moonshot_top10 = get_top_moonshots(top_n=MetaConfig.TOP_N_PICKS)
    results["moonshot_top10"] = moonshot_top10
    
    # Log quality-over-quantity status
    if len(moonshot_top10) < 3:
        logger.warning(
            f"  ⚠️ LOW OPPORTUNITY: Only {len(moonshot_top10)} moonshots passed Policy B gates. "
            f"Capital preserved — this is expected on quiet days."
        )
    
    # Save to file
    moon_file = output_dir / f"moonshot_top10_{now.strftime('%Y%m%d')}.json"
    with open(moon_file, "w") as f:
        json.dump({"timestamp": now.isoformat(), "picks": moonshot_top10}, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {moon_file}")
    
    # ================================================================
    # STEP 2a: Dual-Direction Awareness (FEB 16 v5)
    # ================================================================
    # Cross-check for symbols appearing in BOTH moonshot AND puts picks.
    # When a stock shows up in both directions, it means the scoring found
    # evidence for both up and down — the system must pick ONE direction.
    # We flag these conflicts so the user is aware and can tiebreak manually.
    puts_syms = {p.get("symbol", "") for p in puts_top10}
    moon_syms = {m.get("symbol", "") for m in moonshot_top10}
    overlap = puts_syms & moon_syms - {""}
    if overlap:
        logger.warning(
            f"  ⚠️ DUAL-DIRECTION CONFLICT: {overlap} appear in BOTH moonshot AND puts picks!"
        )
        for sym in overlap:
            # Get conviction scores from both
            moon_conv = next((m.get("_conviction_score", 0) for m in moonshot_top10
                              if m.get("symbol") == sym), 0)
            puts_conv = next((p.get("_conviction_score", 0) for p in puts_top10
                              if p.get("symbol") == sym), 0)
            # Flag both with the conflict
            for picks_list in (moonshot_top10, puts_top10):
                for p in picks_list:
                    if p.get("symbol") == sym:
                        p["_dual_direction_conflict"] = True
                        p["_opposing_conviction"] = (
                            puts_conv if picks_list is moonshot_top10 else moon_conv
                        )
            # Log which direction wins
            winner = "CALLS" if moon_conv > puts_conv else "PUTS"
            logger.warning(
                f"    {sym}: moonshot_conv={moon_conv:.3f} vs puts_conv={puts_conv:.3f} "
                f"→ {winner} has higher conviction"
            )

    # ================================================================
    # STEP 2b: Gap-Up Detection (Same-Day Plays)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 2b: Gap-Up Detection (Same-Day Plays)...")
    logger.info("=" * 50)

    gap_up_data = {}
    try:
        from engine_adapters.gap_up_detector import detect_gap_ups, format_gap_up_report
        gap_up_data = detect_gap_ups(polygon_api_key=MetaConfig.POLYGON_API_KEY)
        gap_candidates = gap_up_data.get("candidates", [])
        if gap_candidates:
            logger.info(f"  🚀 {len(gap_candidates)} gap-up candidates detected")
            # Save gap-up data
            gap_file = output_dir / f"gap_up_alerts_{now.strftime('%Y%m%d_%H%M')}.json"
            with open(gap_file, "w") as f:
                json.dump(gap_up_data, f, indent=2, default=str)
            logger.info(f"  💾 Saved: {gap_file}")
            # Also save as latest
            gap_latest = output_dir / "gap_up_alerts_latest.json"
            with open(gap_latest, "w") as f:
                json.dump(gap_up_data, f, indent=2, default=str)
        else:
            logger.info("  ℹ️ No gap-up candidates detected — quiet pre-market")
    except Exception as e:
        logger.warning(f"  ⚠️ Gap-up detection failed: {e}")

    results["gap_up_alerts"] = gap_up_data

    # ================================================================
    # STEP 2c: Universe Coverage Report (FEB 16 v5.2)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 2c: Universe Coverage Report...")
    logger.info("=" * 50)

    try:
        from engine_adapters.universe_scanner import get_coverage_report
        coverage_report = get_coverage_report()
        coverage_pct = coverage_report.get("coverage_pct", 0)
        uncovered_count = coverage_report.get("uncovered_count", 0)
        logger.info(
            f"  🌐 Coverage: {coverage_pct:.1f}% of universe "
            f"({uncovered_count} tickers uncovered)"
        )
        if uncovered_count > 0:
            logger.info(
                f"  ⚠️ Uncovered: {', '.join(coverage_report.get('uncovered_tickers', [])[:15])}"
            )

        # Save coverage report
        cov_file = output_dir / f"coverage_report_{now.strftime('%Y%m%d')}.json"
        with open(cov_file, "w") as f:
            json.dump(coverage_report, f, indent=2, default=str)

        results["coverage_report"] = coverage_report
    except Exception as e:
        logger.warning(f"  ⚠️ Coverage report failed: {e}")
        results["coverage_report"] = {}

    # ================================================================
    # STEP 3: Cross-Engine Analysis
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 3: Cross-Engine Analysis...")
    logger.info("=" * 50)
    
    from analysis.cross_analyzer import cross_analyze
    cross_results = cross_analyze(
        puts_top10=puts_top10,
        moonshot_top10=moonshot_top10,
        polygon_api_key=MetaConfig.POLYGON_API_KEY,
    )
    results["cross_analysis"] = cross_results
    
    # Back-fill prices from cross-analysis market data into the original picks
    # (so the saved top10 files and report tables show real prices)
    _backfill_prices_from_cross(puts_top10, cross_results.get("puts_through_moonshot", []))
    _backfill_prices_from_cross(moonshot_top10, cross_results.get("moonshot_through_puts", []))
    
    # Re-save top10 files with enriched prices
    with open(puts_file, "w") as f:
        json.dump({"timestamp": now.isoformat(), "picks": puts_top10}, f, indent=2, default=str)
    with open(moon_file, "w") as f:
        json.dump({"timestamp": now.isoformat(), "picks": moonshot_top10}, f, indent=2, default=str)
    
    # Save cross-analysis
    cross_file = output_dir / f"cross_analysis_{now.strftime('%Y%m%d')}.json"
    with open(cross_file, "w") as f:
        json.dump(cross_results, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {cross_file}")
    # Save latest cross-analysis (always reflects most recent run)
    # Use atomic write (write to temp then rename) so the dashboard never reads partial data
    latest_cross_path = output_dir / "cross_analysis_latest.json"
    latest_cross_tmp = output_dir / "cross_analysis_latest.json.tmp"
    with open(latest_cross_tmp, "w") as f:
        json.dump(cross_results, f, indent=2, default=str)
    latest_cross_tmp.rename(latest_cross_path)
    
    # ================================================================
    # STEP 3b: Inject Market Direction from PutsEngine
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 3b: Reading Market Direction...")
    logger.info("=" * 50)
    market_direction = _read_market_direction()
    if market_direction:
        cross_results["market_direction"] = market_direction
        logger.info(f"  🌊 Regime: {market_direction.get('regime', 'N/A')} | "
                     f"Direction: {market_direction.get('direction', 'N/A')} | "
                     f"Confidence: {market_direction.get('confidence', 'N/A')}")
        if market_direction.get("best_plays"):
            logger.info(f"  📈 Best plays: {market_direction['best_plays'][:3]}")
    else:
        logger.warning("  ⚠️ Market direction data not available")
        cross_results["market_direction"] = {}
    results["market_direction"] = market_direction or {}
    
    # Re-save cross_analysis with market direction included
    with open(cross_file, "w") as f:
        json.dump(cross_results, f, indent=2, default=str)
    with open(latest_cross_tmp, "w") as f:
        json.dump(cross_results, f, indent=2, default=str)
    latest_cross_tmp.rename(latest_cross_path)

    # ── Generate weather-grade market direction prediction ──
    # This saves to output/market_direction_{timeframe}_latest.json
    # so email, telegram, and X poster can reliably read it
    try:
        from analysis.market_direction_predictor import MarketDirectionPredictor
        md_predictor = MarketDirectionPredictor()
        md_hour = now.hour
        md_timeframe = "today" if md_hour < 12 else "tomorrow"
        md_prediction = md_predictor.predict_market_direction(timeframe=md_timeframe)
        cross_results["weather_direction"] = {
            "label": md_prediction.get("direction_label", ""),
            "confidence_pct": md_prediction.get("confidence_pct", 0),
            "timeframe": md_timeframe,
            "composite": md_prediction.get("composite_score", 0),
        }
        logger.info(f"  🌤️ Weather direction: {md_prediction.get('direction_label', 'N/A')} "
                     f"({md_prediction.get('confidence_pct', 0):.0f}%)")
    except Exception as e:
        logger.warning(f"  ⚠️ Weather-grade market direction failed: {e}")
        cross_results["weather_direction"] = {}

    # ================================================================
    # STEP 4: Generate 3-Sentence Summaries
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 4: Generating Summaries...")
    logger.info("=" * 50)
    
    summaries = {
        "timestamp": now.isoformat(),
        "puts_picks_summaries": [],
        "moonshot_picks_summaries": [],
        "conflict_summaries": [],
        "final_summary": f"Meta Engine Daily Analysis ({now.strftime('%B %d, %Y')}): "
                         f"{len(puts_top10)} put and {len(moonshot_top10)} moonshot "
                         f"candidates identified.",
    }
    try:
        from analysis.summary_generator import generate_all_summaries
        summaries = generate_all_summaries(cross_results)
        results["summaries"] = summaries
    except Exception as e:
        logger.error(f"Summary generation failed: {e}", exc_info=True)
        logger.warning("  ⚠️ Continuing pipeline with basic summaries — "
                       "email/telegram/X will still be sent.")
        # Build minimal summaries so downstream steps have data
        for item in cross_results.get("puts_through_moonshot", []):
            summaries["puts_picks_summaries"].append({
                "symbol": item["symbol"],
                "summary": f"{item['symbol']} at ${float(item.get('price', 0)):.2f}, "
                           f"PutsEngine score {float(item.get('score', 0)):.2f}.",
                "puts_score": float(item.get("score", 0)),
                "moonshot_level": item.get("moonshot_analysis", {}).get(
                    "opportunity_level", "N/A"),
            })
        for item in cross_results.get("moonshot_through_puts", []):
            summaries["moonshot_picks_summaries"].append({
                "symbol": item["symbol"],
                "summary": f"{item['symbol']} at ${float(item.get('price', 0)):.2f}, "
                           f"Moonshot score {float(item.get('score', 0)):.2f}.",
                "moonshot_score": float(item.get("score", 0)),
                "puts_risk": item.get("puts_analysis", {}).get("risk_level", "N/A"),
            })
    results["summaries"] = summaries
    
    # Save summaries
    try:
        summary_file = output_dir / f"summaries_{now.strftime('%Y%m%d')}.json"
        with open(summary_file, "w") as f:
            json.dump(summaries, f, indent=2, default=str)
        logger.info(f"  💾 Saved: {summary_file}")
        # Save latest summaries (always reflects most recent run)
        latest_sum_tmp = output_dir / "summaries_latest.json.tmp"
        with open(latest_sum_tmp, "w") as f:
            json.dump(summaries, f, indent=2, default=str)
        latest_sum_tmp.rename(output_dir / "summaries_latest.json")
    except Exception as e:
        logger.error(f"  Summary save failed: {e}")
    
    # Print summaries to log
    logger.info(f"\n📊 FINAL SUMMARY:\n{summaries.get('final_summary', '')}")
    
    for p in summaries.get("puts_picks_summaries", []):
        logger.info(f"\n🔴 {p['symbol']}: {p['summary'][:200]}...")
    
    for m in summaries.get("moonshot_picks_summaries", []):
        logger.info(f"\n🟢 {m['symbol']}: {m['summary'][:200]}...")
    
    # ================================================================
    # STEP 5: Generate Technical Chart
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 5: Generating Technical Chart...")
    logger.info("=" * 50)
    
    chart_path = None
    try:
        from analysis.chart_generator import generate_meta_chart
        chart_path = generate_meta_chart(
            cross_results=cross_results,
            polygon_api_key=MetaConfig.POLYGON_API_KEY,
            output_dir=str(output_dir),
        )
        results["chart_path"] = chart_path
    except Exception as e:
        logger.error(f"Chart generation failed: {e}")
    
    # ================================================================
    # STEP 5b: Generate Markdown Report
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 5b: Generating Markdown Report...")
    logger.info("=" * 50)
    
    report_md_path = None
    try:
        from analysis.report_generator import generate_md_report
        report_md_path = generate_md_report(
            puts_picks=puts_top10,
            moon_picks=moonshot_top10,
            cross_data=cross_results,
            summaries=summaries,
            output_dir=str(output_dir),
            date_str=now.strftime('%Y%m%d'),
        )
        results["report_md_path"] = report_md_path
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
    
    # ================================================================
    # STEP 6: Send Email (Full .md as HTML + PDF attachment)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 6: Sending Email...")
    logger.info("=" * 50)
    
    try:
        from notifications.email_sender import send_meta_email
        email_sent = send_meta_email(
            summaries=summaries,
            chart_path=chart_path,
            report_md_path=report_md_path,
            smtp_server=MetaConfig.SMTP_SERVER,
            smtp_port=MetaConfig.SMTP_PORT,
            smtp_user=MetaConfig.SMTP_USER,
            smtp_password=MetaConfig.SMTP_PASSWORD,
            recipient=MetaConfig.ALERT_EMAIL,
            gap_up_data=gap_up_data,
        )
        results["notifications"]["email"] = email_sent
    except Exception as e:
        logger.error(f"Email failed: {e}")
    
    # ================================================================
    # STEP 7: Send Telegram (Summaries + Conflict Matrix ONLY)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 7: Sending Telegram (Summaries + Conflicts)...")
    logger.info("=" * 50)
    
    try:
        from notifications.telegram_sender import send_meta_telegram
        tg_sent = send_meta_telegram(
            summaries=summaries,
            chart_path=chart_path,
            bot_token=MetaConfig.TELEGRAM_BOT_TOKEN,
            chat_id=MetaConfig.TELEGRAM_CHAT_ID,
            gap_up_data=gap_up_data,
        )
        results["notifications"]["telegram"] = tg_sent
    except Exception as e:
        logger.error(f"Telegram failed: {e}")
    
    # Determine session label from current time (used by X poster and trading)
    # 2-session schedule: Morning (9:35 AM), Afternoon (3:15 PM)
    if now.hour < 12:
        session_label = "AM"
    else:
        session_label = "PM"

    # ================================================================
    # STEP 8: Post to X/Twitter (Top 3 Puts + Top 3 Calls)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 8: Posting to X/Twitter (Top 3 each)...")
    logger.info("=" * 50)
    
    try:
        from notifications.x_poster import post_meta_to_x
        x_posted = post_meta_to_x(
            summaries=summaries,
            cross_results=cross_results,
            session_label=session_label,
            gap_up_data=gap_up_data,
        )
        results["notifications"]["x_twitter"] = x_posted
    except Exception as e:
        logger.error(f"X/Twitter failed: {e}")
    
    # ================================================================
    # STEP 9: Automated Trading (Alpaca Options)
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 9: Executing Trades (Alpaca Paper)...")
    logger.info("=" * 50)

    results["trading"] = {"status": "skipped"}
    try:
        from trading.executor import execute_trades
        trade_result = execute_trades(
            cross_results=cross_results,
            session_label=session_label,
        )
        results["trading"] = trade_result
    except Exception as e:
        logger.error(f"Trading execution failed: {e}", exc_info=True)
        results["trading"] = {"status": "error", "error": str(e)}

    # ================================================================
    # FINAL STATUS
    # ================================================================
    results["status"] = "completed"
    results["completed_at"] = datetime.now(EST).isoformat()
    
    # Save final results
    final_file = output_dir / f"meta_engine_run_{now.strftime('%Y%m%d_%H%M')}.json"
    with open(final_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    trading_status = results.get("trading", {})
    trades_placed = trading_status.get("trades_placed", 0)

    logger.info("\n" + "=" * 70)
    logger.info("🏛️  META ENGINE — COMPLETED")
    logger.info(f"   Puts picks: {len(puts_top10)}")
    logger.info(f"   Moonshot picks: {len(moonshot_top10)}")
    logger.info(f"   Email: {'✅' if results['notifications']['email'] else '❌'}")
    logger.info(f"   Telegram: {'✅' if results['notifications']['telegram'] else '❌'}")
    logger.info(f"   X/Twitter: {'✅' if results['notifications']['x_twitter'] else '❌'}")
    logger.info(f"   Chart: {'✅' if chart_path else '❌'}")
    logger.info(f"   Trading: {'✅' if trades_placed > 0 else '⏸️'} ({trades_placed} orders)")
    logger.info(f"   Output: {output_dir}")
    logger.info("=" * 70)
    
    # ──────────────────────────────────────────────────────────
    # Run validation monitor (non-blocking, logs errors only)
    # ──────────────────────────────────────────────────────────
    try:
        from monitoring.validation_monitor import ValidationMonitor
        monitor = ValidationMonitor()
        # Quick validation (last 1 day only for speed)
        monitor.generate_validation_report(days=1)
    except Exception as e:
        logger.debug(f"Validation monitor skipped: {e}")
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Meta Engine — Cross-Engine Analysis")
    parser.add_argument("--force", action="store_true", help="Run even on non-trading days")
    args = parser.parse_args()
    
    run_meta_engine(force=args.force)
