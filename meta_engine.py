"""
🏛️ META ENGINE — Core Orchestrator
=====================================
The Meta Engine sits on top of PutsEngine and Moonshot Engine,
running at 9:35 AM EST every trading day to:

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
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional

import pytz

# Ensure Meta Engine path is in sys.path
META_DIR = Path(__file__).parent
if str(META_DIR) not in sys.path:
    sys.path.insert(0, str(META_DIR))

from config import MetaConfig

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
    # STEP 1: Get Top 10 from PutsEngine
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 1: Getting PutsEngine Top 10...")
    logger.info("=" * 50)
    
    from engine_adapters.puts_adapter import get_top_puts
    puts_top10 = get_top_puts(top_n=MetaConfig.TOP_N_PICKS)
    results["puts_top10"] = puts_top10
    
    # Save to file
    puts_file = output_dir / f"puts_top10_{now.strftime('%Y%m%d')}.json"
    with open(puts_file, "w") as f:
        json.dump({"timestamp": now.isoformat(), "picks": puts_top10}, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {puts_file}")
    
    # ================================================================
    # STEP 2: Get Top 10 from Moonshot Engine
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 2: Getting Moonshot Top 10...")
    logger.info("=" * 50)
    
    from engine_adapters.moonshot_adapter import get_top_moonshots
    moonshot_top10 = get_top_moonshots(top_n=MetaConfig.TOP_N_PICKS)
    results["moonshot_top10"] = moonshot_top10
    
    # Save to file
    moon_file = output_dir / f"moonshot_top10_{now.strftime('%Y%m%d')}.json"
    with open(moon_file, "w") as f:
        json.dump({"timestamp": now.isoformat(), "picks": moonshot_top10}, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {moon_file}")
    
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
    
    # Save cross-analysis
    cross_file = output_dir / f"cross_analysis_{now.strftime('%Y%m%d')}.json"
    with open(cross_file, "w") as f:
        json.dump(cross_results, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {cross_file}")
    
    # ================================================================
    # STEP 4: Generate 3-Sentence Summaries
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 4: Generating Summaries...")
    logger.info("=" * 50)
    
    from analysis.summary_generator import generate_all_summaries
    summaries = generate_all_summaries(cross_results)
    results["summaries"] = summaries
    
    # Save summaries
    summary_file = output_dir / f"summaries_{now.strftime('%Y%m%d')}.json"
    with open(summary_file, "w") as f:
        json.dump(summaries, f, indent=2, default=str)
    logger.info(f"  💾 Saved: {summary_file}")
    
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
    # STEP 6: Send Email
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 6: Sending Email...")
    logger.info("=" * 50)
    
    try:
        from notifications.email_sender import send_meta_email
        email_sent = send_meta_email(
            summaries=summaries,
            chart_path=chart_path,
            smtp_server=MetaConfig.SMTP_SERVER,
            smtp_port=MetaConfig.SMTP_PORT,
            smtp_user=MetaConfig.SMTP_USER,
            smtp_password=MetaConfig.SMTP_PASSWORD,
            recipient=MetaConfig.ALERT_EMAIL,
        )
        results["notifications"]["email"] = email_sent
    except Exception as e:
        logger.error(f"Email failed: {e}")
    
    # ================================================================
    # STEP 7: Send Telegram
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 7: Sending Telegram...")
    logger.info("=" * 50)
    
    try:
        from notifications.telegram_sender import send_meta_telegram
        tg_sent = send_meta_telegram(
            summaries=summaries,
            chart_path=chart_path,
            bot_token=MetaConfig.TELEGRAM_BOT_TOKEN,
            chat_id=MetaConfig.TELEGRAM_CHAT_ID,
        )
        results["notifications"]["telegram"] = tg_sent
    except Exception as e:
        logger.error(f"Telegram failed: {e}")
    
    # ================================================================
    # STEP 8: Post to X/Twitter
    # ================================================================
    logger.info("\n" + "=" * 50)
    logger.info("STEP 8: Posting to X/Twitter...")
    logger.info("=" * 50)
    
    try:
        from notifications.x_poster import post_meta_to_x
        x_posted = post_meta_to_x(summaries)
        results["notifications"]["x_twitter"] = x_posted
    except Exception as e:
        logger.error(f"X/Twitter failed: {e}")
    
    # ================================================================
    # FINAL STATUS
    # ================================================================
    results["status"] = "completed"
    results["completed_at"] = datetime.now(EST).isoformat()
    
    # Save final results
    final_file = output_dir / f"meta_engine_run_{now.strftime('%Y%m%d_%H%M')}.json"
    with open(final_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info("\n" + "=" * 70)
    logger.info("🏛️  META ENGINE — COMPLETED")
    logger.info(f"   Puts picks: {len(puts_top10)}")
    logger.info(f"   Moonshot picks: {len(moonshot_top10)}")
    logger.info(f"   Email: {'✅' if results['notifications']['email'] else '❌'}")
    logger.info(f"   Telegram: {'✅' if results['notifications']['telegram'] else '❌'}")
    logger.info(f"   X/Twitter: {'✅' if results['notifications']['x_twitter'] else '❌'}")
    logger.info(f"   Chart: {'✅' if chart_path else '❌'}")
    logger.info(f"   Output: {output_dir}")
    logger.info("=" * 70)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Meta Engine — Cross-Engine Analysis")
    parser.add_argument("--force", action="store_true", help="Run even on non-trading days")
    args = parser.parse_args()
    
    run_meta_engine(force=args.force)
