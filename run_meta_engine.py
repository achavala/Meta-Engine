#!/usr/bin/env python3
"""
🏛️ META ENGINE — Quick Start Entry Point
==========================================

Usage:
    # Run the full Meta Engine pipeline:
    python run_meta_engine.py
    
    # Force run (even on weekends/holidays):
    python run_meta_engine.py --force
    
    # Run only specific steps:
    python run_meta_engine.py --scan-only       # Just get Top 10s
    python run_meta_engine.py --no-email         # Skip email
    python run_meta_engine.py --no-telegram      # Skip Telegram
    python run_meta_engine.py --no-x             # Skip X/Twitter
    
    # Start the 9:35 AM scheduler:
    python run_meta_engine.py --schedule
    
    # Check configuration:
    python run_meta_engine.py --check
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Ensure Meta Engine is in path
META_DIR = Path(__file__).parent
sys.path.insert(0, str(META_DIR))


def check_config():
    """Print configuration status."""
    from config import MetaConfig
    
    status = MetaConfig.validate()
    
    print("\n🏛️  META ENGINE — Configuration Check")
    print("=" * 50)
    
    print("\n📡 API Keys:")
    for api, configured in status["apis"].items():
        icon = "✅" if configured else "❌"
        print(f"  {icon} {api}")
    
    print("\n📧 Email:")
    email = status["email"]
    icon = "✅" if email["configured"] else "❌"
    print(f"  {icon} Configured: {email['configured']}")
    if email["configured"]:
        print(f"     Server: {email['server']}")
        print(f"     Recipient: {email['recipient']}")
    
    print("\n📱 Telegram:")
    tg = status["telegram"]
    icon = "✅" if tg["configured"] else "❌"
    print(f"  {icon} Configured: {tg['configured']}")
    
    print("\n🐦 X/Twitter:")
    x = status["x_twitter"]
    icon = "✅" if x["configured"] else "❌"
    print(f"  {icon} Configured: {x['configured']}")
    
    print("\n🔧 Engine Paths:")
    eng = status["engines"]
    for name, exists in eng.items():
        icon = "✅" if exists else "❌"
        print(f"  {icon} {name}")
    
    print("\n⏰ Schedule:")
    print(f"  Run time: {MetaConfig.RUN_TIME_ET} ET (Mon-Fri)")
    print(f"  Top N picks: {MetaConfig.TOP_N_PICKS}")
    
    # Check required env vars
    print("\n📋 Environment Variables Needed:")
    env_vars = {
        "ALPACA_API_KEY": bool(os.getenv("ALPACA_API_KEY")),
        "ALPACA_SECRET_KEY": bool(os.getenv("ALPACA_SECRET_KEY")),
        "POLYGON_API_KEY / MASSIVE_API_KEY": bool(os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY")),
        "UNUSUAL_WHALES_API_KEY": bool(os.getenv("UNUSUAL_WHALES_API_KEY")),
        "META_SMTP_USER (or SMTP_USER)": bool(os.getenv("META_SMTP_USER") or os.getenv("SMTP_USER") or os.getenv("PUTSENGINE_EMAIL_SENDER")),
        "META_SMTP_PASSWORD (or SMTP_PASSWORD)": bool(os.getenv("META_SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD") or os.getenv("PUTSENGINE_EMAIL_PASSWORD")),
        "META_ALERT_EMAIL (or ALERT_EMAIL)": bool(os.getenv("META_ALERT_EMAIL") or os.getenv("ALERT_EMAIL") or os.getenv("PUTSENGINE_EMAIL_RECIPIENT")),
        "TELEGRAM_BOT_TOKEN": bool(os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("META_TELEGRAM_BOT_TOKEN")),
        "TELEGRAM_CHAT_ID": bool(os.getenv("TELEGRAM_CHAT_ID") or os.getenv("META_TELEGRAM_CHAT_ID")),
        "X_API_KEY": bool(os.getenv("X_API_KEY")),
        "X_API_SECRET": bool(os.getenv("X_API_SECRET")),
        "X_ACCESS_TOKEN": bool(os.getenv("X_ACCESS_TOKEN")),
        "X_ACCESS_TOKEN_SECRET": bool(os.getenv("X_ACCESS_TOKEN_SECRET")),
    }
    
    for var, found in env_vars.items():
        icon = "✅" if found else "⬜"
        print(f"  {icon} {var}")
    
    print()
    
    # Summary
    required_ok = all([
        status["apis"]["alpaca"],
        status["apis"]["polygon"] or status["apis"]["unusual_whales"],
        status["engines"]["putsengine_exists"],
        status["engines"]["tradenova_exists"],
    ])
    
    if required_ok:
        print("✅ REQUIRED configuration OK — Meta Engine can run")
    else:
        print("❌ Missing REQUIRED configuration — fix above issues")
    
    optional_ok = all([
        status["email"]["configured"],
        status["telegram"]["configured"],
        status["x_twitter"]["configured"],
    ])
    
    if optional_ok:
        print("✅ ALL notification channels configured")
    else:
        missing = []
        if not status["email"]["configured"]: missing.append("Email")
        if not status["telegram"]["configured"]: missing.append("Telegram")
        if not status["x_twitter"]["configured"]: missing.append("X/Twitter")
        print(f"⚠️  Optional notifications not configured: {', '.join(missing)}")
    
    print()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="🏛️ Meta Engine — Cross-Engine Analysis (PutsEngine × Moonshot)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_meta_engine.py                  # Run full pipeline
  python run_meta_engine.py --force          # Force run on weekends
  python run_meta_engine.py --check          # Check configuration
  python run_meta_engine.py --schedule       # Start 9:35 AM scheduler
  python run_meta_engine.py --scan-only      # Only get Top 10s
        """
    )
    
    parser.add_argument("--force", action="store_true",
                       help="Run even on non-trading days")
    parser.add_argument("--check", action="store_true",
                       help="Check configuration and exit")
    parser.add_argument("--schedule", action="store_true",
                       help="Start the 9:35 AM ET scheduler daemon")
    parser.add_argument("--scan-only", action="store_true",
                       help="Only scan for Top 10s, skip notifications")
    parser.add_argument("--no-email", action="store_true",
                       help="Skip sending email")
    parser.add_argument("--no-telegram", action="store_true",
                       help="Skip sending Telegram")
    parser.add_argument("--no-x", action="store_true",
                       help="Skip posting to X/Twitter")
    
    args = parser.parse_args()
    
    if args.check:
        check_config()
        return
    
    if args.schedule:
        from scheduler import start_scheduler
        start_scheduler()
        return
    
    # Run full pipeline
    from meta_engine import run_meta_engine
    
    if args.scan_only:
        # Modified run: just scan and save, no notifications
        from config import MetaConfig
        
        print("\n🏛️  META ENGINE — Scan Only Mode")
        print("=" * 50)
        
        from engine_adapters.puts_adapter import get_top_puts
        from engine_adapters.moonshot_adapter import get_top_moonshots
        
        puts = get_top_puts(MetaConfig.TOP_N_PICKS)
        moonshots = get_top_moonshots(MetaConfig.TOP_N_PICKS)
        
        print(f"\n🔴 PutsEngine Top {len(puts)}:")
        for i, p in enumerate(puts, 1):
            print(f"  #{i} {p['symbol']:6s} Score: {p['score']:.3f}  ${p.get('price', 0):.2f}")
        
        print(f"\n🟢 Moonshot Top {len(moonshots)}:")
        for i, m in enumerate(moonshots, 1):
            print(f"  #{i} {m['symbol']:6s} Score: {m['score']:.3f}  ${m.get('price', 0):.2f}")
        
        # Save to files
        output_dir = Path(MetaConfig.OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        now = datetime.now()
        with open(output_dir / f"scan_only_{now.strftime('%Y%m%d_%H%M')}.json", "w") as f:
            json.dump({
                "timestamp": now.isoformat(),
                "puts_top10": puts,
                "moonshot_top10": moonshots,
            }, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to {output_dir}")
        return
    
    # Full pipeline
    result = run_meta_engine(force=args.force)
    
    if result.get("status") == "completed":
        print("\n✅ Meta Engine completed successfully!")
    elif result.get("status") == "skipped":
        print(f"\n📅 Skipped: {result.get('reason', '')}")
    else:
        print(f"\n❌ Meta Engine finished with status: {result.get('status', 'unknown')}")


if __name__ == "__main__":
    main()
