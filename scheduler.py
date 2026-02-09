"""
Meta Engine Scheduler
======================
Runs the Meta Engine at 9:35 AM EST every trading day.

Methods:
1. APScheduler (preferred) — background daemon
2. macOS launchd — system-level auto-start
3. Manual — run once via command line

Usage:
    # Start scheduler daemon (runs daily at 9:35 AM ET):
    python scheduler.py
    
    # Run once immediately:
    python meta_engine.py --force
    
    # Run once (only on trading days):
    python meta_engine.py
"""

import sys
import os
import signal
import logging
from datetime import datetime, date
from pathlib import Path

import pytz

# Ensure Meta Engine path is in sys.path
META_DIR = Path(__file__).parent
if str(META_DIR) not in sys.path:
    sys.path.insert(0, str(META_DIR))

from config import MetaConfig

# Logging
LOG_DIR = Path(MetaConfig.LOGS_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "scheduler.log", mode="a"),
    ],
)
logger = logging.getLogger("MetaScheduler")

EST = pytz.timezone("US/Eastern")

# PID file for daemon management
PID_FILE = META_DIR / "meta_scheduler.pid"


def _write_pid():
    """Write current process PID to file."""
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def _cleanup_pid(*args):
    """Remove PID file on exit."""
    if PID_FILE.exists():
        PID_FILE.unlink()
    logger.info("Scheduler stopped.")
    sys.exit(0)


def _scheduled_run():
    """Wrapper called by APScheduler at 9:35 AM ET."""
    logger.info("=" * 60)
    logger.info(f"⏰ Scheduled run triggered at {datetime.now(EST).strftime('%I:%M:%S %p ET')}")
    logger.info("=" * 60)
    
    try:
        from meta_engine import run_meta_engine
        result = run_meta_engine(force=False)
        status = result.get("status", "unknown")
        logger.info(f"Run completed with status: {status}")
    except Exception as e:
        logger.error(f"Scheduled run FAILED: {e}", exc_info=True)


def start_scheduler():
    """
    Start the APScheduler daemon that runs Meta Engine at 9:35 AM ET daily.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error("APScheduler not installed. Run: pip install apscheduler")
        sys.exit(1)
    
    # Parse run time
    run_time = MetaConfig.RUN_TIME_ET  # "09:35"
    hour, minute = map(int, run_time.split(":"))
    
    logger.info("=" * 60)
    logger.info("🏛️  META ENGINE SCHEDULER")
    logger.info(f"   Run time: {run_time} ET (every trading day)")
    logger.info(f"   PID: {os.getpid()}")
    logger.info("=" * 60)
    
    # Write PID
    _write_pid()
    
    # Register cleanup
    signal.signal(signal.SIGTERM, _cleanup_pid)
    signal.signal(signal.SIGINT, _cleanup_pid)
    
    # Create scheduler
    scheduler = BlockingScheduler(timezone=EST)
    
    # Schedule daily at 9:35 AM ET, Monday-Friday
    scheduler.add_job(
        _scheduled_run,
        trigger=CronTrigger(
            hour=hour,
            minute=minute,
            day_of_week="mon-fri",
            timezone=EST,
        ),
        id="meta_engine_daily",
        name="Meta Engine Daily Run",
        misfire_grace_time=300,  # 5 min grace period
    )
    
    logger.info(f"✅ Job scheduled: {run_time} ET, Mon-Fri")
    logger.info("Waiting for scheduled run time...")
    logger.info("(Press Ctrl+C to stop)")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        _cleanup_pid()


def stop_scheduler():
    """Stop the scheduler daemon by PID."""
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info(f"Scheduler stopped (PID: {pid})")
            PID_FILE.unlink()
        except ProcessLookupError:
            logger.info(f"Process {pid} not found. Cleaning up PID file.")
            PID_FILE.unlink()
    else:
        logger.info("No scheduler PID file found. Not running?")


def status():
    """Check scheduler status."""
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, 0)  # Check if process exists
            print(f"✅ Scheduler is running (PID: {pid})")
            return True
        except OSError:
            print(f"❌ Scheduler PID file exists but process {pid} is not running")
            PID_FILE.unlink()
            return False
    else:
        print("❌ Scheduler is not running")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Meta Engine Scheduler")
    parser.add_argument("action", nargs="?", default="start",
                       choices=["start", "stop", "status", "run-now"],
                       help="Action to perform")
    args = parser.parse_args()
    
    if args.action == "start":
        start_scheduler()
    elif args.action == "stop":
        stop_scheduler()
    elif args.action == "status":
        status()
    elif args.action == "run-now":
        logger.info("Running Meta Engine NOW...")
        from meta_engine import run_meta_engine
        run_meta_engine(force=True)
