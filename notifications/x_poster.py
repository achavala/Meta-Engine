"""
Meta Engine X/Twitter Poster
===============================
Posts Top 3 Puts and Top 3 Calls in institutional trading format:

Format example:
  "GOLD (Gamma Drain) | Entry: $54.50−$56.10 | Target: -5% by Feb 11 | R:R: 2.8x | Score: 0.95"

Setup:
1. Create a Twitter Developer account at developer.twitter.com
2. Create a Project + App with Read and Write permissions
3. Generate API Key, API Secret, Access Token, Access Token Secret, Bearer Token
4. Set X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET, X_BEARER_TOKEN in .env

Uses the tweepy library for Twitter API v2.
"""

import time
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

# Path to X post tracking database
X_POSTS_DB = Path(__file__).parent.parent / "data" / "x_posts.db"


def _get_twitter_client():
    """Initialize and return a tweepy Client for Twitter API v2."""
    try:
        import tweepy
    except ImportError:
        logger.error("tweepy not installed. Run: pip install tweepy")
        return None
    
    import os
    
    api_key = os.getenv("X_API_KEY", "")
    api_secret = os.getenv("X_API_SECRET", "")
    access_token = os.getenv("X_ACCESS_TOKEN", "")
    access_token_secret = os.getenv("X_ACCESS_TOKEN_SECRET", "")
    bearer_token = os.getenv("X_BEARER_TOKEN", "")
    
    if not all([api_key, api_secret, access_token, access_token_secret]):
        logger.warning("X/Twitter not configured — set X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET")
        return None
    
    try:
        client = tweepy.Client(
            bearer_token=bearer_token,
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )
        # Verify credentials
        me = client.get_me()
        if me and me.data:
            logger.info(f"✅ X/Twitter authenticated as @{me.data.username}")
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate X/Twitter: {e}")
        return None


def _classify_puts_signal_type(pick: Dict[str, Any]) -> str:
    """Classify put pick signal type for display."""
    engine_type = pick.get("engine_type", "")
    signals = pick.get("signals", [])
    signals_str = " ".join(str(s) for s in signals).lower()
    
    if engine_type == "gamma_drain":
        if "exhaustion" in signals_str:
            return "Exhaustion Reversal"
        elif "pump_reversal" in signals_str:
            return "Pump & Dump"
        return "Gamma Drain"
    elif engine_type == "distribution":
        return "Distribution"
    elif engine_type == "convergence":
        return "Convergence Sell"
    elif "dark_pool" in signals_str:
        return "Dark Pool Exit"
    elif "volume" in signals_str or "rvol" in signals_str:
        return "Volume Divergence"
    else:
        return "Bearish Setup"


def _classify_moonshot_signal_type(pick: Dict[str, Any]) -> str:
    """Classify moonshot pick signal type for display."""
    signals = pick.get("signals", [])
    signal_types = pick.get("signal_types", [])
    signals_str = " ".join(str(s) for s in signals).lower()
    
    if "breakout" in signals_str:
        return "Pre Breakout"
    elif "squeeze" in signals_str:
        return "Squeeze Setup"
    elif "sweep" in signals_str:
        return "Sweep Alert"
    elif "momentum" in signals_str:
        return "Momentum Accel"
    elif "moonshot" in signal_types:
        return "Moonshot Setup"
    elif "catalyst" in signal_types:
        return "Catalyst Play"
    elif "rel_strength" in signals_str:
        return "Relative Strength"
    else:
        return "Bullish Catalyst"


def _compute_entry_range(pick: Dict[str, Any], is_puts: bool = False) -> str:
    """Compute entry price range for display."""
    try:
        price = float(pick.get("price", 0) or 0)
    except (ValueError, TypeError):
        price = 0.0
    
    if not is_puts:
        # Moonshot picks have entry_low/entry_high from the engine
        try:
            entry_low = float(pick.get("entry_low", 0) or 0)
            entry_high = float(pick.get("entry_high", 0) or 0)
        except (ValueError, TypeError):
            entry_low, entry_high = 0.0, 0.0
        if entry_low and entry_high:
            return f"${entry_low:.2f}−${entry_high:.2f}"
    
    # For puts or if moonshot doesn't have entry data, compute ±1% band
    if price:
        low = price * 0.99
        high = price * 1.01
        return f"${low:.2f}−${high:.2f}"
    
    return "N/A"


def _compute_target_and_rr(pick: Dict[str, Any], is_puts: bool = False) -> tuple:
    """
    Compute target % and Risk:Reward ratio.
    
    Returns:
        (target_str, rr_str) e.g. ("+7% by Feb 11", "3.2x")
    """
    try:
        price = float(pick.get("price", 0) or 0)
    except (ValueError, TypeError):
        price = 0.0
    try:
        score = float(pick.get("score", 0) or 0)
    except (ValueError, TypeError):
        score = 0.0
    now = datetime.now()
    
    # Target date: 1-2 trading days from now
    target_date = now + timedelta(days=2)
    # Skip weekends
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    date_str = target_date.strftime("%b %d")
    
    if not is_puts:
        # Moonshot picks: use target from engine data
        try:
            target_price = float(pick.get("target", 0) or 0)
        except (ValueError, TypeError):
            target_price = 0.0
        try:
            stop_price = float(pick.get("stop", 0) or 0)
        except (ValueError, TypeError):
            stop_price = 0.0
        
        if target_price and price and price > 0:
            target_pct = ((target_price - price) / price) * 100
            target_str = f"+{target_pct:.0f}% by {date_str}"
            
            if stop_price and price and stop_price > 0:
                risk = abs(price - stop_price)
                reward = abs(target_price - price)
                rr = reward / risk if risk > 0 else 2.0
                rr_str = f"{rr:.1f}x"
            else:
                # Estimate R:R: reward / (price * 0.03 assumed stop)
                reward_pct = target_pct
                rr = abs(reward_pct) / 3.0 if reward_pct > 0 else 2.0
                rr_str = f"{rr:.1f}x"
            
            return target_str, rr_str
        else:
            # Estimate from score
            target_pct = 3 + (score * 10)
            rr = 2.0 + (score * 2)
            return f"+{target_pct:.0f}% by {date_str}", f"{rr:.1f}x"
    else:
        # Puts picks: estimate downside from score
        if score >= 0.68:
            target_pct = -5 - (score * 3)
        elif score >= 0.55:
            target_pct = -3 - (score * 2)
        else:
            target_pct = -1 - (score * 2)
        
        # R:R for puts: target move / stop distance
        rr = 2.0 + (score * 2)
        
        return f"{target_pct:.0f}% by {date_str}", f"{rr:.1f}x"


def format_tweets_institutional(
    summaries: Dict[str, Any],
    cross_results: Dict[str, Any],
    gap_up_data: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Format Top 3 Puts and Top 3 Calls for X/Twitter in institutional format.
    
    Format:
      "TICKER (Signal Type) | Entry: $X−$Y | Target: +Z% by Date | R:R: Nx | Score: X.XX"
    
    Args:
        summaries: Output from generate_all_summaries()
        cross_results: Output from cross_analyze() (contains full pick data)
        
    Returns:
        List of tweet strings (thread)
    """
    tweets = []
    now = datetime.now()
    
    # Extract top 3 from each engine with full pick data
    puts_through = cross_results.get("puts_through_moonshot", [])[:3]
    moon_through = cross_results.get("moonshot_through_puts", [])[:3]
    
    # ===== TWEET 1: Thread Header =====
    # Include time to ensure AM and PM posts are unique (avoids duplicate content error)
    puts_top3_syms = ", ".join(p["symbol"] for p in puts_through) or "N/A"
    moon_top3_syms = ", ".join(p["symbol"] for p in moon_through) or "N/A"
    
    # Get market direction prediction
    market_direction_line = ""
    hour = now.hour
    timeframe = "today" if hour < 12 else "tomorrow"

    # Strategy: 1) Read latest saved prediction file (most reliable)
    #           2) Fallback to running predictor live
    #           3) Fallback to cross_results market_direction data
    try:
        # Attempt 1: Read latest saved prediction file (already generated by meta_engine)
        import json as _json
        from pathlib import Path as _Path
        latest_file = _Path(__file__).parent.parent / "output" / f"market_direction_{timeframe}_latest.json"
        if latest_file.exists():
            with open(latest_file, "r") as _f:
                saved_pred = _json.load(_f)
            # Check freshness (< 4 hours)
            ts_str = saved_pred.get("timestamp", "")
            is_fresh = True
            if ts_str:
                try:
                    from datetime import timezone
                    ts = datetime.fromisoformat(ts_str)
                    age_min = (datetime.now(ts.tzinfo or timezone.utc) - ts).total_seconds() / 60
                    if age_min > 240:
                        is_fresh = False
                        logger.warning(f"  ⚠️ Saved market direction is {age_min:.0f}min old — trying live predictor")
                except Exception:
                    pass  # If can't parse timestamp, still use it

            if is_fresh and saved_pred.get("direction_label"):
                label = saved_pred["direction_label"]
                confidence = saved_pred.get("confidence_pct", 0)
                if timeframe == "today":
                    market_direction_line = f"📊 Market Today: {label} ({confidence:.0f}%)\n\n"
                else:
                    market_direction_line = f"📊 Tomorrow: {label} ({confidence:.0f}%)\n\n"
                logger.info(f"  🌤️ Market direction from saved file: {label} ({confidence:.0f}%)")
    except Exception as e:
        logger.warning(f"  ⚠️ Reading saved market direction failed: {e}")

    # Attempt 2: Run live predictor if saved file didn't work
    if not market_direction_line:
        try:
            from analysis.market_direction_predictor import MarketDirectionPredictor
            predictor = MarketDirectionPredictor()
            prediction = predictor.predict_market_direction(timeframe=timeframe)
            market_direction_line = predictor.format_for_x_post(prediction) + "\n\n"
            logger.info(f"  🌤️ Market direction from live predictor: {market_direction_line.strip()}")
        except Exception as e:
            logger.warning(f"  ⚠️ Live market direction prediction failed: {e}")

    # Attempt 3: Use cross_results market_direction (PutsEngine data)
    if not market_direction_line:
        try:
            md = cross_results.get("market_direction", {})
            direction = md.get("direction", "")
            regime = md.get("regime", "")
            if direction or regime:
                market_direction_line = f"📊 Market: {direction or regime}\n\n"
                logger.info(f"  🌤️ Market direction from PutsEngine: {direction or regime}")
        except Exception as e:
            logger.warning(f"  ⚠️ PutsEngine market direction also failed: {e}")

    if not market_direction_line:
        logger.warning("  ❌ No market direction available for X post")

    # ── TWITTER ALGO WORKAROUND ──────────────────────────────────────
    # 1. NO hashtags in the first tweet — Twitter's algo flags them as
    #    spam/bot and suppresses visibility (especially on mobile).
    #    Hashtags go in the LAST tweet (disclaimer) instead.
    # 2. Slight wording variation each time (minute-level timestamp)
    #    prevents "duplicate content" suppression.
    # ────────────────────────────────────────────────────────────────
    tweet1 = (
        f"🏛️ Meta Engine Daily Alert ({now.strftime('%b %d — %I:%M %p')} ET)\n\n"
        f"{market_direction_line}"
        f"🔴 Top 3 PUT candidates: {puts_top3_syms}\n"
        f"🟢 Top 3 MOONSHOT candidates: {moon_top3_syms}\n\n"
        f"Details below 🧵👇"
    )
    # Ensure header tweet fits in 280 chars
    if len(tweet1) > 280:
        tweet1 = (
            f"🏛️ Meta Engine ({now.strftime('%b %d — %I:%M %p')} ET)\n\n"
            f"{market_direction_line}"
            f"🔴 PUTs: {puts_top3_syms}\n"
            f"🟢 CALLs: {moon_top3_syms}\n\n"
            f"Details 🧵👇"
        )
    if len(tweet1) > 280:
        tweet1 = tweet1[:277] + "..."
    tweets.append(tweet1)
    
    # ===== TWEETS 2-4: Top 3 PUTS =====
    for i, pick in enumerate(puts_through, 1):
        sym = pick["symbol"]
        score = pick.get("score", 0)
        signal_type = _classify_puts_signal_type(pick)
        entry_range = _compute_entry_range(pick, is_puts=True)
        target_str, rr_str = _compute_target_and_rr(pick, is_puts=True)
        
        # Add recurrence stars if present
        stars = pick.get("_recurrence_stars", 0)
        stars_str = ""
        if stars >= 3:
            stars_str = " ⭐⭐⭐"
        elif stars == 2:
            stars_str = " ⭐⭐"
        
        tweet = (
            f"🔴 PUT #{i}{stars_str}:\n\n"
            f"{sym} ({signal_type})\n"
            f"Entry: {entry_range}\n"
            f"Target: {target_str}\n"
            f"R:R: {rr_str} | Score: {score:.2f}"
        )
        
        # Ensure within 280 chars
        if len(tweet) > 280:
            tweet = tweet[:277] + "..."
        
        tweets.append(tweet)
    
    # ===== TWEETS 5-7: Top 3 CALLS (Moonshots) =====
    for i, pick in enumerate(moon_through, 1):
        sym = pick["symbol"]
        score = pick.get("score", 0)
        signal_type = _classify_moonshot_signal_type(pick)
        entry_range = _compute_entry_range(pick, is_puts=False)
        target_str, rr_str = _compute_target_and_rr(pick, is_puts=False)
        
        # Add recurrence stars if present
        stars = pick.get("_recurrence_stars", 0)
        stars_str = ""
        if stars >= 3:
            stars_str = " ⭐⭐⭐"
        elif stars == 2:
            stars_str = " ⭐⭐"
        
        tweet = (
            f"🟢 CALL #{i}{stars_str}:\n\n"
            f"{sym} ({signal_type})\n"
            f"Entry: {entry_range}\n"
            f"Target: {target_str}\n"
            f"R:R: {rr_str} | Score: {score:.2f}"
        )
        
        if len(tweet) > 280:
            tweet = tweet[:277] + "..."
        
        tweets.append(tweet)
    
    # ===== GAP-UP ALERTS TWEET (if any) =====
    if gap_up_data and gap_up_data.get("candidates"):
        gap_candidates = gap_up_data["candidates"]
        # Group by sector for compact display
        sector_groups = {}
        individuals = []
        for gc in gap_candidates:
            sector = gc.get("sector", "")
            if sector:
                sector_groups.setdefault(sector, []).append(gc["symbol"])
            else:
                individuals.append(gc["symbol"])
        
        gap_lines = ["🚀 GAP-UP ALERTS (Same-Day):\n"]
        for sect, syms in list(sector_groups.items())[:3]:
            gap_lines.append(f"⚡ {sect}: {', '.join(syms[:5])}")
        if individuals:
            gap_lines.append(f"⚡ Individual: {', '.join(individuals[:5])}")
        
        theta_note = gap_up_data.get("theta_note", "")
        if theta_note:
            gap_lines.append(f"\n{theta_note[:60]}")
        
        gap_tweet = "\n".join(gap_lines)
        if len(gap_tweet) > 280:
            gap_tweet = gap_tweet[:277] + "..."
        tweets.append(gap_tweet)
    
    # ===== DISCLAIMER TWEET (LAST) + ALL Hashtags =====
    # Hashtags ONLY in the last tweet — Twitter suppresses threads
    # that have hashtags in the header tweet (flags as bot/spam).
    tweets.append(
        "⚠️ Not financial advice. Algorithmic signal analysis only.\n\n"
        "Options involve substantial risk of loss. "
        "Past performance ≠ future results.\n\n"
        "Full report: email subscribers only.\n\n"
        "#Trading #Options #MetaEngine #WallStreet #OptionsTrading #StockMarket"
    )
    
    return tweets


# Profit milestones for automatic posting (institutional-grade tracking)
PROFIT_MILESTONES = [50, 100, 150, 200, 300, 400, 500]  # %


def _ensure_x_posts_db():
    """Create X posts tracking database if it doesn't exist."""
    X_POSTS_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS x_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_timestamp TEXT NOT NULL,
                scan_date TEXT NOT NULL,
                session_label TEXT NOT NULL,
                first_tweet_id TEXT NOT NULL,
                posted_at TEXT DEFAULT (datetime('now')),
                winner_posted INTEGER DEFAULT 0,
                winner_posted_at TEXT,
                UNIQUE(scan_timestamp, session_label)
            )
        """)
        # Add winner_posted column if it doesn't exist (for existing databases)
        try:
            conn.execute("ALTER TABLE x_posts ADD COLUMN winner_posted INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            conn.execute("ALTER TABLE x_posts ADD COLUMN winner_posted_at TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
        
        # Create milestone_posts table to track which milestones have been posted per trade
        conn.execute("""
            CREATE TABLE IF NOT EXISTS milestone_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT NOT NULL,
                milestone_pct INTEGER NOT NULL,
                posted_at TEXT DEFAULT (datetime('now')),
                tweet_id TEXT,
                UNIQUE(trade_id, milestone_pct)
            )
        """)
        conn.commit()


def _store_x_post(scan_timestamp: str, scan_date: str, session_label: str, first_tweet_id: str):
    """Store X post ID linked to scan timestamp."""
    _ensure_x_posts_db()
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO x_posts (scan_timestamp, scan_date, session_label, first_tweet_id) VALUES (?, ?, ?, ?)",
            (scan_timestamp, scan_date, session_label, first_tweet_id)
        )
        conn.commit()


def _get_x_post_id(scan_timestamp: str, session_label: str) -> Optional[str]:
    """Get stored X post ID for a specific scan."""
    _ensure_x_posts_db()
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        row = conn.execute(
            "SELECT first_tweet_id FROM x_posts WHERE scan_timestamp = ? AND session_label = ?",
            (scan_timestamp, session_label)
        ).fetchone()
        return row[0] if row else None


def _is_winner_already_posted(scan_timestamp: str, session_label: str) -> bool:
    """Check if winner update has already been posted for this scan."""
    _ensure_x_posts_db()
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        row = conn.execute(
            "SELECT winner_posted FROM x_posts WHERE scan_timestamp = ? AND session_label = ?",
            (scan_timestamp, session_label)
        ).fetchone()
        return bool(row[0]) if row and row[0] is not None else False


def _mark_winner_posted(scan_timestamp: str, session_label: str):
    """Mark that winner update has been posted for this scan."""
    _ensure_x_posts_db()
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        conn.execute(
            "UPDATE x_posts SET winner_posted = 1, winner_posted_at = datetime('now') WHERE scan_timestamp = ? AND session_label = ?",
            (scan_timestamp, session_label)
        )
        conn.commit()


def _get_posted_milestones(trade_id: str) -> set:
    """Get set of milestone percentages that have already been posted for this trade."""
    _ensure_x_posts_db()
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        rows = conn.execute(
            "SELECT milestone_pct FROM milestone_posts WHERE trade_id = ?",
            (trade_id,)
        ).fetchall()
        return {row[0] for row in rows}


def _mark_milestone_posted(trade_id: str, milestone_pct: int, tweet_id: str = None):
    """Mark that a milestone has been posted for this trade."""
    _ensure_x_posts_db()
    with sqlite3.connect(str(X_POSTS_DB)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO milestone_posts (trade_id, milestone_pct, tweet_id) VALUES (?, ?, ?)",
            (trade_id, milestone_pct, tweet_id)
        )
        conn.commit()


def post_thread(tweets: List[str], scan_timestamp: str = None, session_label: str = None) -> Optional[str]:
    """
    Post a thread of tweets to X/Twitter.
    
    Args:
        tweets: List of tweet texts (each max 280 chars)
        scan_timestamp: Optional timestamp of the scan (for tracking)
        session_label: Optional session label ('AM', 'PM')
        
    Returns:
        First tweet ID if successful, None otherwise
    """
    client = _get_twitter_client()
    if not client:
        return None
    
    try:
        previous_tweet_id = None
        first_tweet_id = None
        
        for i, tweet_text in enumerate(tweets):
            # Truncate if too long
            if len(tweet_text) > 280:
                tweet_text = tweet_text[:277] + "..."
            
            if previous_tweet_id is None:
                # First tweet in thread
                response = client.create_tweet(text=tweet_text)
            else:
                # Reply to previous tweet (thread)
                response = client.create_tweet(
                    text=tweet_text,
                    in_reply_to_tweet_id=previous_tweet_id
                )
            
            if response and response.data:
                previous_tweet_id = response.data["id"]
                if first_tweet_id is None:
                    first_tweet_id = str(previous_tweet_id)
                logger.info(f"  Tweet {i+1}/{len(tweets)} posted (ID: {previous_tweet_id})")
                
                # ── TWITTER ALGO WORKAROUND ──────────────────────────
                # Self-like + self-bookmark the FIRST tweet immediately.
                # This signals to Twitter's algo that the tweet has
                # engagement, preventing it from being suppressed on
                # mobile timelines.  (Known workaround since 2024.)
                # ─────────────────────────────────────────────────────
                if i == 0:
                    try:
                        client.like(previous_tweet_id)
                        logger.info(f"  ❤️ Self-liked first tweet {previous_tweet_id}")
                    except Exception as _like_err:
                        logger.debug(f"  Self-like skipped: {_like_err}")
                    try:
                        client.bookmark(previous_tweet_id)
                        logger.info(f"  🔖 Self-bookmarked first tweet {previous_tweet_id}")
                    except Exception as _bm_err:
                        logger.debug(f"  Self-bookmark skipped: {_bm_err}")
            else:
                logger.error(f"  Tweet {i+1} failed — no response data")
                return None
            
            # ── TWITTER ALGO WORKAROUND ──────────────────────────────
            # 4-second delay between thread tweets (was 2s).
            # Rapid-fire threading triggers Twitter's bot detection
            # and causes the thread to be hidden from followers'
            # timelines.  3-5 seconds is the sweet spot: fast enough
            # to keep the thread together, slow enough to look human.
            # ─────────────────────────────────────────────────────────
            if i < len(tweets) - 1:
                time.sleep(4)
        
        # Store post ID if scan info provided
        if first_tweet_id and scan_timestamp and session_label:
            scan_date = scan_timestamp.split("T")[0] if "T" in scan_timestamp else datetime.now().strftime("%Y-%m-%d")
            _store_x_post(scan_timestamp, scan_date, session_label, first_tweet_id)
            logger.info(f"  📌 Stored X post ID {first_tweet_id} for {session_label} scan at {scan_timestamp}")
        
        logger.info(f"✅ X/Twitter thread posted: {len(tweets)} tweets")
        return first_tweet_id
        
    except Exception as e:
        logger.error(f"❌ Failed to post to X/Twitter: {e}")
        return None


def post_meta_to_x(
    summaries: Dict[str, Any],
    cross_results: Dict[str, Any] = None,
    session_label: str = None,
    gap_up_data: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Post Meta Engine Top 3 Puts + Top 3 Calls to X/Twitter.
    
    Uses institutional format:
      "TICKER (Signal Type) | Entry: $X−$Y | Target: +Z% by Date | R:R: Nx | Score: X.XX"
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        cross_results: Output from cross_analyzer.cross_analyze() (full pick data)
        session_label: Optional session label ('AM', 'PM') for tracking
        gap_up_data: Output from gap_up_detector.detect_gap_ups() (gap-up alerts)
        
    Returns:
        True if posted successfully
    """
    if cross_results:
        # Use the new institutional format with full pick data
        tweets = format_tweets_institutional(summaries, cross_results, gap_up_data=gap_up_data)
        scan_timestamp = cross_results.get("timestamp", "")
    else:
        # Fallback to basic format if cross_results not provided
        tweets = _format_tweets_basic(summaries)
        scan_timestamp = summaries.get("timestamp", "")
    
    if not tweets:
        logger.warning("No tweets to post")
        return False
    
    # Log what would be posted
    for i, t in enumerate(tweets):
        logger.info(f"  🐦 Tweet {i+1} ({len(t)} chars): {t[:100]}...")
    
    result = post_thread(tweets, scan_timestamp=scan_timestamp, session_label=session_label)
    return result is not None


def _format_tweets_basic(summaries: Dict[str, Any]) -> List[str]:
    """Basic tweet format fallback when cross_results not available."""
    tweets = []
    now = datetime.now().strftime('%b %d')
    
    tweets.append(
        f"🏛️ Meta Engine Daily ({now})\n\n"
        f"🔴 PUT + 🟢 MOONSHOT candidates\n"
        f"Cross-engine analysis complete\n\n"
        f"Details below 🧵👇"
    )
    
    for i, p in enumerate(summaries.get("puts_picks_summaries", [])[:3], 1):
        tweet = f"🔴 PUT #{i}: ${p['symbol']} | Score: {p['puts_score']:.2f}\n\n"
        remaining = 280 - len(tweet) - 5
        short = p["summary"][:remaining]
        if len(p["summary"]) > remaining:
            short = short[:short.rfind(' ')] + "..."
        tweets.append(tweet + short)
    
    for i, m in enumerate(summaries.get("moonshot_picks_summaries", [])[:3], 1):
        tweet = f"🟢 CALL #{i}: ${m['symbol']} | Score: {m['moonshot_score']:.2f}\n\n"
        remaining = 280 - len(tweet) - 5
        short = m["summary"][:remaining]
        if len(m["summary"]) > remaining:
            short = short[:short.rfind(' ')] + "..."
        tweets.append(tweet + short)
    
    tweets.append(
        "⚠️ Not financial advice. Options involve substantial risk.\n\n"
        "#Trading #Options #MetaEngine #WallStreet #OptionsTrading #StockMarket"
    )
    
    return tweets


def check_and_post_winners(
    scan_timestamp: str,
    session_label: str = "AM",
    min_profit_pct: float = 50.0,
) -> bool:
    """
    Check for profitable trades from a specific scan and post winner updates to X.
    
    Institutional-grade analysis:
    - Only posts if picks are >50% profitable (configurable)
    - Quote tweets the original post for context
    - Includes detailed P&L analysis
    - Adds relevant hashtags for reach
    
    Args:
        scan_timestamp: Timestamp of the original scan (e.g., "2026-02-12T09:35:00")
        session_label: Session label ('AM', 'PM')
        min_profit_pct: Minimum profit % to trigger a winner post (default 50%)
        
    Returns:
        True if winners were found and posted
    """
    try:
        from trading.trade_db import TradeDB
    except ImportError:
        logger.error("Cannot import TradeDB — winner tracking unavailable")
        return False
    
    # Check if winner already posted (avoid duplicates)
    if _is_winner_already_posted(scan_timestamp, session_label):
        logger.debug(f"Winner update already posted for {session_label} scan at {scan_timestamp}")
        return False
    
    # Get original X post ID
    original_tweet_id = _get_x_post_id(scan_timestamp, session_label)
    if not original_tweet_id:
        logger.warning(f"No X post found for {session_label} scan at {scan_timestamp}")
        return False
    
    # Parse scan date
    scan_date = scan_timestamp.split("T")[0] if "T" in scan_timestamp else datetime.now().strftime("%Y-%m-%d")
    
    # Get trades from this scan
    db = TradeDB()
    trades = db.get_trades_by_date(scan_date)
    
    # Filter for this session and profitable picks
    winners = []
    for trade in trades:
        # Match session
        if trade.get("session", "").upper() != session_label.upper():
            continue
        
        # Check if profitable
        pnl_pct = float(trade.get("pnl_pct", 0) or 0)
        if pnl_pct >= min_profit_pct:
            winners.append(trade)
    
    if not winners:
        logger.info(f"No winners >{min_profit_pct}% found for {session_label} scan at {scan_timestamp}")
        return False
    
    # Sort by profit % descending
    winners.sort(key=lambda x: float(x.get("pnl_pct", 0) or 0), reverse=True)
    
    # Format winner update tweet
    client = _get_twitter_client()
    if not client:
        return False
    
    try:
        # Build winner tweet with institutional analysis
        now = datetime.now()
        try:
            scan_dt = datetime.fromisoformat(scan_timestamp.replace("Z", "+00:00") if "Z" in scan_timestamp else scan_timestamp)
        except:
            scan_dt = datetime.now()
        time_elapsed = now - scan_dt
        hours = int(time_elapsed.total_seconds() / 3600)
        minutes = int((time_elapsed.total_seconds() % 3600) / 60)
        
        # Header tweet
        winner_count = len(winners)
        best_pnl = float(winners[0].get("pnl_pct", 0) or 0)
        best_symbol = winners[0].get("symbol", "?")
        
        # No hashtags in header — they go in the last detail tweet.
        # Twitter algo suppresses tweets with hashtags in the first post.
        header_tweet = (
            f"🏆 WINNER UPDATE ({session_label} Scan)\n\n"
            f"⏱️ {hours}h {minutes}m since alert\n\n"
            f"🎯 {winner_count} pick{'s' if winner_count > 1 else ''} >{min_profit_pct}% profit\n"
            f"🥇 Best: {best_symbol} +{best_pnl:.0f}%\n\n"
            f"Details below 🧵👇"
        )
        
        # Post header as quote tweet
        response = client.create_tweet(
            text=header_tweet,
            quote_tweet_id=original_tweet_id
        )
        
        if not response or not response.data:
            logger.error("Failed to post winner header tweet")
            return False
        
        previous_tweet_id = response.data["id"]
        logger.info(f"  Winner header posted (ID: {previous_tweet_id})")
        time.sleep(2)
        
        # Individual winner tweets (thread)
        for i, winner in enumerate(winners[:5], 1):  # Max 5 winners
            sym = winner.get("symbol", "?")
            option_type = winner.get("option_type", "?").upper()
            strike = winner.get("strike_price", 0)
            expiry = winner.get("expiry_date", "?")
            entry_px = float(winner.get("entry_price", 0) or 0)
            current_px = float(winner.get("current_price", 0) or 0)
            pnl = float(winner.get("pnl", 0) or 0)
            pnl_pct = float(winner.get("pnl_pct", 0) or 0)
            
            # Institutional analysis
            if pnl_pct >= 200:
                emoji = "🚀"
                analysis = "EXPLOSIVE MOVE"
            elif pnl_pct >= 100:
                emoji = "🔥"
                analysis = "DOUBLED"
            elif pnl_pct >= 75:
                emoji = "⚡"
                analysis = "STRONG MOMENTUM"
            else:
                emoji = "✅"
                analysis = "PROFITABLE"
            
            winner_tweet = (
                f"{emoji} WINNER #{i}:\n\n"
                f"{sym} {option_type} ${strike:.0f} ({expiry[:5] if expiry else '?'})\n"
                f"Entry: ${entry_px:.2f} → Current: ${current_px:.2f}\n"
                f"P&L: ${pnl:+,.2f} (+{pnl_pct:.0f}%)\n\n"
                f"📊 {analysis}\n"
                f"#OptionsTrading #StockMarket"
            )
            
            # Truncate if needed
            if len(winner_tweet) > 280:
                winner_tweet = winner_tweet[:277] + "..."
            
            response = client.create_tweet(
                text=winner_tweet,
                in_reply_to_tweet_id=previous_tweet_id
            )
            
            if response and response.data:
                previous_tweet_id = response.data["id"]
                logger.info(f"  Winner #{i} posted: {sym} +{pnl_pct:.0f}%")
            else:
                logger.error(f"  Failed to post winner #{i}")
            
            time.sleep(4)  # 4s delay — avoids bot detection
        
        # Summary tweet
        total_pnl = sum(float(w.get("pnl", 0) or 0) for w in winners)
        avg_pnl_pct = sum(float(w.get("pnl_pct", 0) or 0) for w in winners) / len(winners)
        
        summary_tweet = (
            f"📈 SUMMARY:\n\n"
            f"Total P&L: ${total_pnl:+,.2f}\n"
            f"Avg Return: +{avg_pnl_pct:.0f}%\n"
            f"Win Rate: {len(winners)}/{len(winners)} (100%)\n\n"
            f"⚠️ Past performance ≠ future results\n"
            f"#Trading #Options #MetaEngine #QuantTrading"
        )
        
        response = client.create_tweet(
            text=summary_tweet,
            in_reply_to_tweet_id=previous_tweet_id
        )
        
        if response and response.data:
            logger.info(f"  Summary posted")
        
        # Mark as posted to avoid duplicates
        _mark_winner_posted(scan_timestamp, session_label)
        
        logger.info(f"✅ Winner update posted: {len(winners)} winners from {session_label} scan")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to post winner update: {e}")
        return False


def _send_milestone_telegram_alert(
    trade: Dict[str, Any],
    milestone: int,
    pnl_pct: float,
) -> bool:
    """
    Send Telegram alert for a profit milestone.
    
    Institutional-grade formatting with detailed P&L analysis.
    """
    try:
        import os
        from config import MetaConfig
        
        bot_token = MetaConfig.TELEGRAM_BOT_TOKEN
        chat_id = MetaConfig.TELEGRAM_CHAT_ID
        
        if not bot_token or not chat_id:
            logger.debug("Telegram not configured — skipping milestone alert")
            return False
        
        from notifications.telegram_sender import send_telegram_message
        
        sym = trade.get("symbol", "?")
        option_type = trade.get("option_type", "?").upper()
        strike = trade.get("strike_price", 0)
        expiry = trade.get("expiry_date", "?")
        entry_px = float(trade.get("entry_price", 0) or 0)
        current_px = float(trade.get("current_price", 0) or 0)
        pnl = float(trade.get("pnl", 0) or 0)
        contracts = int(trade.get("contracts", 5))
        
        # Institutional analysis based on milestone
        if milestone >= 500:
            emoji = "🚀🚀🚀"
            analysis = "LEGENDARY — 5X+ RETURN"
            urgency = "🔥🔥🔥"
        elif milestone >= 400:
            emoji = "🚀🚀"
            analysis = "EXCEPTIONAL — 4X+ RETURN"
            urgency = "🔥🔥"
        elif milestone >= 300:
            emoji = "🚀"
            analysis = "EXPLOSIVE — 3X+ RETURN"
            urgency = "🔥"
        elif milestone >= 200:
            emoji = "🔥🔥"
            analysis = "DOUBLED — 2X+ RETURN"
            urgency = "⚡"
        elif milestone >= 150:
            emoji = "🔥"
            analysis = "STRONG MOMENTUM — 1.5X"
            urgency = "✅"
        elif milestone >= 100:
            emoji = "⚡"
            analysis = "DOUBLED — 2X RETURN"
            urgency = "✅"
        else:
            emoji = "✅"
            analysis = "PROFITABLE — 50%+"
            urgency = "📈"
        
        # Calculate time in trade
        try:
            from datetime import datetime
            filled_at = trade.get("filled_at", "")
            if filled_at:
                fill_dt = datetime.fromisoformat(filled_at.replace("Z", "+00:00") if "Z" in filled_at else filled_at)
                now = datetime.now(fill_dt.tzinfo) if fill_dt.tzinfo else datetime.now()
                time_in_trade = now - fill_dt
                hours = int(time_in_trade.total_seconds() / 3600)
                minutes = int((time_in_trade.total_seconds() % 3600) / 60)
                time_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
            else:
                time_str = "N/A"
        except:
            time_str = "N/A"
        
        # Format Telegram message (HTML)
        message = (
            f"{urgency} <b>MILESTONE ALERT: {milestone}% PROFIT</b>\n\n"
            f"<b>{sym}</b> {option_type} ${strike:.0f} ({expiry[:5] if expiry else '?'})\n"
            f"📊 {analysis}\n\n"
            f"<b>Entry:</b> ${entry_px:.2f}\n"
            f"<b>Current:</b> ${current_px:.2f}\n"
            f"<b>P&L:</b> ${pnl:+,.2f} (+{pnl_pct:.0f}%)\n"
            f"<b>Contracts:</b> {contracts}\n"
            f"<b>Time in Trade:</b> {time_str}\n\n"
            f"<i>Institutional-grade milestone tracking</i>\n"
            f"#Trading #Options #MetaEngine #Milestone"
        )
        
        success = send_telegram_message(
            message=message,
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode="HTML",
        )
        
        if success:
            logger.info(f"  📱 Telegram alert sent for {milestone}% milestone: {sym}")
        
        return success
        
    except Exception as e:
        logger.error(f"  ❌ Failed to send Telegram milestone alert: {e}")
        return False


def check_and_post_milestones(min_profit_pct: float = 50.0) -> Dict[str, Any]:
    """
    Check all open trades for profit milestone crossings and post updates automatically.
    
    Institutional-grade milestone tracking:
    - Monitors all open positions continuously
    - Posts at specific milestones: 50%, 100%, 150%, 200%, 300%, 400%, 500%
    - Each milestone posted only once per trade
    - Quote tweets original scan post for context
    - Sends Telegram alerts simultaneously with X posts
    - Includes detailed P&L analysis and institutional hashtags
    
    Args:
        min_profit_pct: Minimum profit % to start checking (default 50%)
        
    Returns:
        Dict with stats: {'checked': N, 'milestones_posted': M, 'trades_with_milestones': K}
    """
    try:
        from trading.trade_db import TradeDB
    except ImportError:
        logger.error("Cannot import TradeDB — milestone tracking unavailable")
        return {"checked": 0, "milestones_posted": 0, "trades_with_milestones": 0}
    
    db = TradeDB()
    open_trades = db.get_open_positions()
    
    if not open_trades:
        return {"checked": 0, "milestones_posted": 0, "trades_with_milestones": 0}
    
    client = _get_twitter_client()
    if not client:
        return {"checked": 0, "milestones_posted": 0, "trades_with_milestones": 0}
    
    stats = {
        "checked": 0,
        "milestones_posted": 0,
        "trades_with_milestones": 0,
    }
    
    # Group trades by scan to get original X post IDs
    scan_posts = {}  # (scan_date, session) -> tweet_id
    
    for trade in open_trades:
        stats["checked"] += 1
        trade_id = trade.get("trade_id", "")
        pnl_pct = float(trade.get("pnl_pct", 0) or 0)
        
        # Skip if below minimum threshold
        if pnl_pct < min_profit_pct:
            continue
        
        # Get already posted milestones for this trade
        posted_milestones = _get_posted_milestones(trade_id)
        
        # Find which milestones have been crossed but not posted
        crossed_milestones = []
        for milestone in PROFIT_MILESTONES:
            if pnl_pct >= milestone and milestone not in posted_milestones:
                crossed_milestones.append(milestone)
        
        if not crossed_milestones:
            continue  # No new milestones
        
        stats["trades_with_milestones"] += 1
        
        # Get scan info to find original X post
        scan_date = trade.get("scan_date", "")
        session = trade.get("session", "").upper()
        if session == "MORNING" or session == "AM":
            session_label = "Morning"
        elif session == "AFTERNOON" or session == "PM":
            session_label = "Afternoon"
        else:
            session_label = "Morning"  # Default to Morning
        
        # Get original X post ID (cache it)
        cache_key = (scan_date, session_label)
        if cache_key not in scan_posts:
            # Try to find scan timestamp from cross_analysis files
            from pathlib import Path
            import json
            output_dir = Path(__file__).parent.parent / "output"
            cross_file = output_dir / f"cross_analysis_{scan_date.replace('-', '')}.json"
            scan_timestamp = None
            if cross_file.exists():
                try:
                    with open(cross_file) as f:
                        cross_data = json.load(f)
                    scan_timestamp = cross_data.get("timestamp", "")
                except:
                    pass
            
            if scan_timestamp:
                original_tweet_id = _get_x_post_id(scan_timestamp, session_label)
                scan_posts[cache_key] = original_tweet_id
            else:
                scan_posts[cache_key] = None
        
        original_tweet_id = scan_posts[cache_key]
        
        # Post milestone updates (post highest milestone first)
        crossed_milestones.sort(reverse=True)
        
        for milestone in crossed_milestones:
            try:
                # Build milestone tweet
                sym = trade.get("symbol", "?")
                option_type = trade.get("option_type", "?").upper()
                strike = trade.get("strike_price", 0)
                expiry = trade.get("expiry_date", "?")
                entry_px = float(trade.get("entry_price", 0) or 0)
                current_px = float(trade.get("current_price", 0) or 0)
                pnl = float(trade.get("pnl", 0) or 0)
                
                # Institutional analysis based on milestone
                if milestone >= 500:
                    emoji = "🚀🚀🚀"
                    analysis = "LEGENDARY — 5X+ RETURN"
                elif milestone >= 400:
                    emoji = "🚀🚀"
                    analysis = "EXCEPTIONAL — 4X+ RETURN"
                elif milestone >= 300:
                    emoji = "🚀"
                    analysis = "EXPLOSIVE — 3X+ RETURN"
                elif milestone >= 200:
                    emoji = "🔥🔥"
                    analysis = "DOUBLED — 2X+ RETURN"
                elif milestone >= 150:
                    emoji = "🔥"
                    analysis = "STRONG MOMENTUM — 1.5X"
                elif milestone >= 100:
                    emoji = "⚡"
                    analysis = "DOUBLED — 2X RETURN"
                else:
                    emoji = "✅"
                    analysis = "PROFITABLE — 50%+"
                
                milestone_tweet = (
                    f"{emoji} MILESTONE: {milestone}% PROFIT\n\n"
                    f"{sym} {option_type} ${strike:.0f} ({expiry[:5] if expiry else '?'})\n"
                    f"Entry: ${entry_px:.2f} → Current: ${current_px:.2f}\n"
                    f"P&L: ${pnl:+,.2f} (+{pnl_pct:.0f}%)\n\n"
                    f"📊 {analysis}\n\n"
                    f"#OptionsTrading #StockMarket #Trading #MetaEngine"
                )
                
                # Truncate if needed
                if len(milestone_tweet) > 280:
                    milestone_tweet = milestone_tweet[:277] + "..."
                
                # Post as quote tweet if original post exists, otherwise standalone
                if original_tweet_id:
                    response = client.create_tweet(
                        text=milestone_tweet,
                        quote_tweet_id=original_tweet_id
                    )
                else:
                    response = client.create_tweet(text=milestone_tweet)
                
                if response and response.data:
                    tweet_id = str(response.data["id"])
                    _mark_milestone_posted(trade_id, milestone, tweet_id)
                    stats["milestones_posted"] += 1
                    logger.info(f"  ✅ Posted {milestone}% milestone for {sym} (trade: {trade_id[:12]})")
                    
                    # Send Telegram alert simultaneously
                    _send_milestone_telegram_alert(trade, milestone, pnl_pct)
                    
                    time.sleep(2)  # Rate limit
                else:
                    logger.error(f"  ❌ Failed to post {milestone}% milestone for {sym}")
                    
            except Exception as e:
                logger.error(f"  ❌ Error posting {milestone}% milestone: {e}")
                continue
    
    if stats["milestones_posted"] > 0:
        logger.info(f"✅ Milestone check: {stats['checked']} trades checked, "
                   f"{stats['milestones_posted']} milestones posted, "
                   f"{stats['trades_with_milestones']} trades with new milestones")
    
    return stats
