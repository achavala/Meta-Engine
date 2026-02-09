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
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


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
    price = pick.get("price", 0)
    
    if not is_puts:
        # Moonshot picks have entry_low/entry_high from the engine
        entry_low = pick.get("entry_low", 0)
        entry_high = pick.get("entry_high", 0)
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
    price = pick.get("price", 0)
    score = pick.get("score", 0)
    now = datetime.now()
    
    # Target date: 1-2 trading days from now
    target_date = now + timedelta(days=2)
    # Skip weekends
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    date_str = target_date.strftime("%b %d")
    
    if not is_puts:
        # Moonshot picks: use target from engine data
        target_price = pick.get("target", 0)
        stop_price = pick.get("stop", 0)
        
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
    n_puts = len(cross_results.get("puts_through_moonshot", []))
    n_moon = len(cross_results.get("moonshot_through_puts", []))
    
    tweet1 = (
        f"🏛️ Meta Engine Daily Alert ({now.strftime('%b %d')})\n\n"
        f"🔴 {n_puts} PUT candidates scanned\n"
        f"🟢 {n_moon} MOONSHOT candidates scanned\n\n"
        f"Top 3 from each engine below 🧵👇\n\n"
        f"#Trading #Options #MetaEngine"
    )
    tweets.append(tweet1)
    
    # ===== TWEETS 2-4: Top 3 PUTS =====
    for i, pick in enumerate(puts_through, 1):
        sym = pick["symbol"]
        score = pick.get("score", 0)
        signal_type = _classify_puts_signal_type(pick)
        entry_range = _compute_entry_range(pick, is_puts=True)
        target_str, rr_str = _compute_target_and_rr(pick, is_puts=True)
        
        # Get cross-analysis insight
        moon_analysis = pick.get("moonshot_analysis", {})
        moon_opp = moon_analysis.get("opportunity_level", "N/A")
        
        tweet = (
            f"🔴 PUT #{i}:\n\n"
            f"{sym} ({signal_type})\n"
            f"Entry: {entry_range}\n"
            f"Target: {target_str}\n"
            f"R:R: {rr_str} | Score: {score:.2f}\n\n"
            f"Cross-check: Moon {moon_opp}"
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
        
        # Get cross-analysis insight
        puts_analysis = pick.get("puts_analysis", {})
        puts_risk = puts_analysis.get("risk_level", "N/A")
        
        tweet = (
            f"🟢 CALL #{i}:\n\n"
            f"{sym} ({signal_type})\n"
            f"Entry: {entry_range}\n"
            f"Target: {target_str}\n"
            f"R:R: {rr_str} | Score: {score:.2f}\n\n"
            f"Cross-check: Puts Risk {puts_risk}"
        )
        
        if len(tweet) > 280:
            tweet = tweet[:277] + "..."
        
        tweets.append(tweet)
    
    # ===== TWEET 8: Disclaimer =====
    tweets.append(
        "⚠️ Not financial advice. Algorithmic signal analysis only.\n\n"
        "Options involve substantial risk of loss. "
        "Past performance ≠ future results.\n\n"
        "Full report: email subscribers only.\n\n"
        "#Trading #Options #MetaEngine #WallStreet"
    )
    
    return tweets


def post_thread(tweets: List[str]) -> bool:
    """
    Post a thread of tweets to X/Twitter.
    
    Args:
        tweets: List of tweet texts (each max 280 chars)
        
    Returns:
        True if thread posted successfully
    """
    client = _get_twitter_client()
    if not client:
        return False
    
    try:
        previous_tweet_id = None
        
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
                logger.info(f"  Tweet {i+1}/{len(tweets)} posted (ID: {previous_tweet_id})")
            else:
                logger.error(f"  Tweet {i+1} failed — no response data")
                return False
            
            # Rate limit: wait between tweets
            if i < len(tweets) - 1:
                time.sleep(2)
        
        logger.info(f"✅ X/Twitter thread posted: {len(tweets)} tweets")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to post to X/Twitter: {e}")
        return False


def post_meta_to_x(
    summaries: Dict[str, Any],
    cross_results: Dict[str, Any] = None,
) -> bool:
    """
    Post Meta Engine Top 3 Puts + Top 3 Calls to X/Twitter.
    
    Uses institutional format:
      "TICKER (Signal Type) | Entry: $X−$Y | Target: +Z% by Date | R:R: Nx | Score: X.XX"
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        cross_results: Output from cross_analyzer.cross_analyze() (full pick data)
        
    Returns:
        True if posted successfully
    """
    if cross_results:
        # Use the new institutional format with full pick data
        tweets = format_tweets_institutional(summaries, cross_results)
    else:
        # Fallback to basic format if cross_results not provided
        tweets = _format_tweets_basic(summaries)
    
    if not tweets:
        logger.warning("No tweets to post")
        return False
    
    # Log what would be posted
    for i, t in enumerate(tweets):
        logger.info(f"  🐦 Tweet {i+1} ({len(t)} chars): {t[:100]}...")
    
    return post_thread(tweets)


def _format_tweets_basic(summaries: Dict[str, Any]) -> List[str]:
    """Basic tweet format fallback when cross_results not available."""
    tweets = []
    now = datetime.now().strftime('%b %d')
    
    tweets.append(
        f"🏛️ Meta Engine Daily ({now})\n\n"
        f"🔴 PUT + 🟢 MOONSHOT candidates\n"
        f"Cross-engine analysis complete 🧵👇"
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
        "#Trading #Options #MetaEngine"
    )
    
    return tweets
