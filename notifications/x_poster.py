"""
Meta Engine X/Twitter Poster
===============================
Posts the 3-sentence summaries as a tweet thread on X (Twitter).

Setup:
1. Create a Twitter Developer account at developer.twitter.com
2. Create an app with Read and Write permissions
3. Generate API Key, API Secret, Access Token, Access Token Secret
4. Set X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET in .env

Uses the tweepy library for Twitter API v2.
"""

import time
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


def post_meta_to_x(summaries: Dict[str, Any]) -> bool:
    """
    Post Meta Engine analysis to X/Twitter as a thread.
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        
    Returns:
        True if posted successfully
    """
    from analysis.summary_generator import format_summaries_for_x
    
    tweets = format_summaries_for_x(summaries)
    
    if not tweets:
        logger.warning("No tweets to post")
        return False
    
    return post_thread(tweets)
