"""
Meta Engine Telegram Sender
=============================
Sends 3-sentence summaries and alerts via Telegram Bot API.

Setup:
1. Create a bot via @BotFather on Telegram
2. Get your bot token
3. Get your chat_id (send /start to your bot, then check
   https://api.telegram.org/bot<TOKEN>/getUpdates)
4. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env
"""

import requests
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"


def send_telegram_message(
    message: str,
    bot_token: str = "",
    chat_id: str = "",
    parse_mode: str = "Markdown",
) -> bool:
    """
    Send a text message via Telegram Bot API.
    
    Args:
        message: Message text (supports Markdown or HTML)
        bot_token: Telegram bot token
        chat_id: Target chat ID
        parse_mode: 'Markdown' or 'HTML'
        
    Returns:
        True if message sent successfully
    """
    if not bot_token or not chat_id:
        logger.warning("Telegram not configured — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        return False
    
    try:
        url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        
        resp = requests.post(url, json=payload, timeout=15)
        
        if resp.status_code == 200:
            logger.info("✅ Telegram message sent")
            return True
        else:
            logger.error(f"Telegram API error: {resp.status_code} — {resp.text[:200]}")
            # If Markdown fails, try without parse_mode
            if parse_mode == "Markdown":
                logger.info("Retrying without Markdown formatting...")
                payload["parse_mode"] = None
                # Strip markdown
                clean_text = message.replace("*", "").replace("_", "").replace("`", "")
                payload["text"] = clean_text
                resp2 = requests.post(url, json=payload, timeout=15)
                if resp2.status_code == 200:
                    logger.info("✅ Telegram message sent (plain text)")
                    return True
            return False
            
    except Exception as e:
        logger.error(f"❌ Telegram send failed: {e}")
        return False


def send_telegram_photo(
    photo_path: str,
    caption: str = "",
    bot_token: str = "",
    chat_id: str = "",
) -> bool:
    """
    Send a photo (chart) via Telegram Bot API.
    
    Args:
        photo_path: Path to the image file
        caption: Photo caption (max 1024 chars)
        bot_token: Telegram bot token
        chat_id: Target chat ID
        
    Returns:
        True if photo sent successfully
    """
    if not bot_token or not chat_id:
        logger.warning("Telegram not configured")
        return False
    
    if not Path(photo_path).exists():
        logger.error(f"Photo not found: {photo_path}")
        return False
    
    try:
        url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/sendPhoto"
        
        with open(photo_path, "rb") as photo:
            files = {"photo": photo}
            data = {
                "chat_id": chat_id,
                "caption": caption[:1024],  # Telegram limit
                "parse_mode": "Markdown",
            }
            
            resp = requests.post(url, files=files, data=data, timeout=30)
        
        if resp.status_code == 200:
            logger.info("✅ Telegram chart sent")
            return True
        else:
            logger.error(f"Telegram photo error: {resp.status_code} — {resp.text[:200]}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Telegram photo send failed: {e}")
        return False


def send_meta_telegram(
    summaries: Dict[str, Any],
    chart_path: Optional[str] = None,
    bot_token: str = "",
    chat_id: str = "",
) -> bool:
    """
    Send the complete Meta Engine report via Telegram.
    Sends the summary message first, then the chart as a photo.
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        chart_path: Path to the technical chart PNG
        bot_token: Telegram bot token
        chat_id: Target chat ID
        
    Returns:
        True if all messages sent successfully
    """
    from analysis.summary_generator import format_summaries_for_telegram
    
    # 1. Send text summary
    message = format_summaries_for_telegram(summaries)
    text_sent = send_telegram_message(message, bot_token, chat_id)
    
    # 2. Send chart photo
    chart_sent = False
    if chart_path and Path(chart_path).exists():
        caption = (
            f"📊 Meta Engine Technical Chart\n"
            f"_{datetime.now().strftime('%B %d, %Y')}_\n"
            f"Top picks from PutsEngine (bearish) & Moonshot (bullish)"
        )
        chart_sent = send_telegram_photo(chart_path, caption, bot_token, chat_id)
    
    return text_sent
